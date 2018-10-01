#include "RIT.h"
#include "ttup.h"
#include "math.h"
#include <stdio.h>
#include "nodeHeapAm.h"
#include "nodeSort.h"

static timestmp forkNode(timestmp root, timestmp lower, timestmp upper);
static void arithmeticLeftRightNodes(List **leftQueries, List **rightQueries, timestmp lower, timestmp upper, timestmp forknode, timestmp root);
static void arithmeticUpDownLeftRightNodes(List **leftQueries, List **rightQueries, RIT *outer_rit, RIT *inner_rit);
static void *getNodeLowerCopy (const void *t);
static void *getNodeUpperCopy (const void *t);
static int cmp_timestmp_timestmp(const void *a, const void *b);

RIT *rit_create(List *rel)
{
  RIT *ret = private_malloc(sizeof(RIT));
  ret->onDisk = false;
  ret->lowerIdx = createBTree(rel, cmp_timestmp_timestmp, getNodeLowerCopy, private_free); /* (node, lower) */
  ret->upperIdx = createBTree(rel, cmp_timestmp_timestmp, getNodeUpperCopy, private_free); /* (node, upper) */
  void *largestKey = btree_findLargest(ret->upperIdx);
  if(largestKey)
    ret->root = (timestmp) pow(2, floor(log2((double) *(timestmp *)largestKey)));
  else
    ret->root = 0;

  return ret;
}

int cmp_rit_clustering(const void *a, const void *b)
{
  void *key_a = getNodeLowerCopy(a);
  void *key_b = getNodeLowerCopy(b);
  int ret = cmp_timestmp_timestmp(key_a, key_b);
  private_free(key_a);
  private_free(key_b);
  return ret;
}

RIT *rit_create_disk(HeapAm *rel, HeapAm *cluster_storage)
{
  RIT *ret = private_malloc(sizeof(RIT));
  ret->onDisk = true;
  ret->rel = rel;
  ret->isClusetered = cluster_storage != NULL;

  if(ret->isClusetered)
  {
    ret->rel = cluster_storage;
    Sort *sort = ExecInitSort(rel, cmp_rit_clustering, rel->htupsze, WORK_MEM, rel->f->f->type);
    ttup *tup;
    while((tup = ExecSort(sort)) != NULL)
    {
      ExecHeapAmInsert(ret->rel, tup);
    }
  }

  /*
   * create a temporary main memory version of the relation without data and TID
   */
  List *new_rel = NIL;
  ExecHeapAmScanInit(ret->rel);
  ttup *tup, *tmp_tup;
  HTID *tmp_tid;
  HTID tid;
  while((tup = ExecHeapAmScanWithTID(ret->rel, &tid)) != NULL)
  {
    tmp_tid = private_malloc(sizeof(HTID));
    tmp_tid->pageno = tid.pageno;
    tmp_tid->htupno = tid.htupno;
    tmp_tup = makeTTup(tup->ts, tup->te, tmp_tid);
    new_rel = lappend(new_rel, tmp_tup);
  }

  ret->mem_rel = new_rel;

  ret->lowerIdx = createBTree(new_rel, cmp_timestmp_timestmp, getNodeLowerCopy, private_free); /* (node, lower) */
  ret->upperIdx = createBTree(new_rel, cmp_timestmp_timestmp, getNodeUpperCopy, private_free); /* (node, upper) */
  void *largestKey = btree_findLargest(ret->upperIdx);
  if(largestKey)
    ret->root = (timestmp) pow(2, floor(log2((double) *(timestmp *)largestKey)));
  else
    ret->root = 0;

  return ret;
}

void
rit_close(RIT *rit)
{
  destroyBTree(rit->lowerIdx);
  destroyBTree(rit->upperIdx);
  if(rit->onDisk)
  {
    ListCell *cell;
    foreach(cell, rit->mem_rel)
    {
      freeTTup(lfirst(cell));
    }
    list_free(rit->mem_rel);
  }
  private_free(rit);
}

void
rit_select(RIT *rit, timestmp lower, timestmp upper, long *match, long *fhits)
{
  List *leftNodes;
  List *rightNodes;
  timestmp forknode;
  ListCell *cell, *tup_cell;
  btree_leaf_node *btree_leaf;
  timestmp key[2];


  forknode = forkNode(rit->root, lower, upper);
  arithmeticLeftRightNodes(&leftNodes, &rightNodes, lower, upper, forknode, rit->root);

  /*
   * SELECT id
   * FROM Intervals i, leftNodes left
   * WHERE i.node BETWEEN left.min AND left.max
   * AND i.upper >=:lower
   *
   * translates to
   *
   * for each left
   *   upperIndex search
   *
   */
  foreach(cell, leftNodes)
  {
    timestmp left_min = *(timestmp*)lfirst(cell);
    timestmp left_max = *(timestmp*)(lfirst(cell)+sizeof(timestmp));
    key[0] = left_min;
    key[1] = lower;

    btree_leaf = btree_findLargerEqualLeaf(rit->upperIdx, &key);
    for( ; btree_leaf != NULL; btree_leaf = btree_leaf->next)
    {
      timestmp i_node = *(timestmp*)btree_leaf->n.key;
      timestmp i_upper = *(timestmp*)(btree_leaf->n.key+sizeof(timestmp));
      if(i_node > left_max)
        break;

      if(i_upper < lower)
      {
        /* never happens */
        (*fhits)++;
        continue;
      }

      /* produce result */
      foreach(tup_cell, btree_leaf->tups)
      {
        (*match)++;
      }
    }
  }

  /*
   * UNION ALL
   * SELECT id
   * FROM Intervals i, rightNodes right
   * WHERE i.node = right.node
   * AND i.lower <= :upper
   *
   * translates to
   *
   * for each right
   *   lowerIndex search
   *
   */
  foreach(cell, rightNodes)
  {
    timestmp right_node = *(timestmp*)lfirst(cell);
    key[0] = right_node;
    key[1] = 0;
    btree_leaf = btree_findLargerEqualLeaf(rit->lowerIdx, &key);
    for( ; btree_leaf != NULL; btree_leaf = btree_leaf->next)
    {
      timestmp i_node = *(timestmp*)btree_leaf->n.key;
      timestmp i_lower = *(timestmp*)(btree_leaf->n.key+sizeof(timestmp));
      if(i_node != right_node || i_lower > upper)
        break;

      /* produce result */
      foreach(tup_cell, btree_leaf->tups)
      {
        (*match)++;
      }
    }
  }

  list_free_deep(leftNodes);
  list_free_deep(rightNodes);
}

static int cmp_timestmp_timestmp(const void *a, const void *b)
{
  int ret = (*(timestmp*)a > *(timestmp*)b) - (*(timestmp*)a < *(timestmp*)b);
  if(ret == 0)
    ret = (*(timestmp*)(a+sizeof(timestmp)) > *(timestmp*)(b+sizeof(timestmp))) - (*(timestmp*)(a+sizeof(timestmp)) < *(timestmp*)(b+sizeof(timestmp)));
  return ret;
}

static void *getNodeLowerCopy (const void *t)
{
  /* create a key of the form (forkNode, lower) */
  ttup *tup = (ttup *)t;
  void *ret = private_malloc(2*sizeof(timestmp));
  timestmp root = (timestmp) pow(2, floor(log2((double) tup->te)));

  *((timestmp*)ret) = forkNode(root, tup->ts, tup->te);
  *((timestmp*)(ret+sizeof(timestmp))) = tup->ts;

  return ret;
}

static void *getNodeUpperCopy (const void *t)
{
  /* create a key of the form (forkNode, lower) */
  ttup *tup = (ttup *)t;
  void *ret = private_malloc(2*sizeof(timestmp));
  timestmp root = (timestmp) pow(2, floor(log2((double) tup->te)));

  *((timestmp*)ret) = forkNode(root, tup->ts, tup->te);
  *((timestmp*)(ret+sizeof(timestmp))) = tup->te;

  return ret;
}

static timestmp forkNode(timestmp root, timestmp lower, timestmp upper)
{
  timestmp node = root;
  timestmp step;
  for(step = node/2; step >= 1; step /= 2)
  {
    if(upper < node)
      node -= step;
    else if(node < lower)
      node += step;
    else
      break;
  }
  return node;
}

static void arithmeticLeftRightNodes(List **leftQueries, List **rightQueries, timestmp lower, timestmp upper, timestmp forknode, timestmp root)
{
  timestmp node = root;
  timestmp step = node;
  timestmp *key;
  timestmp tmp_step;

  timestmp fn_lower = lower;
  timestmp fn_upper = upper;
  *leftQueries = NIL;
  *rightQueries = NIL;

  if(forknode < 2*root)
  {
    node = root;
    step = node/2;
    for( ; node!=forknode; step /= 2)
    {
      if(node < forknode)
      {
        key = private_malloc(2*sizeof(timestmp));
        key[0] = node;
        key[1] = node;
        *leftQueries = lappend(*leftQueries, key);
        node += step;
      }
      else
      {
        key = private_malloc(sizeof(timestmp));
        key[0] = node;
        *rightQueries = lappend(*rightQueries, key);
        node -= step;
      }
    }
  }

  tmp_step = step;
  /* node == forknode */

  if(fn_lower < 2 * root)
  {
    for( ; node != fn_lower; step /= 2)
    {
      if(node < fn_lower)
      {
        key = private_malloc(2*sizeof(timestmp));
        key[0] = node;
        key[1] = node;
        *leftQueries = lappend(*leftQueries, key);
        node += step;
      }
      else
        node -= step;
    }
  }

  step = tmp_step;
  node = forknode;

  if(fn_upper < 2 * root)
  {
    for( ; node != fn_upper; step /= 2)
    {
      if(node > fn_upper)
      {
        key = private_malloc(sizeof(timestmp));
        key[0] = node;
        *rightQueries = lappend(*rightQueries, key);
        node -= step;
      }
      else
        node += step;
    }
  }

  /* Finally, put lower and upper */
  key = private_malloc(2*sizeof(timestmp));
  key[0] = fn_lower;
  key[1] = fn_upper;
  *leftQueries = lappend(*leftQueries, key);
}

void rit_UpDownJoin_ORIGINAL(RIT *outer_rit, RIT *inner_rit, long *match, long *fhits)
{
  List *leftQueries, *rightQueries;
  arithmeticUpDownLeftRightNodes(&leftQueries, &rightQueries, outer_rit, inner_rit);

  /*
   * SELECT lr.id, us.id
   * FROM leftQueries q, lowerIndex_R lr, upperIndex_S us
   * WHERE lr.node = q.fork
   *       AND us.node BETWEEN q.from AND q.to
   *       AND lr.lower <= us.upper
   */
  List *q = leftQueries;
  BTree *lr = outer_rit->lowerIdx;
  BTree *us = inner_rit->upperIdx;
  btree_leaf_node *lr_leaf, *us_leaf;
  timestmp lr_key[2];
  timestmp us_key[2];
  ListCell *q_cell;
  foreach(q_cell, q)
  {
    timestmp q_fork = ((timestmp*)lfirst(q_cell))[0];
    timestmp q_from = ((timestmp*)lfirst(q_cell))[1];
    timestmp q_to = ((timestmp*)lfirst(q_cell))[2];

    lr_key[0] = q_fork;
    lr_key[1] = 0;
    for(lr_leaf = btree_findLargerEqualLeaf(lr, lr_key); lr_leaf != NULL; lr_leaf = lr_leaf->next)
    {
      timestmp lr_node = ((timestmp*)lr_leaf->n.key)[0];
      timestmp lr_lower = ((timestmp*)lr_leaf->n.key)[1];
      if(lr_node !=  q_fork)
        break;

      us_key[0] = q_from;
      us_key[1] = lr_lower;
      for(us_leaf = btree_findLargerEqualLeaf(us, us_key); us_leaf != NULL; us_leaf = us_leaf->next)
      {
        timestmp us_node = ((timestmp*)us_leaf->n.key)[0];
        timestmp us_upper = ((timestmp*)us_leaf->n.key)[1];
        if(us_node > q_to)
          break;

        if(lr_lower <= us_upper)
        {
          /* join lr and us tuples */
          ListCell *outer_cell, *inner_cell;
          ttup *outer_tup, *inner_tup;
          foreach(outer_cell, lr_leaf->tups)
          {
            outer_tup = (ttup *)lfirst(outer_cell);
            foreach(inner_cell, us_leaf->tups)
            {
              inner_tup = (ttup *)lfirst(inner_cell);
              if(outer_tup->ts <= inner_tup->te && outer_tup->te >= inner_tup->ts)
              {
                (*match)++;
              }
              else
                (*fhits)++;
            }
          }
        }
        else
        {
          /* skip */
        }
      }
    }
  }

  /* UNION ALL */

  /*
   * SELECT ur.id, ls.id
   * FROM rightQueries q, upperIndex_R ur, lowerIndex_S ls
   * WHERE ur.node = q.fork
   *       AND ls.node BETWEEN q.from AND q.to
   *       AND ur.upper >= ls.lower
   */
  q = rightQueries;
  BTree *ur = outer_rit->upperIdx;
  BTree *ls = inner_rit->lowerIdx;
  btree_leaf_node *ur_leaf, *ls_leaf;
  timestmp ur_key[2];
  timestmp ls_key[2];
  foreach(q_cell, q)
  {
    timestmp q_fork = ((timestmp*)lfirst(q_cell))[0];
    timestmp q_from = ((timestmp*)lfirst(q_cell))[1];
    timestmp q_to = ((timestmp*)lfirst(q_cell))[2];

    ls_key[0] = q_from;
    ls_key[1] = 0;
    for(ls_leaf = btree_findLargerEqualLeaf(ls, ls_key); ls_leaf != NULL; ls_leaf = ls_leaf->next)
    {
      timestmp ls_node = ((timestmp*)ls_leaf->n.key)[0];
      timestmp ls_lower = ((timestmp*)ls_leaf->n.key)[1];
      if(ls_node >  q_to)
        break;

      ur_key[0] = q_fork;
      ur_key[1] = ls_lower;
      for(ur_leaf = btree_findLargerEqualLeaf(ur, ur_key); ur_leaf != NULL; ur_leaf = ur_leaf->next)
      {
        timestmp ur_node = ((timestmp*)ur_leaf->n.key)[0];
        timestmp ur_upper = ((timestmp*)ur_leaf->n.key)[1];

        if(ur_node != q_fork)
          break;

        if(ur_upper >= ls_lower)
        {
          /* join ur and ls tuples */
          ListCell *outer_cell, *inner_cell;
          ttup *outer_tup, *inner_tup;
          foreach(outer_cell, ur_leaf->tups)
          {
            outer_tup = (ttup *)lfirst(outer_cell);
            foreach(inner_cell, ls_leaf->tups)
            {
              inner_tup = (ttup *)lfirst(inner_cell);
              if(outer_tup->ts <= inner_tup->te && outer_tup->te >= inner_tup->ts)
              {
                (*match)++;
              }
              else
                (*fhits)++;
            }
          }
        }
        else
        {
          /* skip */
        }
      }
    }
  }

  /* UNION ALL */

  /*
   * SELECT lr.id, ls.id
   * FROM lowerIndex_R lr, lowerIndex_S ls
   * WHERE lr.node = ls.node
   */
  lr = outer_rit->lowerIdx;
  ls = inner_rit->lowerIdx;
  lr_key[0] = 0;
  lr_key[1] = 0;

  for(lr_leaf = btree_findLargerEqualLeaf(lr, lr_key); lr_leaf != NULL; lr_leaf = lr_leaf->next)
  {
    timestmp lr_node = ((timestmp*)lr_leaf->n.key)[0];

    ls_key[0] = lr_node;
    ls_key[1] = 0;
    for(ls_leaf = btree_findLargerEqualLeaf(ls, ls_key); ls_leaf != NULL; ls_leaf = ls_leaf->next)
    {
      timestmp ls_node = ((timestmp*)ls_leaf->n.key)[0];
      if(ls_node !=  lr_node)
        break;

      /* join lr and ls tuples */
     ListCell *outer_cell, *inner_cell;
     ttup *outer_tup, *inner_tup;
     foreach(outer_cell, lr_leaf->tups)
     {
       outer_tup = (ttup *)lfirst(outer_cell);
       foreach(inner_cell, ls_leaf->tups)
       {
         inner_tup = (ttup *)lfirst(inner_cell);
         if(outer_tup->ts <= inner_tup->te && outer_tup->te >= inner_tup->ts)
         {
           (*match)++;
         }
         else
           (*fhits)++;
       }
     }
    }
  }


  ListCell*cell;
  foreach(cell, leftQueries)
  {
    private_free(lfirst(cell));
  }

  foreach(cell, rightQueries)
  {
    private_free(lfirst(cell));
  }

  list_free(leftQueries);
  list_free(rightQueries);
}

static void arithmeticUpDownLeftRightNodes(List **leftQueries, List **rightQueries, RIT *outer_rit, RIT *inner_rit)
{
  timestmp node;
  timestmp step;
  timestmp *key;
  timestmp tmp_step;
  btree_leaf_node *iter_node;

  timestmp fn_lower;
  timestmp fn_upper;
  *leftQueries = NIL;
  *rightQueries = NIL;
  timestmp forknode;

  /*
   * SELECT DISTINCT node
   * FROM lowerIndex_R
   */
  timestmp min_key[2];
  min_key[0] = 0;
  min_key[1] = 0;
  /*
   * for each node
   */
  forknode = -1;
  for(iter_node = btree_findLargerEqualLeaf(outer_rit->lowerIdx, min_key); iter_node != NULL; iter_node=iter_node->next)
  {
    if(((timestmp *)iter_node->n.key)[0] == forknode)
      continue;
    forknode = ((timestmp*)iter_node->n.key)[0];

    /*
     * static determination of the maximum interval of a fork node
     */
    node = outer_rit->root;
    step = node;
    while(node != forknode)
    {
      step /= 2;
      if(node < forknode)
        node += step;
      else
        node -= step;
    }
    fn_lower = node - step + 1;
    fn_upper = node + step - 1;

    /* up nodes */
    if(forknode < 2 * inner_rit->root)
    {
      node = inner_rit->root;
      step = node/2;
      for( ; node!=forknode; step /= 2)
      {
        if(node < forknode)
        {
          key = private_malloc(3*sizeof(timestmp));
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          *leftQueries = lappend(*leftQueries, key);
          node += step;
        }
        else
        {
          key = private_malloc(3*sizeof(timestmp));
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          *rightQueries = lappend(*rightQueries, key);
          node -= step;
        }
      }
    }

    /* Down node ranges */
    tmp_step = step;
    /* node == forknode */

    if(fn_lower < 2 * inner_rit->root)
    {
      for( ; node != fn_lower; step /= 2)
      {
        if(node < fn_lower)
        {
          key = private_malloc(3*sizeof(timestmp));
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          *leftQueries = lappend(*leftQueries, key);
          node += step;
        }
        else
          node -= step;
      }
      key = private_malloc(3*sizeof(timestmp));
      key[0] = forknode;
      key[1] = fn_lower;
      key[2] = forknode-1;
      *leftQueries = lappend(*leftQueries, key);
    }

    step = tmp_step;
    node = forknode;

    if(fn_upper < 2 * inner_rit->root)
    {
      for( ; node != fn_upper; step /= 2)
      {
        if(node > fn_upper)
        {
          key = private_malloc(3*sizeof(timestmp));
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          *rightQueries = lappend(*rightQueries, key);
          node -= step;
        }
        else
          node += step;
      }
      key = private_malloc(3*sizeof(timestmp));
      key[0] = forknode;
      key[1] = forknode + 1;
      key[2] = fn_upper;
      *rightQueries = lappend(*rightQueries, key);
    }
  }
}


void rit_UpDownJoin(RIT *outer_rit, RIT *inner_rit, long *match, long *fhits)
{
  timestmp node;
  timestmp step;
  timestmp key[3];
  timestmp tmp_step;
  btree_leaf_node *iter_node;

  timestmp fn_lower;
  timestmp fn_upper;
  timestmp forknode;

  BTree *lr, *us, *ur, *ls;
  btree_leaf_node *lr_leaf, *us_leaf, *ur_leaf, *ls_leaf;
  timestmp lr_key[2], us_key[2], ur_key[2], ls_key[2];

  timestmp q_fork;
  timestmp q_from;
  timestmp q_to;

  int return_label = 0;



  /*
   * SELECT DISTINCT node
   * FROM lowerIndex_R
   */
  timestmp min_key[2];
  min_key[0] = 0;
  min_key[1] = 0;
  /*
   * for each node
   */
  forknode = -1;
  for(iter_node = btree_findLargerEqualLeaf(outer_rit->lowerIdx, min_key); iter_node != NULL; iter_node=iter_node->next)
  {
    if(((timestmp *)iter_node->n.key)[0] == forknode)
      continue;
    forknode = ((timestmp*)iter_node->n.key)[0];

    /*
     * static determination of the maximum interval of a fork node
     */
    node = outer_rit->root;
    step = node;
    while(node != forknode)
    {
      step /= 2;
      if(node < forknode)
        node += step;
      else
        node -= step;
    }
    fn_lower = node - step + 1;
    fn_upper = node + step - 1;

    /* up nodes */
    if(forknode < 2 * inner_rit->root)
    {
      node = inner_rit->root;
      step = node/2;
      for( ; node!=forknode; step /= 2)
      {
        if(node < forknode)
        {
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          //*leftQueries = lappend(*leftQueries, key);
          return_label = 1;
          goto left_query_join;
return_left_join_1:
          node += step;
        }
        else
        {
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          //*rightQueries = lappend(*rightQueries, key);
          return_label = 1;
          goto right_query_join;
return_right_join_1:
          node -= step;
        }
      }
    }

    /* Down node ranges */
    tmp_step = step;
    /* node == forknode */

    if(fn_lower < 2 * inner_rit->root)
    {
      for( ; node != fn_lower; step /= 2)
      {
        if(node < fn_lower)
        {
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          //*leftQueries = lappend(*leftQueries, key);
          return_label = 2;
          goto left_query_join;
return_left_join_2:
          node += step;
        }
        else
          node -= step;
      }
      key[0] = forknode;
      key[1] = fn_lower;
      key[2] = forknode-1;
      //*leftQueries = lappend(*leftQueries, key);
      return_label = 3;
      goto left_query_join;
return_left_join_3: ;
    }

    step = tmp_step;
    node = forknode;

    if(fn_upper < 2 * inner_rit->root)
    {
      for( ; node != fn_upper; step /= 2)
      {
        if(node > fn_upper)
        {
          key[0] = forknode;
          key[1] = node;
          key[2] = node;
          //*rightQueries = lappend(*rightQueries, key);
          return_label = 2;
          goto right_query_join;
return_right_join_2:
          node -= step;
        }
        else
          node += step;
      }
      key[0] = forknode;
      key[1] = forknode + 1;
      key[2] = fn_upper;
      //*rightQueries = lappend(*rightQueries, key);
      return_label = 3;
      goto right_query_join;
return_right_join_3: ;
    }
  }

  goto index_index_join;

left_query_join:
  /*
   * SELECT lr.id, us.id
   * FROM leftQueries q, lowerIndex_R lr, upperIndex_S us
   * WHERE lr.node = q.fork
   *       AND us.node BETWEEN q.from AND q.to
   *       AND lr.lower <= us.upper
   */
  lr = outer_rit->lowerIdx;
  us = inner_rit->upperIdx;

  q_fork = key[0];
  q_from = key[1];
  q_to = key[2];

  lr_key[0] = q_fork;
  lr_key[1] = 0;
  for(lr_leaf = btree_findLargerEqualLeaf(lr, lr_key); lr_leaf != NULL; lr_leaf = lr_leaf->next)
  {
    timestmp lr_node = ((timestmp*)lr_leaf->n.key)[0];
    timestmp lr_lower = ((timestmp*)lr_leaf->n.key)[1];
    if(lr_node !=  q_fork)
      break;

    us_key[0] = q_from;
    us_key[1] = lr_lower;
    for(us_leaf = btree_findLargerEqualLeaf(us, us_key); us_leaf != NULL; us_leaf = us_leaf->next)
    {
      timestmp us_node = ((timestmp*)us_leaf->n.key)[0];
      timestmp us_upper = ((timestmp*)us_leaf->n.key)[1];
      if(us_node > q_to)
        break;

      if(lr_lower <= us_upper)
      {
        /* join lr and us tuples */
        ListCell *outer_cell, *inner_cell;
        ttup *outer_tup, *inner_tup;
        ttup *physical_outer_tup, *physical_inner_tup;
        foreach(outer_cell, lr_leaf->tups)
        {
          outer_tup = (ttup *)lfirst(outer_cell);
          if(outer_rit->onDisk)
            physical_outer_tup = ExecHeapAmFetch(outer_rit->rel, *(HTID*)outer_tup->data);
          else
            physical_outer_tup = outer_tup;
          foreach(inner_cell, us_leaf->tups)
          {
            inner_tup = (ttup *)lfirst(inner_cell);
            if(inner_rit->onDisk)
              physical_inner_tup = ExecHeapAmFetch(inner_rit->rel, *(HTID*)inner_tup->data);
            else
              physical_inner_tup = inner_tup;
            if(physical_outer_tup->ts <= physical_inner_tup->te && physical_outer_tup->te >= physical_inner_tup->ts)
            {
              (*match)++;
            }
            else
              (*fhits)++;
          }
        }
      }
      else
      {
        /* skip */
      }
    }
  }

  switch (return_label) {
    case 1:
      goto return_left_join_1;
      break;
    case 2:
      goto return_left_join_2;
      break;
    case 3:
      goto return_left_join_3;
      break;
    default:
      printf("Label error!!\n");
      break;
  }

right_query_join:

  /*
   * SELECT ur.id, ls.id
   * FROM rightQueries q, upperIndex_R ur, lowerIndex_S ls
   * WHERE ur.node = q.fork
   *       AND ls.node BETWEEN q.from AND q.to
   *       AND ur.upper >= ls.lower
   */

  ur = outer_rit->upperIdx;
  ls = inner_rit->lowerIdx;
  q_fork = key[0];
  q_from = key[1];
  q_to = key[2];
  ls_key[0] = q_from;
  ls_key[1] = 0;
  for(ls_leaf = btree_findLargerEqualLeaf(ls, ls_key); ls_leaf != NULL; ls_leaf = ls_leaf->next)
  {
    timestmp ls_node = ((timestmp*)ls_leaf->n.key)[0];
    timestmp ls_lower = ((timestmp*)ls_leaf->n.key)[1];
    if(ls_node >  q_to)
      break;

    ur_key[0] = q_fork;
    ur_key[1] = ls_lower;
    for(ur_leaf = btree_findLargerEqualLeaf(ur, ur_key); ur_leaf != NULL; ur_leaf = ur_leaf->next)
    {
      timestmp ur_node = ((timestmp*)ur_leaf->n.key)[0];
      timestmp ur_upper = ((timestmp*)ur_leaf->n.key)[1];

      if(ur_node != q_fork)
        break;

      if(ur_upper >= ls_lower)
      {
        /* join ur and ls tuples */
        ListCell *outer_cell, *inner_cell;
        ttup *outer_tup, *inner_tup;
        ttup *physical_outer_tup, *physical_inner_tup;
        foreach(outer_cell, ur_leaf->tups)
        {
          outer_tup = (ttup *)lfirst(outer_cell);
          if(outer_rit->onDisk)
            physical_outer_tup = ExecHeapAmFetch(outer_rit->rel, *(HTID*)outer_tup->data);
          else
            physical_outer_tup = outer_tup;
          foreach(inner_cell, ls_leaf->tups)
          {
            inner_tup = (ttup *)lfirst(inner_cell);
            if(inner_rit->onDisk)
              physical_inner_tup = ExecHeapAmFetch(inner_rit->rel, *(HTID*)inner_tup->data);
            else
              physical_inner_tup = inner_tup;
            if(physical_outer_tup->ts <= physical_inner_tup->te && physical_outer_tup->te >= physical_inner_tup->ts)
            {
              (*match)++;
            }
            else
              (*fhits)++;
          }
        }
      }
      else
      {
        /* skip */
      }
    }
  }

  switch (return_label) {
    case 1:
      goto return_right_join_1;
      break;
    case 2:
      goto return_right_join_2;
      break;
    case 3:
      goto return_right_join_3;
      break;
    default:
      printf("Label error!!\n");
      break;
  }

index_index_join:
  /*
   * SELECT lr.id, ls.id
   * FROM lowerIndex_R lr, lowerIndex_S ls
   * WHERE lr.node = ls.node
   */
  lr = outer_rit->lowerIdx;
  ls = inner_rit->lowerIdx;
  lr_key[0] = 0;
  lr_key[1] = 0;

  for(lr_leaf = btree_findLargerEqualLeaf(lr, lr_key); lr_leaf != NULL; lr_leaf = lr_leaf->next)
  {
    timestmp lr_node = ((timestmp*)lr_leaf->n.key)[0];

    ls_key[0] = lr_node;
    ls_key[1] = 0;
    for(ls_leaf = btree_findLargerEqualLeaf(ls, ls_key); ls_leaf != NULL; ls_leaf = ls_leaf->next)
    {
      timestmp ls_node = ((timestmp*)ls_leaf->n.key)[0];
      if(ls_node !=  lr_node)
        break;

      /* join lr and ls tuples */
     ListCell *outer_cell, *inner_cell;
     ttup *outer_tup, *inner_tup;
     ttup *physical_outer_tup, *physical_inner_tup;
     foreach(outer_cell, lr_leaf->tups)
     {
       outer_tup = (ttup *)lfirst(outer_cell);
       if(outer_rit->onDisk)
         physical_outer_tup = ExecHeapAmFetch(outer_rit->rel, *(HTID*)outer_tup->data);
       else
         physical_outer_tup = outer_tup;
       foreach(inner_cell, ls_leaf->tups)
       {
         inner_tup = (ttup *)lfirst(inner_cell);
         if(inner_rit->onDisk)
           physical_inner_tup = ExecHeapAmFetch(inner_rit->rel, *(HTID*)inner_tup->data);
         else
           physical_inner_tup = inner_tup;
         if(physical_outer_tup->ts <= physical_inner_tup->te && physical_outer_tup->te >= physical_inner_tup->ts)
         {
           (*match)++;
         }
         else
           (*fhits)++;
       }
     }
    }
  }
}
