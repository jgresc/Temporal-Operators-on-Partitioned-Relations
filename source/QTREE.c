#include "QTREE.h"
#include <math.h>
#include "stdio.h"

static void qtree_insert_rec(QTREE *qtree, qtree_node *node, ttup* tup);
static void qtree_insert_rec_density_based_split(QTREE *qtree, qtree_node *node, ttup* tup);
static void qtree_insert_rec_disk(QTREE *qtree, PartitionAm *storage, qtree_node *node, ttup* tup);
static void qtree_insert_rec_disk_density_based_split(QTREE *qtree, PartitionAm *storage, qtree_node *node, ttup* tup);
static void qtree_cluster_memory(QTREE *qtree);

QTREE *qtree_create(List *rel, timestmp lower, timestmp upper, size_t loose, int T)
{
  if(loose < 1)
    return NULL;

  QTREE *ret = private_malloc(sizeof(QTREE));
  ListCell *tup_cell;

  ret->loose = loose;
  ret->T = T;
  ret->root = private_malloc(sizeof(qtree_node));
  size_t length = upper-lower+1;
  /* adjust to a power of two */
  length = (size_t) pow(2, ceil(log2((double)length)));
  ret->root->lower = lower - (length*ret->loose - length)/2;
  ret->root->upper = lower + length - 1 + (length*ret->loose - length)/2;
  ret->root->tups = NIL;
  ret->root->left = NULL;
  ret->root->right = NULL;

  ttup *tup;
  foreach(tup_cell, rel)
  {
    tup = (ttup *)lfirst(tup_cell);
    qtree_insert_rec_density_based_split(ret, ret->root, tup);
  }

  qtree_cluster_memory(ret);

  return ret;
}

QTREE *qtree_create_disk(HeapAm *rel, PartitionAm *storage, timestmp lower, timestmp upper, size_t loose, int T)
{
  if(loose < 1)
    return NULL;

  QTREE *ret = private_malloc(sizeof(QTREE));

  ret->loose = loose;
  ret->T = T;
  ret->root = private_malloc(sizeof(qtree_node));
  size_t length = upper-lower+1;
  /* adjust to a power of two */
  length = (size_t) pow(2, ceil(log2((double)length)));
  ret->root->lower = lower - (length*ret->loose - length)/2;
  ret->root->upper = lower + length - 1 + (length*ret->loose - length)/2;
  ret->root->tups = NIL;
  ret->root->left = NULL;
  ret->root->right = NULL;

  ttup *tup;
  ExecHeapAmScanInit(rel);
  while((tup = ExecHeapAmScan(rel)) != NULL)
  {
    if(ret->T)
      qtree_insert_rec_disk_density_based_split(ret, storage, ret->root, tup);
    else
      qtree_insert_rec_disk(ret, storage, ret->root, tup);
  }

  return ret;
}

static void qtree_insert_rec(QTREE *qtree, qtree_node *node, ttup* tup)
{
  if(node->upper == node->lower) {
    if(node->lower != tup->ts || node->upper != tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);
    node->tups = lappend(node->tups, tup);
    return;
  }

  int put_left = 0;
  int put_right = 0;

  /*
   * calculate lower and upper w/o loose bound
   */
  timestmp node_lower_no_loose = node->lower + ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;
  timestmp node_upper_no_loose = node->upper - ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;

  /* check if tup fits into the left child */
  timestmp left_lower, left_upper;
  left_lower = node_lower_no_loose;
  left_upper = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1;
  /* add loose parts */
  size_t length = left_upper - left_lower + 1;
  left_lower -= (length*qtree->loose - length)/2;
  left_upper += (length*qtree->loose - length)/2;

  if(left_lower <= tup->ts && left_upper >= tup->te)
    put_left = 1;
  /* check if tup fits into the right child */
  timestmp right_lower, right_upper;
  right_lower = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1 + 1;
  right_upper = node_upper_no_loose;
  /* add loose parts */
  length = right_upper - right_lower + 1;
  right_lower -= (length*qtree->loose - length)/2;
  right_upper += (length*qtree->loose - length)/2;

  if(right_lower <= tup->ts && right_upper >= tup->te)
    put_right = 1;

  if(put_left && put_right)
  {
    if(left_upper - tup->te > tup->ts - right_lower)
      put_right = 0;
    else
      put_left = 0;
  }

  if(!put_left && !put_right)
  {
    if(node->lower > tup->ts || node->upper < tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);
    node->tups = lappend(node->tups, tup);
    return;
  }

  qtree_node *to_put;
  if(put_left)
  {
    if(node->left)
      to_put = node->left;
    else
    {
      node->left = private_malloc(sizeof(qtree_node));
      node->left->lower = left_lower;
      node->left->upper = left_upper;
      node->left->left = NULL;
      node->left->right = NULL;
      node->left->tups = NIL;
      to_put = node->left;
    }
  }
  if(put_right)
  {
    if(node->right)
      to_put = node->right;
    else
    {
      node->right = private_malloc(sizeof(qtree_node));
      node->right->lower = right_lower;
      node->right->upper = right_upper;
      node->right->left = NULL;
      node->right->right = NULL;
      node->right->tups = NIL;
      to_put = node->right;
    }
  }

  qtree_insert_rec(qtree, to_put, tup);
}

static void qtree_insert_rec_density_based_split(QTREE *qtree, qtree_node *node, ttup* tup)
{
  if(node->upper == node->lower) {
    if(node->lower != tup->ts || node->upper != tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);
    node->tups = lappend(node->tups, tup);
    return;
  }

  if(list_length(node->tups) < qtree->T)
  {
    /* simply insert */
    node->tups = lappend(node->tups, tup);
    return;
  }

  int put_left = 0;
  int put_right = 0;

  /*
   * calculate lower and upper w/o loose bound
   */
  timestmp node_lower_no_loose = node->lower + ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;
  timestmp node_upper_no_loose = node->upper - ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;

  /* check if tup fits into the left child */
  timestmp left_lower, left_upper;
  left_lower = node_lower_no_loose;
  left_upper = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1;
  /* add loose parts */
  size_t length = left_upper - left_lower + 1;
  left_lower -= (length*qtree->loose - length)/2;
  left_upper += (length*qtree->loose - length)/2;

  if(left_lower <= tup->ts && left_upper >= tup->te)
    put_left = 1;
  /* check if tup fits into the right child */
  timestmp right_lower, right_upper;
  right_lower = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1 + 1;
  right_upper = node_upper_no_loose;
  /* add loose parts */
  length = right_upper - right_lower + 1;
  right_lower -= (length*qtree->loose - length)/2;
  right_upper += (length*qtree->loose - length)/2;

  if(right_lower <= tup->ts && right_upper >= tup->te)
    put_right = 1;

  if(put_left && put_right)
  {
    if(left_upper - tup->te > tup->ts - right_lower)
      put_right = 0;
    else
      put_left = 0;
  }

  if(!put_left && !put_right)
  {
    if(node->lower > tup->ts || node->upper < tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);

    if(list_length(node->tups) > qtree->T)
    {
      /* already in overflow */
      node->tups = lappend(node->tups, tup);
      return;
    }
    else
    {
      /* try to find another tuple to send to the leafs */
      ttup *candidate;
      ListCell *tCell;
      int candidate_found = 0;
      foreach(tCell, node->tups)
      {
        candidate = lfirst(tCell);
        if(left_lower <= candidate->ts && left_upper >= candidate->te)
        {
          put_left = 1;
          lfirst(tCell) = tup;
          tup = candidate;
          candidate_found = 1;
          break;
        }
        else if(right_lower <= candidate->ts && right_upper >= candidate->te)
        {
          put_right = 1;
          lfirst(tCell) = tup;
          tup = candidate;
          candidate_found = 1;
          break;
        }
      }
      if(!candidate_found)
      {
        /* go to overflow */
        node->tups = lappend(node->tups, tup);
        return;
      }
    }
  }

  qtree_node *to_put;
  if(put_left)
  {
    if(node->left)
      to_put = node->left;
    else
    {
      node->left = private_malloc(sizeof(qtree_node));
      node->left->lower = left_lower;
      node->left->upper = left_upper;
      node->left->left = NULL;
      node->left->right = NULL;
      node->left->tups = NIL;
      to_put = node->left;
    }
  }
  if(put_right)
  {
    if(node->right)
      to_put = node->right;
    else
    {
      node->right = private_malloc(sizeof(qtree_node));
      node->right->lower = right_lower;
      node->right->upper = right_upper;
      node->right->left = NULL;
      node->right->right = NULL;
      node->right->tups = NIL;
      to_put = node->right;
    }
  }

  qtree_insert_rec_density_based_split(qtree, to_put, tup);
}

static void qtree_insert_rec_disk(QTREE *qtree, PartitionAm *storage, qtree_node *node, ttup* tup)
{
  if(node->upper == node->lower) {
    if(node->lower != tup->ts || node->upper != tup->te)
      printf("ERROR: something went wrong %s:%d\n", __FILE__, __LINE__);
    if(node->tups == NIL)
      node->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
    return;
  }

  int put_left = 0;
  int put_right = 0;

  /*
   * calculate lower and upper w/o loose bound
   */
  timestmp node_lower_no_loose = node->lower + ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;
  timestmp node_upper_no_loose = node->upper - ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;

  /* check if tup fits into the left child */
  timestmp left_lower, left_upper;
  left_lower = node_lower_no_loose;
  left_upper = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1;
  /* add loose parts */
  size_t length = left_upper - left_lower + 1;
  left_lower -= (length*qtree->loose - length)/2;
  left_upper += (length*qtree->loose - length)/2;

  if(left_lower <= tup->ts && left_upper >= tup->te)
    put_left = 1;
  /* check if tup fits into the right child */
  timestmp right_lower, right_upper;
  right_lower = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1 + 1;
  right_upper = node_upper_no_loose;
  /* add loose parts */
  length = right_upper - right_lower + 1;
  right_lower -= (length*qtree->loose - length)/2;
  right_upper += (length*qtree->loose - length)/2;

  if(right_lower <= tup->ts && right_upper >= tup->te)
    put_right = 1;

  if(put_left && put_right)
  {
    if(left_upper - tup->te > tup->ts - right_lower)
      put_right = 0;
    else
      put_left = 0;
  }

  if(!put_left && !put_right)
  {
    if(node->lower > tup->ts || node->upper < tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);
    if(node->tups == NIL)
      node->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
    return;
  }

  qtree_node *to_put;
  if(put_left)
  {
    if(node->left)
      to_put = node->left;
    else
    {
      node->left = private_malloc(sizeof(qtree_node));
      node->left->lower = left_lower;
      node->left->upper = left_upper;
      node->left->left = NULL;
      node->left->right = NULL;
      node->left->tups = NIL;
      to_put = node->left;
    }
  }
  if(put_right)
  {
    if(node->right)
      to_put = node->right;
    else
    {
      node->right = private_malloc(sizeof(qtree_node));
      node->right->lower = right_lower;
      node->right->upper = right_upper;
      node->right->left = NULL;
      node->right->right = NULL;
      node->right->tups = NIL;
      to_put = node->right;
    }
  }

  qtree_insert_rec_disk(qtree, storage, to_put, tup);
}

static void qtree_insert_rec_disk_density_based_split(QTREE *qtree, PartitionAm *storage, qtree_node *node, ttup* tup)
{
  if(node->upper == node->lower) {
    if(node->lower != tup->ts || node->upper != tup->te)
      printf("ERROR: something went wrong %s:%d\n", __FILE__, __LINE__);
    if(node->tups == NIL)
      node->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
    return;
  }

  int put_left = 0;
  int put_right = 0;

  if(node->tups == NIL || (!ExecPartitionCheckHasOverflow(storage, lfirst(list_head(node->tups))) && !ExecPartitionCheckInsertCausesOverflow(storage, lfirst(list_head(node->tups)))))
  {
    /* Simply insert */
    if(node->tups == NIL)
      node->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
    return;
  }

  /*
   * calculate lower and upper w/o loose bound
   */
  timestmp node_lower_no_loose = node->lower + ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;
  timestmp node_upper_no_loose = node->upper - ((node->upper-node->lower+1)-(node->upper-node->lower+1)/qtree->loose)/2;

  /* check if tup fits into the left child */
  timestmp left_lower, left_upper;
  left_lower = node_lower_no_loose;
  left_upper = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1;
  /* add loose parts */
  size_t length = left_upper - left_lower + 1;
  left_lower -= (length*qtree->loose - length)/2;
  left_upper += (length*qtree->loose - length)/2;

  if(left_lower <= tup->ts && left_upper >= tup->te)
    put_left = 1;
  /* check if tup fits into the right child */
  timestmp right_lower, right_upper;
  right_lower = node_lower_no_loose + (node_upper_no_loose-node_lower_no_loose+1)/2 - 1 + 1;
  right_upper = node_upper_no_loose;
  /* add loose parts */
  length = right_upper - right_lower + 1;
  right_lower -= (length*qtree->loose - length)/2;
  right_upper += (length*qtree->loose - length)/2;

  if(right_lower <= tup->ts && right_upper >= tup->te)
    put_right = 1;

  if(put_left && put_right)
  {
    if(left_upper - tup->te > tup->ts - right_lower)
      put_right = 0;
    else
      put_left = 0;
  }

  ttup *tmp = NULL;
  if(!put_left && !put_right)
  {
    if(node->lower > tup->ts || node->upper < tup->te)
      printf("ERROR: something went wrong node=[%ld, %ld] tup=[%ld, %ld] %s:%d\n", node->lower, node->upper, tup->ts, tup->te, __FILE__, __LINE__);

    if(ExecPartitionCheckHasOverflow(storage, lfirst(list_head(node->tups))))
    {
      /* already in overflow */
      ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
      return;
    }
    else
    {
      /* try to find another tuple to send to the leafs */
      ExecPartitionAmScanInit(storage, lfirst(list_head(node->tups)));
      HTID candidate_tid;
      ttup *candidate, *tmp;
      bool candidate_found = 0;
      while((candidate = ExecPartitionAmScanWithTID(storage, &candidate_tid)) != NULL)
      {
        if(left_lower <= candidate->ts && left_upper >= candidate->te)
        {
          put_left = 1;
          ExecPartitionAmScanEnd(storage, lfirst(list_head(node->tups)));
          tmp = ttupdup(candidate, storage->tupsze);
          ExecPartitionAmReplace(storage, tup, candidate_tid);
          tup = tmp;
          candidate_found = 1;
          break;
        }
        else if(right_lower <= candidate->ts && right_upper >= candidate->te)
        {
          put_right = 1;
          ExecPartitionAmScanEnd(storage, lfirst(list_head(node->tups)));
          tmp = ttupdup(candidate, storage->tupsze);
          ExecPartitionAmReplace(storage, tup, candidate_tid);
          tup = tmp;
          candidate_found = 1;
          break;
        }
      }
      if(!candidate_found)
      {
        /* go to overflow */
        ExecPartitionAmInsert(storage, tup, lfirst(list_head(node->tups)));
        return;
      }
    }
  }

  qtree_node *to_put;
  if(put_left)
  {
    if(node->left)
      to_put = node->left;
    else
    {
      node->left = private_malloc(sizeof(qtree_node));
      node->left->lower = left_lower;
      node->left->upper = left_upper;
      node->left->left = NULL;
      node->left->right = NULL;
      node->left->tups = NIL;
      to_put = node->left;
    }
  }
  if(put_right)
  {
    if(node->right)
      to_put = node->right;
    else
    {
      node->right = private_malloc(sizeof(qtree_node));
      node->right->lower = right_lower;
      node->right->upper = right_upper;
      node->right->left = NULL;
      node->right->right = NULL;
      node->right->tups = NIL;
      to_put = node->right;
    }
  }

  qtree_insert_rec_disk_density_based_split(qtree, storage, to_put, tup);
  if(tmp)
    freeTTup(tmp);
}

static void destroyQtreeRec(QTREE *qtree, qtree_node *node)
{
  if(node == NULL)
    return;

  destroyQtreeRec(qtree, node->left);
  destroyQtreeRec(qtree, node->right);
  list_free(node->tups);
  private_free(node);
}

static void destroyQtreeRec_disk(QTREE *qtree, qtree_node *node)
{
  if(node == NULL)
    return;

  destroyQtreeRec_disk(qtree, node->left);
  destroyQtreeRec_disk(qtree, node->right);
  if(node->tups != NIL)
    private_free(lfirst(list_head(node->tups)));
  list_free(node->tups);
  private_free(node);
}

void qtree_close(QTREE *qtree)
{
  destroyQtreeRec(qtree, qtree->root);
  private_free(qtree);
}

void qtree_close_disk(QTREE *qtree)
{
  destroyQtreeRec_disk(qtree, qtree->root);
  private_free(qtree);
}

void
qtree_select(QTREE *qtree, timestmp lower, timestmp upper, long *match, long *fhits);

static void qtree_JoinInnerRec(qtree_node *outer_node, qtree_node *inner_node, long *match, long *fhits)
{
  /*
   * Join outer and inner node
   */
  if(list_length(outer_node->tups) > 0 && list_length(inner_node->tups) > 0)
  {
    ListCell *outer_cell, *inner_cell;
    foreach(outer_cell, outer_node->tups)
    {
      ttup *outer_tup = (ttup *) lfirst(outer_cell);
      foreach(inner_cell, inner_node->tups)
      {
        ttup *inner_tup = (ttup *) lfirst(inner_cell);
        if(outer_tup->ts <= inner_tup->te && outer_tup->te >= inner_tup->ts)
          (*match)++;
        else
          (*fhits)++;
      }
    }
  }

  /* recurse */
  if(inner_node->left && outer_node->lower <= inner_node->left->upper && outer_node->upper >= inner_node->left->lower)
    qtree_JoinInnerRec(outer_node, inner_node->left, match, fhits);
  if(inner_node->right && outer_node->lower <= inner_node->right->upper && outer_node->upper >= inner_node->right->lower)
    qtree_JoinInnerRec(outer_node, inner_node->right, match, fhits);
}

static void qtree_JoinOuterRec(qtree_node *outer_node, QTREE *inner_qtree, long *match, long *fhits)
{
  if(list_length(outer_node->tups) > 0 && outer_node->lower <= inner_qtree->root->upper && outer_node->upper >= inner_qtree->root->lower)
    qtree_JoinInnerRec(outer_node, inner_qtree->root, match, fhits);
  if(outer_node->left && outer_node->left->lower <= inner_qtree->root->upper && outer_node->left->upper >= inner_qtree->root->lower)
    qtree_JoinOuterRec(outer_node->left, inner_qtree, match, fhits);
  if(outer_node->right && outer_node->right->lower <= inner_qtree->root->upper && outer_node->right->upper >= inner_qtree->root->lower)
    qtree_JoinOuterRec(outer_node->right, inner_qtree, match, fhits);
}

void
qtree_Join(QTREE *outer_qtree, QTREE *inner_qtree, long *match, long *fhits)
{
  qtree_JoinOuterRec(outer_qtree->root, inner_qtree, match, fhits);
}

static void qtree_JoinInnerRec_disk(qtree_node *outer_node, PartitionAm *outer_storage, qtree_node *inner_node, PartitionAm *inner_storage, long *match, long *fhits)
{
  /*
   * Join outer and inner node
   */
  if(list_length(outer_node->tups) > 0 && list_length(inner_node->tups) > 0)
  {
     ttup *outer_tup, *inner_tup;
     ListCell *outer_tup_cell;
     /* fetch all tuples from the outer partition */
     ExecPartitionAmScanInit(outer_storage, lfirst(list_head(outer_node->tups)));
     outer_tup = ExecPartitionAmScan(outer_storage);
     while(outer_tup != NULL)
     {
       /* fetch all tuples from the outer partition */
       List *outer_block_tups = NIL;
       do {
         outer_block_tups = lappend(outer_block_tups, ttupdup(outer_tup, outer_storage->tupsze));
         outer_tup = ExecPartitionAmScan(outer_storage);
       } while(outer_tup != NULL);

       if(outer_block_tups == NIL)
         break;

       ExecPartitionAmScanInit(inner_storage, lfirst(list_head(inner_node->tups)));
       while((inner_tup = ExecPartitionAmScan(inner_storage)) != NULL)
       {
         /* join a single inner tuple with the page of outer tuples */
         foreach(outer_tup_cell, outer_block_tups)
         {
           if (inner_tup->ts <= ((ttup *)lfirst(outer_tup_cell))->te
               && inner_tup->te >= ((ttup *)lfirst(outer_tup_cell))->ts)
             (*match)++;
           else
             (*fhits)++;
         }
       }
       /* free tuples from the page */
       foreach(outer_tup_cell, outer_block_tups)
       {
         freeTTup(lfirst(outer_tup_cell));
       }
       list_free(outer_block_tups);
     }
  }

  /* recurse */
  if(inner_node->left && outer_node->lower <= inner_node->left->upper && outer_node->upper >= inner_node->left->lower)
    qtree_JoinInnerRec_disk(outer_node, outer_storage, inner_node->left, inner_storage, match, fhits);
  if(inner_node->right && outer_node->lower <= inner_node->right->upper && outer_node->upper >= inner_node->right->lower)
    qtree_JoinInnerRec_disk(outer_node, outer_storage, inner_node->right, inner_storage, match, fhits);
}

static void qtree_JoinOuterRec_disk(qtree_node *outer_node, PartitionAm *outer_storage, QTREE *inner_qtree, PartitionAm *inner_storage, long *match, long *fhits)
{
  if(list_length(outer_node->tups) > 0 && outer_node->lower <= inner_qtree->root->upper && outer_node->upper >= inner_qtree->root->lower)
    qtree_JoinInnerRec_disk(outer_node, outer_storage, inner_qtree->root, inner_storage, match, fhits);
  if(outer_node->left && outer_node->left->lower <= inner_qtree->root->upper && outer_node->left->upper >= inner_qtree->root->lower)
    qtree_JoinOuterRec_disk(outer_node->left, outer_storage, inner_qtree, inner_storage, match, fhits);
  if(outer_node->right && outer_node->right->lower <= inner_qtree->root->upper && outer_node->right->upper >= inner_qtree->root->lower)
    qtree_JoinOuterRec_disk(outer_node->right, outer_storage, inner_qtree, inner_storage, match, fhits);
}

void
qtree_Join_disk(QTREE *outer_qtree, PartitionAm *outer_storage, QTREE *inner_qtree, PartitionAm *inner_storage, long *match, long *fhits)
{
  qtree_JoinOuterRec_disk(outer_qtree->root, outer_storage, inner_qtree, inner_storage, match, fhits);
}

static void qtree_cluster_memory_rec(qtree_node *node)
{
  List *new_list = NIL;
  ListCell *tcell;
  foreach(tcell, node->tups)
    new_list = lappend(new_list, lfirst(tcell));
  list_free(node->tups);
  node->tups = new_list;

  if(node->left)
    qtree_cluster_memory_rec(node->left);
  if(node->right)
    qtree_cluster_memory_rec(node->right);
}

static void qtree_cluster_memory(QTREE *qtree)
{
  qtree_cluster_memory_rec(qtree->root);
}

static size_t qtree_num_partitions_rec(qtree_node *node)
{
  size_t nsubnodes = 0;
  if(node->left)
    nsubnodes = qtree_num_partitions_rec(node->left);
  if(node->right)
    nsubnodes += qtree_num_partitions_rec(node->right);
  return 1 + nsubnodes;
}

size_t qtree_num_partitions(QTREE *qtree)
{
  return qtree_num_partitions_rec(qtree->root);
}

