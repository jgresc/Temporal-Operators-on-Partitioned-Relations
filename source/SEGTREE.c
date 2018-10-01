/*
 * SEGTREE.c
 *
 *  Created on: Feb 12, 2014
 *      Author: dignoes
 */

#include "SEGTREE.h"
#include "ttup.h"

static void segtree_insert_rec(SegTreeNode *node, ttup *tup);
static void segtree_insert_rec_disk(SegTreeNode *node, ttup *tup, HTID *tid);

static int cmp_timestmp(const void *a, const void *b)
{
  return (*(timestmp*)a > *(timestmp*)b) - (*(timestmp*)a < *(timestmp*)b);
}

SEGTREE *segtree_create(List *rel)
{
  ListCell *lcell;
  List *points = NIL;
  SEGTREE *ret = private_calloc(1, sizeof(SEGTREE));
  SegTreeNode *cur, *child;

  if(!list_length(rel))
    return ret;

  foreach(lcell, rel)
  {
    ttup *tup = lfirst(lcell);
    points = lappend(points, &tup->ts);
    points = lappend(points, &tup->te);

  }
  points = list_sort(points, cmp_timestmp);

  /*
   * create leafs from distinct sorted points
   */

  ret->root = private_calloc(1, sizeof(SegTreeNode));
  cur = ret->root;
  foreach(lcell, points)
  {
    if(!lnext(lcell))
    {
      cur->lower = cur->upper = *(timestmp*)lfirst(lcell);
    }
    else if(cmp_timestmp(lfirst(lcell), lfirst(lnext(lcell))) != 0)
    {
      cur->lower = cur->upper = *(timestmp*)lfirst(lcell);
      cur->next = private_calloc(1, sizeof(SegTreeNode));
      cur = cur->next;
    }
  }
  list_free(points);

  /*
   * Create tree from leafs
   */
  while(ret->root->next)
  {
    child = ret->root;
    cur = private_calloc(1, sizeof(SegTreeNode));
    ret->root = cur;
    while(child)
    {
      if(!cur->left)
      {
        cur->left = child;
        cur->lower = child->lower;
        cur->upper = child->upper;
      }
      else if(!cur->right)
      {
        cur->right = child;
        cur->upper = child->upper;
        if(child->next)
        {
          cur->next = private_calloc(1, sizeof(SegTreeNode));
          cur = cur->next;
        }
      }
      child = child->next;
    }
  }

  /*
   * Add tuple
   */
  foreach(lcell, rel)
  {
    ttup *tup = lfirst(lcell);
    segtree_insert_rec(ret->root, tup);
  }

  return ret;
}

SEGTREE *segtree_create_disk(HeapAm *rel)
{
  ListCell *lcell;
  List *points = NIL;
  SEGTREE *ret = private_calloc(1, sizeof(SEGTREE));
  SegTreeNode *cur, *child;
  timestmp *ts, *te;
  HTID tid;

  ret->rel = rel;

  ExecHeapAmScanInit(rel);

  while(ExecHeapAmScan(rel))
  {
    ttup *tup = &rel->tup;
    ts = private_malloc(sizeof(timestmp));
    *ts = tup->ts;
    te = private_malloc(sizeof(timestmp));
    *te = tup->te;
    points = lappend(points, ts);
    points = lappend(points, te);
  }
  points = list_sort(points, cmp_timestmp);

  /*
   * create leafs from distinct sorted points
   */

  ret->root = private_calloc(1, sizeof(SegTreeNode));
  cur = ret->root;
  foreach(lcell, points)
  {
    if(!lnext(lcell))
    {
      cur->lower = cur->upper = *(timestmp*)lfirst(lcell);
    }
    else if(cmp_timestmp(lfirst(lcell), lfirst(lnext(lcell))) != 0)
    {
      cur->lower = cur->upper = *(timestmp*)lfirst(lcell);
      cur->next = private_calloc(1, sizeof(SegTreeNode));
      cur = cur->next;
    }
  }
  list_free_deep(points);

  /*
   * Create tree from leafs
   */
  while(ret->root->next)
  {
    child = ret->root;
    cur = private_calloc(1, sizeof(SegTreeNode));
    ret->root = cur;
    while(child)
    {
      if(!cur->left)
      {
        cur->left = child;
        cur->lower = child->lower;
        cur->upper = child->upper;
      }
      else if(!cur->right)
      {
        cur->right = child;
        cur->upper = child->upper;
        if(child->next)
        {
          cur->next = private_calloc(1, sizeof(SegTreeNode));
          cur = cur->next;
        }
      }
      child = child->next;
    }
  }

  /*
   * Add tuple
   */
  ExecHeapAmScanInit(rel);

  while(ExecHeapAmScanWithTID(rel, &tid))
  {
    ttup *tup = &rel->tup;
    HTID *ttid = private_malloc(sizeof(HTID));
    *ttid = tid;
    segtree_insert_rec_disk(ret->root, tup, ttid);
  }

  return ret;
}

static void segtree_insert_rec_disk(SegTreeNode *node, ttup *tup, HTID *tid)
{
    if(tup->ts <= node->lower && tup->te >= node->upper)
    {
      /* insert and stop */
      node->tups = lappend(node->tups, tid);
      return;
    }

  /*
   * if tup overlaps children insert recursively
   */
  if(node->left && tup->ts <= node->left->upper && tup->te >= node->left->lower)
    segtree_insert_rec_disk(node->left, tup, tid);
  if(node->right && tup->ts <= node->right->upper && tup->te >= node->right->lower)
    segtree_insert_rec_disk(node->right, tup, tid);
}

static void segtree_insert_rec(SegTreeNode *node, ttup *tup)
{
    if(tup->ts <= node->lower && tup->te >= node->upper)
    {
      /* insert and stop */
      node->tups = lappend(node->tups, tup);
      return;
    }

	/*
	 * if tup overlaps children insert recursively
	 */
	if(node->left && tup->ts <= node->left->upper && tup->te >= node->left->lower)
	  segtree_insert_rec(node->left, tup);
	if(node->right && tup->ts <= node->right->upper && tup->te >= node->right->lower)
	  segtree_insert_rec(node->right, tup);
}

void segtree_close(SEGTREE *segtree)
{
  SegTreeNode *branch = segtree->root;
  SegTreeNode *cur, *tofree;

  cur = branch;
  while(branch != NULL)
  {
	cur = branch;
	branch = branch->left;
    while(cur != NULL)
    {
      tofree = cur;
      cur = cur->next;
      list_free(tofree->tups);
      private_free(tofree);
    }
  }
  private_free(segtree);
}

int cmp_ptr(const void *a, const void *b)
{
  return (a > b) - (a < b);
}

void segtree_Query_WITH_SORT(SEGTREE *segtree, ttup *q, long *match, long *fhits)
{
    SegTreeNode *cur;
	SegTreeNode *branch = segtree->root;
    ListCell *lc;
	List *ptr_list = NIL;

	if(!branch || branch->lower > q->te || branch->upper < q->ts)
		return;
	while(branch != NULL)
	{
	  cur = branch;
	  do{
        /* append all tuple pointer to the result */
        foreach(lc, cur->tups)
		  ptr_list = lappend(ptr_list, lfirst(lc));
	    cur = cur->next;
	  } while(cur && cur->lower <= q->te && cur->upper >= q->ts);

	  if(branch->left && branch->left->lower <= q->te && branch->left->upper >= q->ts)
	    branch = branch->left;
	  else if(branch->right && branch->right->lower <= q->te && branch->right->upper >= q->ts)
		branch = branch->right;
	  else
		branch = NULL;
	}

	/* sort tuple pointers and skip duplicates */
    ptr_list = list_sort(ptr_list, cmp_ptr);
    foreach(lc, ptr_list)
    {
      if(!lnext(lc))
      {
        ttup *tup = lfirst(lc);
        if(tup->ts <= q->te && tup->te >= q->ts)
          (*match)++;
        else
          (*fhits)++;
      }
      else if(cmp_ptr(lfirst(lc), lfirst(lnext(lc))) != 0)
      {
        ttup *tup = lfirst(lc);
        if(tup->ts <= q->te && tup->te >= q->ts)
		  (*match)++;
		else
		  (*fhits)++;
      }
      else
      {
    	/* is duplicate */
    	(*fhits)++;
      }
    }
    list_free(ptr_list);
}

void segtree_Query_disk(SEGTREE *segtree, ttup *q, long *match, long *fhits)
{
  SegTreeNode *cur;
  SegTreeNode *branch = segtree->root;
  ListCell *lc;
  timestmp res_ts;

  ExecHeapAmScanEnd(segtree->rel);

  if(!branch || branch->lower > q->te || branch->upper < q->ts)
    return;
  while(branch != NULL)
  {
    cur = branch;
    do{
      /* check all tuples in the segment */
      foreach(lc, cur->tups)
      {
        ttup *tup = ExecHeapAmFetch(segtree->rel, *(HTID*)lfirst(lc));
        res_ts = MAX(q->ts, tup->ts);
        if(res_ts >= cur->lower)
          (*match)++; /* join */
        else
          (*fhits)++; /* duplicate */
      }
      cur = cur->next;
    } while(cur && cur->lower <= q->te && cur->upper >= q->ts);

    if(branch->left && branch->left->lower <= q->te && branch->left->upper >= q->ts)
      branch = branch->left;
    else if(branch->right && branch->right->lower <= q->te && branch->right->upper >= q->ts)
      branch = branch->right;
    else
      branch = NULL;
  }
}

void segtree_Query(SEGTREE *segtree, ttup *q, long *match, long *fhits)
{
    SegTreeNode *cur;
  SegTreeNode *branch = segtree->root;
    ListCell *lc;
  timestmp res_ts;

  if(!branch || branch->lower > q->te || branch->upper < q->ts)
    return;
  while(branch != NULL)
  {
    cur = branch;
    do{
      /* check all tuples in the segment */
      foreach(lc, cur->tups)
      {
        res_ts = MAX(q->ts, ((ttup*)lfirst(lc))->ts);
        if(res_ts >= cur->lower)
          (*match)++; /* join */
        else
          (*fhits)++; /* duplicate */
      }
      cur = cur->next;
    } while(cur && cur->lower <= q->te && cur->upper >= q->ts);

    if(branch->left && branch->left->lower <= q->te && branch->left->upper >= q->ts)
      branch = branch->left;
    else if(branch->right && branch->right->lower <= q->te && branch->right->upper >= q->ts)
    branch = branch->right;
    else
    branch = NULL;
  }
}

void segtree_Join_disk(HeapAm *outerrel, SEGTREE *inner_segtree, long *match, long *fhits)
{
  ExecHeapAmScanInit(outerrel);
  while(ExecHeapAmScan(outerrel))
    segtree_Query_disk(inner_segtree, &outerrel->tup, match, fhits);
}

void segtree_Join(List *outerrel, SEGTREE *inner_segtree, long *match, long *fhits)
{
  ListCell *lc;
  foreach(lc, outerrel)
    segtree_Query(inner_segtree, (ttup *)lfirst(lc), match, fhits);
}
