#include "OIP.h"
#include <stdio.h>

#include <pthread.h>

typedef struct BranchList {
  int j;
  List *l;
} BranchList;

typedef struct PartitionCell {
  int i;
  List *tups;
} PartitionCell;

static BranchList *new_branchList(int j, List *l);
static PartitionCell *new_partitioncell(int i, List *tups);

static timestmp glob_sort_d;
static timestmp glob_sort_o;

static int cmp_oip_tup(const void *t1, const void *t2)
{
  int i1 = (int)(((ttup *)t1)->ts-glob_sort_o)/glob_sort_d;
  int j1 = (int)(((ttup *)t1)->te-glob_sort_o)/glob_sort_d;
  int i2 = (int)(((ttup *)t2)->ts-glob_sort_o)/glob_sort_d;
  int j2 = (int)(((ttup *)t2)->te-glob_sort_o)/glob_sort_d;

  int ret = (j1 > j2) - (j1 < j2);
  if(ret == 0)
    ret = (i2 > i1) - (i2 < i1);
  return ret;
}

OIP *
oip_create(List *rel, int k,  timestmp d, timestmp o)
{
  ListCell *tup_cell, *branch_cell, *partition_cell;
  int i, j;
  ttup *tup;
  List *partition_list;

  OIP *ret = private_malloc(sizeof(OIP));
  ret->k = k;
  ret->d = d;
  ret->o = o;
  ret->l = NIL;

  /*
   * Sort relation by (j asc, i desc)
   */
  glob_sort_d = d;
  glob_sort_o = o;
 list_sort(rel, cmp_oip_tup);

  foreach(tup_cell, rel)
  {
    tup = ((ttup *)lfirst(tup_cell));
    i = (int) ((tup->ts-o)/d);
    j = (int) ((tup->te-o)/d);

    /* find corresponding branch list */
    foreach(branch_cell, ret->l)
    {
      if(((BranchList *)lfirst(branch_cell))->j <= j)
        break;
    }
    if(branch_cell != NULL && ((BranchList *)lfirst(branch_cell))->j==j)
    {
      /* found */
      /* do nothing */
    }
    else if(branch_cell == list_head(ret->l))
    {
      /* prepend new BranchList to list*/
      ret->l = lcons(new_branchList(j, NIL), ret->l);
      branch_cell = list_head(ret->l);
    }
    else if(branch_cell == NULL)
    {
      /* end of list */
      lappend(ret->l, new_branchList(j, NIL));
      branch_cell = list_tail(ret->l);
    }
    else
    {
      /* prepend new BranchList to cell */
      lappend_cell(ret->l, branch_cell, new_branchList(((BranchList *)lfirst(branch_cell))->j, ((BranchList *)lfirst(branch_cell))->l));
      ((BranchList *)lfirst(branch_cell))->j = j;
      ((BranchList *)lfirst(branch_cell))->l = NIL;
    }

    /* now branch_cell is the corresponding BranchList */
    partition_list = ((BranchList *)lfirst(branch_cell))->l;

    /* find corresponding branch list */
    foreach(partition_cell, partition_list)
    {
      if(((PartitionCell *)lfirst(partition_cell))->i >= i)
        break;
    }
    if(partition_cell != NULL && ((PartitionCell *)lfirst(partition_cell))->i==i)
    {
      /* found */
      /* do nothing */
    } else if(partition_cell == list_head(partition_list)) {
      /* prepend new PartitionCell to list*/
      partition_list = lcons(new_partitioncell(i, NIL), partition_list);
      partition_cell = list_head(partition_list);
    }
    else if(partition_cell == NULL)
    {
      /* end of list */
      lappend(partition_list, new_partitioncell(i, NIL));
      partition_cell = list_tail(partition_list);
    }
    else
    {
      /* prepend new partition to cell */
      lappend_cell(partition_list, partition_cell, new_partitioncell(((PartitionCell *)lfirst(partition_cell))->i, ((PartitionCell *)lfirst(partition_cell))->tups));
      ((PartitionCell *)lfirst(partition_cell))->i = i;
      ((PartitionCell *)lfirst(partition_cell))->tups = NIL;
    }
    ((PartitionCell *)lfirst(partition_cell))->tups = lappend(((PartitionCell *)lfirst(partition_cell))->tups, tup);
    ((BranchList *)lfirst(branch_cell))->l = partition_list;
  }

  return ret;
}

OIP *
oip_create_disk(HeapAm *rel, PartitionAm *storage, int k,  timestmp d, timestmp o)
{
  ListCell *branch_cell, *partition_cell;
  int i, j;
  ttup *tup;
  List *partition_list;
  Sort *sort;

  OIP *ret = private_malloc(sizeof(OIP));
  ret->k = k;
  ret->d = d;
  ret->o = o;
  ret->l = NIL;

  /*
   * Sort relation by (j asc, i desc)
   */
  glob_sort_d = d;
  glob_sort_o = o;
  sort = ExecInitSort(rel, cmp_oip_tup, rel->htupsze, WORK_MEM, rel->f->f->type);

  while((tup = ExecSort(sort)) != NULL)
  {
    i = (int) ((tup->ts-o)/d);
    j = (int) ((tup->te-o)/d);

    /* find corresponding branch list */
    foreach(branch_cell, ret->l)
    {
      if(((BranchList *)lfirst(branch_cell))->j <= j)
        break;
      fprintf(stderr, "Tuple out of order (%s:%d)!\n", __FILE__, __LINE__);
    }
    if(branch_cell != NULL && ((BranchList *)lfirst(branch_cell))->j==j)
    {
      /* found */
      /* do nothing */
    }
    else if(branch_cell == list_head(ret->l))
    {
      /* prepend new BranchList to list*/
      ret->l = lcons(new_branchList(j, NIL), ret->l);
      branch_cell = list_head(ret->l);
    }
    else if(branch_cell == NULL)
    {
      /* end of list */
      lappend(ret->l, new_branchList(j, NIL));
      branch_cell = list_tail(ret->l);
    }
    else
    {
      /* prepend new BranchList to cell */
      lappend_cell(ret->l, branch_cell, new_branchList(((BranchList *)lfirst(branch_cell))->j, ((BranchList *)lfirst(branch_cell))->l));
      ((BranchList *)lfirst(branch_cell))->j = j;
      ((BranchList *)lfirst(branch_cell))->l = NIL;
    }

    /* now branch_cell is the corresponding BranchList */
    partition_list = ((BranchList *)lfirst(branch_cell))->l;

    /* find corresponding branch list */
    foreach(partition_cell, partition_list)
    {
      if(((PartitionCell *)lfirst(partition_cell))->i >= i)
        break;
      fprintf(stderr, "Tuple out of order (%s:%d)!\n", __FILE__, __LINE__);
    }
    if(partition_cell != NULL && ((PartitionCell *)lfirst(partition_cell))->i==i)
    {
      /* found */
      /* do nothing */
    } else if(partition_cell == list_head(partition_list)) {
      /* prepend new PartitionCell to list*/
      partition_list = lcons(new_partitioncell(i, NIL), partition_list);
      partition_cell = list_head(partition_list);
      ((PartitionCell *)lfirst(partition_cell))->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    }
    else if(partition_cell == NULL)
    {
      /* end of list */
      lappend(partition_list, new_partitioncell(i, NIL));
      partition_cell = list_tail(partition_list);
      ((PartitionCell *)lfirst(partition_cell))->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    }
    else
    {
      /* prepend new partition to cell */
      lappend_cell(partition_list, partition_cell, new_partitioncell(((PartitionCell *)lfirst(partition_cell))->i, ((PartitionCell *)lfirst(partition_cell))->tups));
      ((PartitionCell *)lfirst(partition_cell))->i = i;
      ((PartitionCell *)lfirst(partition_cell))->tups = lappend(NIL, ExecPartitionAmMakePartition(storage));
    }
    ExecPartitionAmInsert(storage, tup, lfirst(list_head(((PartitionCell *)lfirst(partition_cell))->tups)));
    ((BranchList *)lfirst(branch_cell))->l = partition_list;
  }

  ExecEndSort(sort);

  return ret;
}


void oip_close(OIP *oip)
{
  ListCell *branch_cell, *partition_cell;
  BranchList *branch_list;
  PartitionCell *partition_node;
  List *l = oip->l;

  foreach(branch_cell, l)
  {
    branch_list = (BranchList *)lfirst(branch_cell);
    foreach(partition_cell, branch_list->l)
    {
      partition_node = (PartitionCell *)lfirst(partition_cell);
      list_free(partition_node->tups);
      private_free(partition_node);
    }
    list_free(branch_list->l);
    private_free(branch_list);
  }
  list_free(l);
  private_free(oip);
}

void
oip_close_disk(OIP *oip)
{
  ListCell *branch_cell, *partition_cell;
  BranchList *branch_list;
  PartitionCell *partition_node;
  List *l = oip->l;

  foreach(branch_cell, l)
  {
    branch_list = (BranchList *)lfirst(branch_cell);
    foreach(partition_cell, branch_list->l)
    {
      partition_node = (PartitionCell *)lfirst(partition_cell);
      private_free(lfirst(list_head(partition_node->tups)));
      list_free(partition_node->tups);
      private_free(partition_node);
    }
    list_free(branch_list->l);
    private_free(branch_list);
  }
  list_free(l);
  private_free(oip);
}

void oip_select(OIP *oip, timestmp lower, timestmp upper, long *match, long *fhits)
{
  ListCell *branch_cell, *partition_cell, *tup_cell;
  BranchList *branch_list;
  PartitionCell *partition_node;
  ttup *tup;
  int s, e;
  timestmp d = oip->d;
  timestmp o = oip->o;
  List *l = oip->l;

  s = (int)(lower-o)/d;
  e = (int)(upper-o)/d;

  foreach(branch_cell, l)
  {
    branch_list = (BranchList *)lfirst(branch_cell);
    if(branch_list->j < s)
      break;
    foreach(partition_cell, branch_list->l)
    {
      partition_node = (PartitionCell *)lfirst(partition_cell);
      if(partition_node->i > e)
        break;

      /* output */
      foreach(tup_cell, partition_node->tups)
      {
        tup = (ttup *)lfirst(tup_cell);
        if(tup->ts <= upper && tup->te >= lower)
          (*match)++;
        else
          (*fhits)++;
      }
    }
  }

}

void oip_join(OIP *outer_oip, OIP *inner_oip, long *match, long *fhits)
{
  ListCell *outer_branch_cell, *outer_partition_cell, *inner_branch_cell, *inner_partition_cell, *outer_tup_cell, *inner_tup_cell;
  BranchList *outer_branch_list, *inner_branch_list;
  PartitionCell *outer_partition_node, *inner_partition_node;
  timestmp lower, upper;
  int s, e;
  ttup *outer_tup, *inner_tup;

  foreach(outer_branch_cell, outer_oip->l)
  {
    outer_branch_list = (BranchList *)lfirst(outer_branch_cell);
    foreach(outer_partition_cell, outer_branch_list->l)
    {
      outer_partition_node = (PartitionCell *)lfirst(outer_partition_cell);
      lower = outer_oip->o + outer_partition_node->i * outer_oip->d;
      upper = outer_oip->o + (outer_branch_list->j + 1) * outer_oip->d - 1;
      s = (int) ((lower-inner_oip->o)/inner_oip->d);
      e = (int) ((upper-inner_oip->o)/inner_oip->d);

      foreach(inner_branch_cell, inner_oip->l)
      {
        inner_branch_list = (BranchList *)lfirst(inner_branch_cell);
        if(inner_branch_list->j < s)
          break;
        foreach(inner_partition_cell, inner_branch_list->l)
        {
          inner_partition_node = (PartitionCell *)lfirst(inner_partition_cell);
          if(inner_partition_node->i > e)
            break;

          /* output */
          foreach(outer_tup_cell, outer_partition_node->tups)
          {
            outer_tup = (ttup *)lfirst(outer_tup_cell);
            foreach(inner_tup_cell, inner_partition_node->tups)
            {
              inner_tup = (ttup *)lfirst(inner_tup_cell);
              if(inner_tup->ts <= outer_tup->te && inner_tup->te >= outer_tup->ts)
              {
            	  (*match)++;
/*
               fprintf(stderr,"O: [%ld,%ld) %s, I: [%ld,%ld)%s \n",
             		  ((ttup *)lfirst(outer_tup_cell))->ts, ((ttup *)lfirst(outer_tup_cell))->te, ((ttup *)lfirst(outer_tup_cell))->data,
						  inner_tup->ts, inner_tup->te, inner_tup->data);

*/
              }
              else
                (*fhits)++;
            }
          }
        }
      }
    }
  }
}


struct oip_thread {
  pthread_t id;

  List *branch_lists;
  OIP *outer_oip;
  OIP *inner_oip;

  pthread_mutex_t *lock_mutex;

  long *match;
  long *fhits;
};

/*
void oip_join_thread(void *oip_thread)
{
  struct oip_thread *thread = (struct oip_thread *)oip_thread;
  ListCell *outer_branch_cell, *outer_partition_cell, *inner_branch_cell, *inner_partition_cell, *outer_tup_cell, *inner_tup_cell;
  BranchList *outer_branch_list, *inner_branch_list;
  PartitionCell *outer_partition_node, *inner_partition_node;
  timestmp lower, upper;
  int s, e;
  ttup *outer_tup, *inner_tup;
  long match, fhits;

  match = 0;
  fhits = 0;

  foreach(outer_branch_cell, thread->branch_lists)
  {
    outer_branch_list = (BranchList *)lfirst(outer_branch_cell);
    foreach(outer_partition_cell, outer_branch_list->l)
    {
      outer_partition_node = (PartitionCell *)lfirst(outer_partition_cell);
      lower = thread->outer_oip->o + outer_partition_node->i * thread->outer_oip->d;
      upper = thread->outer_oip->o + (outer_branch_list->j + 1) * thread->outer_oip->d - 1;
      s = (int) ((lower-thread->inner_oip->o)/thread->inner_oip->d);
      e = (int) ((upper-thread->inner_oip->o)/thread->inner_oip->d);

      foreach(inner_branch_cell, thread->inner_oip->l)
      {
        inner_branch_list = (BranchList *)lfirst(inner_branch_cell);
        if(inner_branch_list->j < s)
          break;
        foreach(inner_partition_cell, inner_branch_list->l)
        {
          inner_partition_node = (PartitionCell *)lfirst(inner_partition_cell);
          if(inner_partition_node->i > e)
            break;

          // output
          foreach(outer_tup_cell, outer_partition_node->tups)
          {
            outer_tup = (ttup *)lfirst(outer_tup_cell);
            foreach(inner_tup_cell, inner_partition_node->tups)
            {
              inner_tup = (ttup *)lfirst(inner_tup_cell);
              if(inner_tup->ts <= outer_tup->te && inner_tup->te >= outer_tup->ts)
                match++;
              else
                fhits++;
            }
          }
        }
      }
    }
  }

  pthread_mutex_lock(thread->lock_mutex);
  (*thread->match) += match;
  (*thread->fhits) += fhits;
  pthread_mutex_unlock(thread->lock_mutex);

  fflush(stdout);
}*/

void oip_join_parallel(OIP *outer_oip, OIP *inner_oip, long *match, long *fhits, int num_threads)
{
  fprintf(stderr, "Not suported!\n");
  exit(0);
}

/*
void oip_join_parallel(OIP *outer_oip, OIP *inner_oip, long *match, long *fhits, int num_threads)
{
  ListCell *outer_branch_cell;
  BranchList *outer_branch_list;
  int i;

  pthread_mutex_t lock_mutex = PTHREAD_MUTEX_INITIALIZER;

  struct oip_thread *threads = private_malloc(num_threads * sizeof(struct oip_thread));
  for(i = 0; i < num_threads; i++)
  {
    threads[i].branch_lists = NIL;
    threads[i].fhits = fhits;
    threads[i].match = match;
    threads[i].inner_oip = inner_oip;
    threads[i].outer_oip = outer_oip;
    threads[i].lock_mutex = &lock_mutex;
  }

  i = 0;
  foreach(outer_branch_cell, outer_oip->l)
  {
    outer_branch_list = (BranchList *)lfirst(outer_branch_cell);
    threads[i % num_threads].branch_lists = lappend(threads[i % num_threads].branch_lists, outer_branch_list);
  }

  for(i = 0; i < num_threads; i++)
  {
    pthread_create(&threads[i].id, NULL, (void *) &oip_join_thread, &threads[i]);
  }

  for(i = 0; i < num_threads; i++)
  {
    pthread_join(threads[i].id, NULL);
  }
}*/

void oip_join_disk(OIP *outer_oip, PartitionAm *outer_storage, OIP *inner_oip, PartitionAm *inner_storage, long *match, long *fhits)
{
  ListCell *outer_branch_cell, *outer_partition_cell, *inner_branch_cell, *inner_partition_cell, *outer_tup_cell;
  BranchList *outer_branch_list, *inner_branch_list;
  PartitionCell *outer_partition_node, *inner_partition_node;
  timestmp lower, upper;
  int s, e;
  ttup *outer_tup, *inner_tup;

  foreach(outer_branch_cell, outer_oip->l)
  {
    outer_branch_list = (BranchList *)lfirst(outer_branch_cell);
    foreach(outer_partition_cell, outer_branch_list->l)
    {
      outer_partition_node = (PartitionCell *)lfirst(outer_partition_cell);
      lower = outer_oip->o + outer_partition_node->i * outer_oip->d;
      upper = outer_oip->o + (outer_branch_list->j + 1) * outer_oip->d - 1;
      s = (int) ((lower-inner_oip->o)/inner_oip->d);
      e = (int) ((upper-inner_oip->o)/inner_oip->d);

      /* fetch all tuples from the outer partition */
      ExecPartitionAmScanInit(outer_storage, lfirst(list_head(outer_partition_node->tups)));
      outer_tup = ExecPartitionAmScan(outer_storage);
      while(outer_tup != NULL)
      {
        /* fetch all tuples from the outer partition */
        List *outer_partition_tups = NIL;
        do {
          outer_partition_tups = lappend(outer_partition_tups, ttupdup(outer_tup, outer_storage->tupsze));
          outer_tup = ExecPartitionAmScan(outer_storage);
        } while(outer_tup != NULL);

        if(outer_partition_tups == NIL)
          break;

        /* iterate over the inner partitioning */
        foreach(inner_branch_cell, inner_oip->l)
        {
          inner_branch_list = (BranchList *) lfirst(inner_branch_cell);
          if (inner_branch_list->j < s)
            break;
          foreach(inner_partition_cell, inner_branch_list->l)
          {
            inner_partition_node = (PartitionCell *) lfirst(inner_partition_cell);
            if (inner_partition_node->i > e)
              break;

            ExecPartitionAmScanInit(inner_storage, lfirst(list_head(inner_partition_node->tups)));
            while((inner_tup = ExecPartitionAmScan(inner_storage)) != NULL)
            {
              /* join a single inner tuple with the outer tuples */
              foreach(outer_tup_cell, outer_partition_tups)
              {
                if (inner_tup->ts < ((ttup *)lfirst(outer_tup_cell))->te
                    && inner_tup->te > ((ttup *)lfirst(outer_tup_cell))->ts)
                {
                  (*match)++;
                 /* fprintf(stderr,"O: [%ld,%ld) %s, I: [%ld,%ld)%s \n",
                		  ((ttup *)lfirst(outer_tup_cell))->ts, ((ttup *)lfirst(outer_tup_cell))->te, ((ttup *)lfirst(outer_tup_cell))->data,
						  inner_tup->ts, inner_tup->te, inner_tup->data);*/
                }
                else
                  (*fhits)++;
              }
            }
          }
        }
        /* free tuples from the outer partition */
        foreach(outer_tup_cell, outer_partition_tups)
        {
          freeTTup(lfirst(outer_tup_cell));
        }
        list_free(outer_partition_tups);
      }
    }
  }
}

void oip_tuple_join(List *outer, OIP *inner_oip, long *match, long *fhits)
{
  ListCell *outer_cell, *inner_branch_cell, *inner_partition_cell, *inner_tup_cell;
  BranchList *inner_branch_list;
  PartitionCell *inner_partition_node;
  timestmp lower, upper;
  int s, e;
  ttup *outer_tup, *inner_tup;

  foreach(outer_cell, outer)
  {
    outer_tup = (ttup *)lfirst(outer_cell);

    lower = outer_tup->ts;
    upper = outer_tup->te;
    s = (int)(lower-inner_oip->o)/inner_oip->d;
    e = (int)(upper-inner_oip->o)/inner_oip->d;

    foreach(inner_branch_cell, inner_oip->l)
    {
      inner_branch_list = (BranchList *)lfirst(inner_branch_cell);
      if(inner_branch_list->j < s)
        break;
      foreach(inner_partition_cell, inner_branch_list->l)
      {
        inner_partition_node = (PartitionCell *)lfirst(inner_partition_cell);
        if(inner_partition_node->i > e)
          break;

        /* output */
        foreach(inner_tup_cell, inner_partition_node->tups)
        {
          inner_tup = (ttup *)lfirst(inner_tup_cell);
          if(inner_tup->ts <= outer_tup->te && inner_tup->te >= outer_tup->ts)
            (*match)++;
          else
            (*fhits)++;
        }
      }
    }
  }
}

void oip_print(List *l)
{
  ListCell *branch_cell, *partition_cell, *tup_cell;
  BranchList *branch_list;
  PartitionCell *partition_node;
  ttup *tup;

  foreach(branch_cell, l)
  {
    branch_list = (BranchList *)lfirst(branch_cell);
    fprintf(stderr, "j=%d", branch_list->j);
    foreach(partition_cell, branch_list->l)
    {
      partition_node = (PartitionCell *)lfirst(partition_cell);
      fprintf(stderr, "[i=%d]={", partition_node->i);
      foreach(tup_cell, partition_node->tups)
      {
        tup = (ttup *)lfirst(tup_cell);
        fprintf(stderr, "[%ld, %ld], ", tup->ts, tup->te);
      }
      fprintf(stderr, "}");
    }
    fprintf(stderr, "\n");
  }
}

size_t oip_num_partitions(OIP *oip)
{
  ListCell *oip_branch_cell;
  size_t ret = 0;
  foreach(oip_branch_cell, oip->l)
  {
    ret += list_length(((BranchList *)lfirst(oip_branch_cell))->l);
  }
  return ret;
}

static BranchList *new_branchList(int j, List *l)
{
  BranchList *ret = private_malloc(sizeof(BranchList));
  ret->j = j;
  ret->l = l;
  return ret;
}

static PartitionCell *new_partitioncell(int i, List *tups)
{
  PartitionCell *ret = private_malloc(sizeof(PartitionCell));
  ret->i = i;
  ret->tups = tups;
  return ret;
}
