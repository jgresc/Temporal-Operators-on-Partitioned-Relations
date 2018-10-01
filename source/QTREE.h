#include "glob.h"
#include "list.h"
#include "ttup.h"
#include "nodeHeapAm.h"
#include "nodePartitionAM.h"

#ifndef QTREE_H_
#define QTREE_H_

typedef struct qtree_node {

  timestmp lower;
  timestmp upper;
  struct qtree_node *left;
  struct qtree_node *right;

  List *tups;

} qtree_node;

typedef struct QTREE {

  qtree_node *root;
  size_t loose; /* loose bounds */
  int T;

} QTREE;

QTREE *
qtree_create(List *rel, timestmp lower, timestmp upper, size_t loose, int T);
QTREE *
qtree_create_disk(HeapAm *rel, PartitionAm *storage, timestmp lower, timestmp upper, size_t loose, int T);
void
qtree_close(QTREE *qtree);
void
qtree_close_disk(QTREE *qtree);
void
qtree_select(QTREE *qtree, timestmp lower, timestmp upper, long *match, long *fhits);
void
qtree_Join(QTREE *outer_qtree, QTREE *inner_qtree, long *match, long *fhits);
void
qtree_Join_disk(QTREE *outer_qtree, PartitionAm *outer_storage, QTREE *inner_qtree, PartitionAm *inner_storage, long *match, long *fhits);


size_t
qtree_num_partitions(QTREE *qtree);
#endif /* QTREE_H_ */
