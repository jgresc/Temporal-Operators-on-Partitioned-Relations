/*
 * SEGTREE.h
 *
 *  Created on: Feb 12, 2014
 *      Author: dignoes
 */

#ifndef SEGTREE_H_
#define SEGTREE_H_

#include "glob.h"
#include "list.h"
#include "nodeHeapAm.h"

typedef struct SegTreeNode {

  timestmp lower;
  timestmp upper;

  struct SegTreeNode *next; /* left sibling */

  struct SegTreeNode *left;
  struct SegTreeNode *right;

  List *tups;
} SegTreeNode;

typedef struct SEGTREE {

	SegTreeNode *root;

	HeapAm *rel; /* for disk resident data */

} SEGTREE;

SEGTREE *
segtree_create(List *rel);
SEGTREE *
segtree_create_disk(HeapAm *rel);
void
segtree_close(SEGTREE *segtree);
void
segtree_Join(List *outerrel, SEGTREE *inner_segtree, long *match, long *fhits);
void
segtree_Join_disk(HeapAm *outerrel, SEGTREE *inner_segtree, long *match, long *fhits);

#endif /* SEGTREE_H_ */
