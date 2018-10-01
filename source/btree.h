#ifndef BTREE_H_
#define BTREE_H_

#include "list.h"
#include "glob.h"
#include "ttup.h"

typedef enum btree_node_type {
  UNKNOWN,
  BRANCHNODE,
  LEAFNODE
} btree_node_type;

struct btree_branch_node;

typedef struct btree_node {
  btree_node_type type;
  void *key;
  struct btree_branch_node *parent;

} btree_node;

typedef struct btree_branch_node {
  btree_node n;

  btree_node *left;
  btree_node *right;
} btree_branch_node;

typedef struct btree_leaf_node {
  btree_node n;

  struct btree_leaf_node *next;
  List *tups;

} btree_leaf_node;

typedef struct BTree {
  btree_branch_node *root;
  int (*cmp) (const void*,const void*);
  void (*free) (void*);
} BTree;

BTree *createBTree(List *rel, int (*cmp) (const void*, const void*), void *(*getKeyCopy) (const void*), void (freeKeyCopy)(void*));
void destroyBTree(BTree *t);
btree_leaf_node *btree_findLargerEqualLeaf(BTree *t, void *key);
void printBTree(BTree *t, void (*printKey)(void *));
void *btree_findLargest(BTree *t);

#endif /* BTREE_H_ */
