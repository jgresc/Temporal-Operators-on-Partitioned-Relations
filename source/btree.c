#include "btree.h"
#include <stdio.h>

/*
 * Btree implementation | <= key > |
 */


typedef struct key_tup_mock {
  void *key;
  ttup *tup;
  int (*cmp) (const void*, const void*);
} key_tup_mock;

static int cmp_key_tup_mock(const void *a, const void *b)
{
  key_tup_mock *t1 = (key_tup_mock *) a;
  key_tup_mock *t2 = (key_tup_mock *) b;
  return t1->cmp(t1->key, t2->key);
}

static btree_leaf_node *makeLeafNode()
{
  btree_leaf_node *ret = private_malloc(sizeof(btree_leaf_node));
  ret->n.type = LEAFNODE;
  ret->n.parent = NULL;
  ret->tups = NIL;
  ret->next = NULL;
  return ret;
}

static btree_branch_node *makeBranchNode()
{
  btree_branch_node *ret = private_malloc(sizeof(btree_branch_node));
  ret->n.type = BRANCHNODE;
  ret->n.parent = NULL;
  ret->left = NULL;
  ret->right = NULL;
  return ret;
}


BTree *createBTree(List *rel, int (*cmp) (const void*, const void*), void *(*getKeyCopy) (const void*), void (freeKeyCopy)(void*))
{
  ListCell *cell, *maxcell;
  btree_leaf_node *firstLeaf, *leaf_node;
  List *mockList;
  key_tup_mock *mock;
  List *levelList1, *levelList2;
  List *maxList1, *maxList2;
  btree_branch_node *branch_node;
  BTree *ret;

  /* prepare a mock list to sort */
  mockList = NIL;

  foreach(cell, rel)
  {
    mock = private_malloc(sizeof(key_tup_mock));
    mock->tup = (ttup *)lfirst(cell);
    mock->key = getKeyCopy(mock->tup);
    mock->cmp = cmp;
    mockList = lappend(mockList, mock);
  }

  mockList = list_sort(mockList, cmp_key_tup_mock);

  levelList1 = NIL;
  maxList1 = NIL;
  mock = (key_tup_mock *)lfirst(list_head(mockList));
  firstLeaf = makeLeafNode();
  firstLeaf->n.key = mock->key;
  firstLeaf->tups = lappend(firstLeaf->tups, mock->tup);
  private_free(mock);
  levelList1 = lappend(levelList1, firstLeaf);
  maxList1 = lappend(maxList1, firstLeaf->n.key);
  leaf_node = firstLeaf;

  for(cell = list_head(mockList)->next; cell != NULL; cell = cell->next)
  {
    mock = (key_tup_mock *)lfirst(cell);
    if(cmp(leaf_node->n.key, mock->key) == 0)
    {
      /* current has same key as previous */
      leaf_node->tups = lappend(leaf_node->tups, mock->tup);
      freeKeyCopy(mock->key);
    }
    else
    {
      /* make new leaf */
      leaf_node->next = makeLeafNode();
      leaf_node = leaf_node->next;
      leaf_node->n.key = mock->key;
      leaf_node->tups = lappend(leaf_node->tups, mock->tup);
      levelList1 = lappend(levelList1, leaf_node);
      maxList1 = lappend(maxList1, leaf_node->n.key);
    }
    private_free(mock);
  }
  list_free(mockList);

  int chidreen;
  do
  {
    levelList2 = NIL;
    maxList2 = NIL;
    chidreen = 2;
    maxcell = list_head(maxList1);
    foreach(cell, levelList1)
    {
      if(chidreen == 2)
      {
        branch_node = makeBranchNode();
        branch_node->left = (btree_node *)lfirst(cell);
        branch_node->left->parent = branch_node;
        branch_node->n.key = lfirst(maxcell);
        levelList2 = lappend(levelList2, branch_node);
        maxList2 = lappend(maxList2, lfirst(maxcell));
        chidreen = 1;
      }
      else
      {
        branch_node->right = (btree_node *)lfirst(cell);
        branch_node->right->parent = branch_node;
        lfirst(list_tail(maxList2)) = lfirst(maxcell);
        chidreen = 2;
      }
      maxcell = maxcell->next;
    }
    list_free(levelList1);
    list_free(maxList1);
    levelList1 = levelList2;
    maxList1 = maxList2;
  } while(list_length(levelList1) > 1);

  ret = private_malloc(sizeof(BTree));
  ret->root = (btree_branch_node *)lfirst(list_head(levelList1));
  list_free(levelList1);
  list_free(maxList1);
  ret->cmp = cmp;
  ret->free = freeKeyCopy;

  return ret;
}

static void destroyBTreeRec(BTree *t, btree_node *node)
{
  if(node == NULL)
    return;
  if(node->type == LEAFNODE)
  {
    t->free(node->key);
    list_free(((btree_leaf_node*)node)->tups);
    private_free(node);
    return;
  }
  destroyBTreeRec(t, ((btree_branch_node *)node)->left);
  destroyBTreeRec(t, ((btree_branch_node *)node)->right);
  private_free(node);

}

void destroyBTree(BTree *t)
{
  destroyBTreeRec(t, (btree_node *)t->root);
  private_free(t);
}

static btree_leaf_node *btree_findLargerEqualLeafRec(BTree *t, void *key, btree_node *node)
{
  btree_node *child;

  if(node == NULL)
    return NULL;

  if(node->type == LEAFNODE)
  {
    if(t->cmp(node->key, key) >= 0)
      return (btree_leaf_node *)node;
    else
      return NULL;
  }

  /* node is branch_node */
  if(t->cmp(key, node->key) <= 0)
    child = ((btree_branch_node *)node)->left;
  else
    child = ((btree_branch_node *)node)->right;


  return btree_findLargerEqualLeafRec(t, key, child);
}

static void *btree_findLargestRec(BTree *t, btree_node *node)
{
  btree_node *rec;
  if(node == NULL)
    return NULL;
  if(node->type == LEAFNODE)
    return node->key;

  rec = ((btree_branch_node *)node)->right;
  if(!rec)
    rec = ((btree_branch_node *)node)->left;

  return btree_findLargestRec(t, rec);
}

void *btree_findLargest(BTree *t)
{
  return btree_findLargestRec(t, (btree_node *)t->root);
}

btree_leaf_node *btree_findLargerEqualLeaf(BTree *t, void *key)
{
  return btree_findLargerEqualLeafRec(t, key, (btree_node *)t->root);
}

static void printTab(int num)
{
  int i;
  for(i = 0; i < num; i++)
  {
    printf("  ");
  }
}

static void printBTreeNode(btree_node *node, void (*printKey)(void *), int level)
{

  if(node == NULL)
  {
    printTab(level);
    printf("{}\n");
    return;
  }

  if(node->type == LEAFNODE)
  {
    printTab(level);
    printf("{LEAFNODE\n");
    printTab(level);
    printf(":key ");
    printKey(node->key);
    printf("\n");
    printTab(level);
    printf("}\n");
    return;
  }

  printTab(level);
  printf("{BRANCHNODE\n");
  printTab(level);
  printf(":key ");
  printKey(node->key);
  printf("\n");
  printTab(level);
  printf(":left\n");
  printBTreeNode(((btree_branch_node*)node)->left, printKey, level+1);
  printTab(level);
  printf(":right\n");
  printBTreeNode(((btree_branch_node*)node)->right, printKey, level+1);
  printTab(level);
  printf("}\n");
}

void printBTree(BTree *t, void (*printKey)(void *))
{
  printBTreeNode((btree_node *)t->root, printKey, 0);
  printf("\n");
}
