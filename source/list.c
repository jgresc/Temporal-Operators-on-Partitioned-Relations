#include "list.h"

#include <stdio.h>
#include "stack.h"

#ifndef USE_INLINE

#define BUBBLE_LIST_LEN 1 /* for lists of this size bubble sort instead of recursice quicksort will be used */

ListCell *
list_head(List *l)
{
  return l ? l->head : NULL;
}

ListCell *
list_tail(List *l)
{
  return l ? l->tail : NULL;
}

int
list_length(List *l)
{
  return l ? l->length : 0;
}
#endif   /* ! USE_INLINE */

/*
 * Return a freshly allocated List. Since empty non-NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static List *
new_list()
{
  List     *new_list;
  ListCell   *new_head;

  new_head = (ListCell *) private_malloc(sizeof(*new_head));
  new_head->next = NULL;
  /* new_head->data is left undefined! */

  new_list = (List *) private_malloc(sizeof(*new_list));
  new_list->length = 1;
  new_list->head = new_head;
  new_list->tail = new_head;

  return new_list;
}

/*
 * Allocate a new cell and make it the tail of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
  ListCell   *new_tail;

  new_tail = (ListCell *) private_malloc(sizeof(*new_tail));
  new_tail->next = NULL;

  list->tail->next = new_tail;
  list->tail = new_tail;
  list->length++;
}

/*
 * Allocate a new cell and make it the head of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
 void
new_head_cell(List *list)
{
  ListCell   *new_head;

  new_head = (ListCell *) private_malloc(sizeof(*new_head));
  new_head->next = list->head;

  list->head = new_head;
  list->length++;
}

/*
 * Add a new cell to the list, in the position after 'prev_cell'. The
 * data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'prev_cell' is assumed
 * to be non-NULL and a member of 'list'.
 */
static ListCell *
add_new_cell(List *list, ListCell *prev_cell)
{
  ListCell   *new_cell;

  new_cell = (ListCell *) private_malloc(sizeof(*new_cell));
  /* new_cell->data is left undefined! */
  new_cell->next = prev_cell->next;
  prev_cell->next = new_cell;

  if (list->tail == prev_cell)
    list->tail = new_cell;

  list->length++;

  return new_cell;
}

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 */
List *
lcons(void *datum, List *list)
{
  if (list == NIL)
    list = new_list();
  else
    new_head_cell(list);

  lfirst(list->head) = datum;
  return list;
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
static void
list_free_private(List *list, bool deep)
{
  ListCell   *cell;

  cell = list_head(list);
  while (cell != NULL)
  {
    ListCell   *tmp = cell;

    cell = lnext(cell);
    if (deep)
      private_free(lfirst(tmp));
    private_free(tmp);
  }

  if (list)
    private_free(list);
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
  if (list == NIL)
    list = new_list();
  else
    new_tail_cell(list);

  lfirst(list->tail) = datum;
  return list;
}

/*
 * Add a new cell to the specified list (which must be non-NIL);
 * it will be placed after the list cell 'prev' (which must be
 * non-NULL and a member of 'list'). The data placed in the new cell
 * is 'datum'. The newly-constructed cell is returned.
 */
ListCell *
lappend_cell(List *list, ListCell *prev, void *datum)
{
  ListCell   *new_cell;

  new_cell = add_new_cell(list, prev);
  lfirst(new_cell) = datum;
  return new_cell;
}

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free(List *list)
{
  list_free_private(list, false);
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a private_malloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free_deep(List *list)
{
  list_free_private(list, true);
}

/*
 * Concatenate list2 to the end of list1, and return list1. list1 is
 * destructively changed. Callers should be sure to use the return
 * value as the new pointer to the concatenated list: the 'list1'
 * input pointer may or may not be the same as the returned pointer.
 *
 * The nodes in list2 are merely appended to the end of list1 in-place
 * (i.e. they aren't copied; the two lists will share some of the same
 * storage). Therefore, invoking list_free() on list2 will also
 * invalidate a portion of list1.
 */
List *
list_concat(List *list1, List *list2)
{
  if (list1 == NIL)
    return list2;
  if (list2 == NIL)
    return list1;
  if (list1 == list2)
    printf("ERROR: cannot list_concat() a list to itself");

  list1->length += list2->length;
  list1->tail->next = list2->head;
  list1->tail = list2->tail;
  private_free(list2);

  return list1;
}

void
list_bubble_sort(ListCell *from, ListCell *to, int len, int (*cmp) (const void*,const void*))
{
  int i, j;
  ListCell *cell;
  void *tmp;
  bool swapPerformed = true;
  for(i = len - 1; swapPerformed && i > 0 ; i--) {
    j = 0;
    swapPerformed = false;
    for(cell = from; cell != to; cell = cell->next)
    {
      if(j++ >= i)
        break;
      if(cmp(lfirst(cell), lfirst(cell->next)) > 0)
      {
        /* swap content */
        swapPerformed = true;
        tmp = lfirst(cell);
        lfirst(cell) = lfirst(cell->next);
        lfirst(cell->next) = tmp;
      }
    }
  }
}

static void
list_quick_sort_recursive_call(ListCell *from, ListCell *to, int len, int (*cmp) (const void*,const void*));

List *
list_quick_sort_recursive(List *list, int (*cmp) (const void*,const void*))
{
  list_quick_sort_recursive_call(list_head(list), list_tail(list)->next, list_length(list), cmp);
  return list;
}

struct qsort_stack_node {
  ListCell *from;
  ListCell *to;
  int len;
};

List *
list_quick_sort_stack(List *list, int (*cmp) (const void*,const void*))
{
  ListCell *p1;
  ListCell *p2;
  int llen; /* number of keys to the left of p1 */
  void *tmp;
  void *key;
  Stack *stack = new_stack();
  struct qsort_stack_node *new_node = private_malloc(sizeof(struct qsort_stack_node));
  new_node->from = list_head(list);
  new_node->to = list_tail(list)->next;
  new_node->len = list_length(list);
  stack_push(stack, new_node);

  struct qsort_stack_node *n;

  while((n = (struct qsort_stack_node *)stack_pop(stack)) != NULL)
  {
    if(n->len < 2)
    {
      private_free(n);
      continue;
    }

    /*
     * keys to the left of p1 are < key and keys to its right are >= key p1 always points to the key
     */
    p1 = n->from;
    key = lfirst(p1);
    llen = 0;
    while(p1->next != n->to && cmp(lfirst(p1->next), key) < 0)
    {
      tmp = lfirst(p1);
      lfirst(p1) = lfirst(p1->next);
      lfirst(p1->next) = tmp;
      p1=p1->next;
      llen++;
    }

    for(p2 = p1->next; p2 != n->to; p2=p2->next)
    {
      if(cmp(lfirst(p2), key) < 0)
      {
        /* switch p2 and p1->next */
        tmp = lfirst(p1->next);
        lfirst(p1->next) = lfirst(p2);
        lfirst(p2) = tmp;
        /* switch p1 and p1->next and advance */
        tmp = lfirst(p1->next);
        lfirst(p1->next) = lfirst(p1);
        lfirst(p1) = tmp;
        p1 = p1->next;
        llen++;
      }
    }

    /* decide whether to put p1 left or right */
    if(llen <= n->len-llen)
    {
      /* left */
      p1 = p1->next;
      llen++;
    }

    new_node = private_malloc(sizeof(struct qsort_stack_node));
    new_node->from = n->from;
    new_node->to = p1;
    new_node->len = llen;
    stack_push(stack, new_node);
    new_node = private_malloc(sizeof(struct qsort_stack_node));
    new_node->from = p1;
    new_node->to = n->to;
    new_node->len = n->len-llen;
    stack_push(stack, new_node);
    private_free(n);
  }
  stack_free(stack);
  return list;
}

List *
list_merge_sort(List *list, int (*cmp) (const void*,const void*))
{
  if(list_length(list) <= 1)
    return list;

  /* split lists into 2 */
  int i = 0;
  int mid = list_length(list) / 2;
  ListCell *c;
  foreach(c, list)
  {
    if(++i >= mid)
      break;
  }

  List *new_list = private_malloc(sizeof(List));
  new_list->head = c->next;
  new_list->tail = list->tail;
  new_list->length = list->length - i;
  c->next = NULL;

  list->tail = c;
  list->length = list->length - new_list->length;

  list = list_merge_sort(list, cmp);
  new_list = list_merge_sort(new_list, cmp);

  /* merge lists into list */
  ListCell *c2 = new_list->head;
  ListCell *tail = list->head;
  if(cmp(lfirst(tail), lfirst(c2)) > 0)
  {
    list->head = c2;
    c2 = c2->next;
    list->head->next = tail;
    tail = list->head;
  }

  while(tail->next != NULL && c2 != NULL)
  {
    if(cmp(lfirst(tail->next), lfirst(c2)) > 0)
    {
      /* put c2 after tail */
      c = tail->next;
      tail->next = c2;
      c2 = c2->next;
      tail->next->next = c;
    }
    tail = tail->next;
  }

  if(c2 != NULL)
  {
    /* append the rest to list */
    tail->next = c2;
    list->tail = new_list->tail;
  }
  list->length += new_list->length;

  private_free(new_list);

  return list;
}

List *
list_sort(List *list, int (*cmp) (const void*,const void*))
{
  return list_merge_sort(list, cmp);
}

void
list_quick_sort_recursive_call(ListCell *from, ListCell *to, int len, int (*cmp) (const void*,const void*))
{

  if(len < 2)
    return;

  if(len <= BUBBLE_LIST_LEN)
  {
    list_bubble_sort(from, to, len, cmp);
    return;
  }

  /*
   * keys to the left of p1 are < key and keys to its right are >= key p1 always points to the key
   */
  ListCell *p1;
  ListCell *p2;
  int llen; /* number of keys to the left of p1 */
  void *tmp;
  void *key;
  p1 = from;
  key = lfirst(p1);
  llen = 0;
  while(p1->next != to && cmp(lfirst(p1->next), key) < 0)
  {
    tmp = lfirst(p1);
    lfirst(p1) = lfirst(p1->next);
    lfirst(p1->next) = tmp;
    p1=p1->next;
    llen++;
  }

  for(p2 = p1->next; p2 != to; p2=p2->next)
  {
    if(cmp(lfirst(p2), key) < 0)
    {
      /* switch p2 and p1->next */
      tmp = lfirst(p1->next);
      lfirst(p1->next) = lfirst(p2);
      lfirst(p2) = tmp;
      /* switch p1 and p1->next and advance */
      tmp = lfirst(p1->next);
      lfirst(p1->next) = lfirst(p1);
      lfirst(p1) = tmp;
      p1 = p1->next;
      llen++;
    }
  }

  /* decide whether to put p1 left or right */
  if(llen <= len-llen)
  {
    /* left */
    p1 = p1->next;
    llen++;
  }

  list_quick_sort_recursive_call(from, p1, llen, cmp);
  list_quick_sort_recursive_call(p1, to, len-llen, cmp);
}
