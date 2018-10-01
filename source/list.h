#ifndef LIST_H
#define LIST_H

#include "glob.h"
#include <stdlib.h>

typedef struct ListCell ListCell;

typedef struct List
{
  int     length;
  ListCell   *head;
  ListCell   *tail;
  int     counter;
  ListCell *next;
} List;

struct ListCell
{

  void     *ptr_value;
  ListCell   *next;
};

/*
 * The *only* valid representation of an empty list is NIL; in other
 * words, a non-NIL list is guaranteed to have length >= 1 and
 * head/tail != NULL
 */
#define NIL           ((List *) NULL)

/*
 * These routines are used frequently. However, we can't implement
 * them as macros, since we want to avoid double-evaluation of macro
 * arguments. Therefore, we implement them using static inline functions
 * if supported by the compiler, or as regular functions otherwise.
 */
#ifdef USE_INLINE

static inline ListCell *
list_head(List *l)
{
  return l ? l->head : NULL;
}

static inline ListCell *
list_tail(List *l)
{
  return l ? l->tail : NULL;
}

static inline int
list_length(List *l)
{
  return l ? l->length : 0;
}
#else

extern ListCell *list_head(List *l);
extern ListCell *list_tail(List *l);
extern int  list_length(List *l);
#endif   /* USE_INLINE */

/*
 * NB: There is an unfortunate legacy from a previous incarnation of
 * the List API: the macro lfirst() was used to mean "the data in this
 * cons cell". To avoid changing every usage of lfirst(), that meaning
 * has been kept. As a result, lfirst() takes a ListCell and returns
 * the data it contains; to get the data in the first cell of a
 * List, use linitial(). Worse, lsecond() is more closely related to
 * linitial() than lfirst(): given a List, lsecond() returns the data
 * in the second cons cell.
 */

#define lnext(lc)       ((lc)->next)
#define lfirst(lc)        ((lc)->ptr_value)

#define linitial(l)       lfirst(list_head(l))

#define llast(l)        lfirst(list_tail(l))

/*
 * Convenience macros for building fixed-length lists
 */
#define list_make1(x1)        lcons(x1, NIL)
#define list_make2(x1,x2)     lcons(x1, list_make1(x2))
#define list_make3(x1,x2,x3)    lcons(x1, list_make2(x2, x3))
#define list_make4(x1,x2,x3,x4)   lcons(x1, list_make3(x2, x3, x4))

/*
 * foreach -
 *    a convenience macro which loops through the list
 */
#define foreach(cell, l)  \
  for ((cell) = list_head(l); (cell) != NULL; (cell) = lnext(cell))

/*
 * for_each_cell -
 *    a convenience macro which loops through a list starting from a
 *    specified cell
 */
#define for_each_cell(cell, initcell) \
  for ((cell) = (initcell); (cell) != NULL; (cell) = lnext(cell))

extern List *lappend(List *list, void *datum);

extern ListCell *lappend_cell(List *list, ListCell *prev, void *datum);

extern List *lcons(void *datum, List *list);

extern void list_free(List *list);
extern void list_free_deep(List *list);

extern List *list_quick_sort_stack(List *list, int (*cmp) (const void*,const void*));
extern List *list_quick_sort_recursive(List *list, int (*cmp) (const void*,const void*));
extern List *list_merge_sort(List *list, int (*cmp) (const void*,const void*));
extern List *list_sort(List *list, int (*cmp) (const void*,const void*));
extern void
new_head_cell(List *list);

#endif   /* LIST_H */
