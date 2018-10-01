#ifndef STACK_H_
#define STACK_H_

#include "glob.h"

typedef struct stack_node {
  void *datum;
  struct stack_node * next;
} stack_node;

typedef struct Stack {
  stack_node * top;
  stack_node * tail;
} Stack ;

Stack *new_stack();
void stack_push(Stack * stack, void *datum);
void * stack_pop(Stack * stack);
void stack_free(Stack * stack);

#endif /* STACK_H_ */
