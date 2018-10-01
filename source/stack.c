#include "stack.h"

Stack * new_stack() {
  Stack *ret;

  ret=(Stack *) private_malloc(sizeof(Stack));
  ret->top=ret->tail=NULL;
  return(ret);
}


void stack_push(Stack *stack, void *datum) {
  stack_node * newNode;

  if(!stack->top) {
    newNode=(stack_node *) private_malloc(sizeof(stack_node));
    newNode->datum=datum;
    newNode->next=stack->top;
    stack->top=newNode;
    stack->tail=newNode;
  } else {
    newNode=(stack_node *) private_malloc(sizeof(stack_node));
    newNode->datum=datum;
    newNode->next=stack->top;
    stack->top=newNode;
  }

}

void *stack_pop(Stack *stack) {
  void *popInfo;
  stack_node * oldNode;

  if(stack->top) {
    popInfo=stack->top->datum;
    oldNode=stack->top;
    stack->top=stack->top->next;
    private_free(oldNode);
    if (!stack->top) stack->tail=NULL;
  } else {
    popInfo=NULL;
  }
  return(popInfo);
}

void stack_free(Stack *stack) {
  stack_node * x=stack->top;
  stack_node * y;

  if(stack) {
    while(x) {
      y=x->next;
      private_free(x);
      x=y;
    }
    private_free(stack);
  }
}

