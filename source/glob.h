
#ifndef GLOB_H_
#define GLOB_H_

#include <stdlib.h>
#include <stdio.h>

#define MIN( a ,b ) ( a > b ? b : a )
#define MAX( a ,b ) ( a < b ? b : a )

typedef long timestmp;

/*
 * Memory for disk algorithms
 * given in number of disk pages
 */
#define WORK_MEM 1000
#define REL_MEM 100

typedef char bool;
#define false ((char)0)
#define true ((char)1)

void NullFunction(void * junk);
void print_time(FILE *f);

extern size_t mem_size;
extern size_t cur_size;
extern size_t peak_size;
extern long mallocs;
extern long frees;

extern double cpu_cost;
extern double io_cost;


//#define DEBUG 1

#ifdef DEBUG
#define DEBLOG(x, ...) fprintf( stderr, "LOG:\t"); print_time(stderr); fprintf( stderr, "\t" ); fprintf( stderr, x, ##__VA_ARGS__ ); fprintf( stderr, "\n");
#else
#define DEBLOG(x, ...)
#endif



#ifdef DEBUG
void *private_malloc(size_t size);
void *private_calloc(size_t count, size_t size);
void private_free(void *ptr);
#else
#define private_malloc malloc
#define private_calloc calloc
#define private_free free
#endif

#endif /* GLOB_H_ */
