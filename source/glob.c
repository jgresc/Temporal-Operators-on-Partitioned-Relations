#include "glob.h"
#include <stdio.h>
#include "time.h"

size_t mem_size = 0;
size_t cur_size = 0;
size_t peak_size = 0;
long mallocs = 0;
long frees = 0;

double cpu_cost = 0.0025;
double io_cost = 1.0;

#ifdef DEBUG
void *private_malloc(size_t size)
{
  void *ret;
  mem_size+=size;
  cur_size+=size;
  peak_size = MAX(peak_size, cur_size);
  mallocs++;
  ret = malloc(size+sizeof(size_t));
  if(ret == NULL)
  {
    fprintf(stderr, "malloc problem!\n");
    fflush(stderr);
    exit(1);
  }
  *((size_t *)ret) = size;
  return ret+sizeof(size_t);
}

void *private_calloc(size_t count, size_t size)
{
  void *ret;
  mem_size+=size;
  cur_size+=size;
  peak_size = MAX(peak_size, cur_size);
  mallocs++;
  ret = calloc(count, size+sizeof(size_t));
  if(ret == NULL)
  {
    fprintf(stderr, "calloc problem!\n");
    fflush(stderr);
    exit(1);
  }
  *((size_t *)ret) = size;
  return ret+sizeof(size_t);
}

void private_free(void *ptr)
{
  void *toFree = ptr-sizeof(size_t);
  frees++,
  cur_size -= *((size_t *)toFree);
  free(toFree);
}
#endif

void print_time(FILE *f) {
  char buff[100];
  time_t now = time (0);
  strftime (buff, 100, "%Y-%m-%d %H:%M:%S.000", localtime (&now));
  fprintf (f, "%s", buff);
}

void NullFunction(void * junk) { ; }
