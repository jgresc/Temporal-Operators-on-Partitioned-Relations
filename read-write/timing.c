#include "timing.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>


void *time_start()
{
  struct timeval *ret = malloc(sizeof(struct timeval));

  gettimeofday(ret, NULL);

  return ret;
}

void time_pprint(void *handle)
{
  long long elapsed;
  struct timeval t0 = *(struct timeval *)handle;
  struct timeval t1;
  gettimeofday(&t1, NULL);
  elapsed = (t1.tv_sec-t0.tv_sec)*1000000LL + t1.tv_usec-t0.tv_usec;

  long long sec = elapsed / 1000000LL;
  long long msec = (elapsed % 1000000LL) / 1000;

  printf("%lld.%03lld", sec, msec);
}

long long time_end(void *handle)
{
  long long elapsed;
  struct timeval t0 = *(struct timeval *)handle;
  struct timeval t1;
  gettimeofday(&t1, NULL);
  elapsed = (t1.tv_sec-t0.tv_sec)*1000000LL + t1.tv_usec-t0.tv_usec;
  free(handle);
  return elapsed;
}
