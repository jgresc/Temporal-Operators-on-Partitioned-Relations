#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../glob.h"
#include <math.h>

static void usage(char *execname);
static double gaussrand();
void dsequential(int argc, char *argv[]);
void drand(int argc, char *argv[]);
void dsample_from_file(int argc, char *argv[]);
void ddcmp(int argc, char *argv[]);
void dgauss(int argc, char *argv[]);

int main(int argc, char *argv[])
{

  if(argc < 2)
  {
    usage(argv[0]);
    exit(-1);
  }

  if(strcasecmp(argv[1], "seq") == 0)
  {
    dsequential(argc, argv);
  }
  else if(strcasecmp(argv[1], "rand") == 0)
  {
    drand(argc, argv);
  }
  else if(strcasecmp(argv[1], "samp") == 0)
  {
    dsample_from_file(argc, argv);
  }
  else if(strcasecmp(argv[1], "dcmp") == 0)
  {
    ddcmp(argc, argv);
  }
  else if(strcasecmp(argv[1], "gauss") == 0)
  {
    dgauss(argc, argv);
  }
  else
    usage(argv[0]);

  return 0;
}

static void usage(char *execname)
{
  printf("Usage: %s <opt> <params ...>\n", execname);
  printf("%s seq <n> <l>\n", execname);
  printf("%s rand <n> <l> <lower> <upper>\n", execname);
  printf("%s samp <fname> <n>\n", execname);
  printf("%s dcmp <lower> <upper>\n", execname);
  printf("%s gauss <n> <mean> <stddev> <lower> <upper>\n", execname);
}

void dsequential(int argc, char *argv[])
{
  long n;
  long l;
  if(argc < 4)
  {
    usage(argv[0]);
    exit(-1);
  }

  n = atol(argv[2]);
  l = atol(argv[3]);

  long i;
  for(i = 0; i < n; i++)
  {
    printf("%ld \t %ld\n", i*l+1, i*l+l);
  }
}

void drand(int argc, char *argv[])
{
  long n;
  long l;
  long us;
  long ue;
  if(argc < 6)
  {
    usage(argv[0]);
    exit(-1);
  }

  n = atol(argv[2]);
  l = atol(argv[3]);
  us = atol(argv[4]);
  ue = atol(argv[5]);

  long ts, te;
  long i;
  for(i = 0; i < n; i++)
  {
    ts = random() % (ue - us);
    ts += us;

    te = random() % MIN((ue-ts+1), l);
    te += ts;
    printf("%ld \t %ld\n", ts, te);
  }
}

void ddcmp(int argc, char *argv[])
{
  long us;
  long ue;
  if(argc < 4)
  {
    usage(argv[0]);
    exit(-1);
  }

  us = atol(argv[2]);
  ue = atol(argv[3]);

  long i, j;
  for(j = us; j <= ue; j++)
    for(i = us; i <= j; i++)
      printf("%ld \t %ld\n", i, j);
}

void dsample_from_file(int argc, char *argv[])
{
  char* fname;
  long n;
  long ts, te;
  if(argc < 4)
  {
    usage(argv[0]);
    exit(-1);
  }

  fname = argv[2];
  n = atol(argv[3]);

  FILE *f = fopen(fname, "r");
  long num_rows = 0;
  while(fscanf(f, "%ld\t%ld%*[^\n]", &ts, &te) == 2)
    num_rows++;

  long pick_each = num_rows/n;

  fseek(f, 0, SEEK_SET);
  long i = 1;
  while(fscanf(f, "%ld\t%ld%*[^\n]", &ts, &te) == 2)
  {
    if(i % pick_each == 0)
      printf("%ld\t%ld\n", ts, te);
    i++;
  }
  fclose(f);
}

void dgauss(int argc, char *argv[])
{
  long n;
  long us;
  long ue;
  long stddev;
  long mean;
  if(argc < 7)
  {
    usage(argv[0]);
    exit(-1);
  }

  n = atol(argv[2]);
  mean = atol(argv[3]);
  stddev = atol(argv[4]);
  us = atol(argv[5]);
  ue = atol(argv[6]);


  long ts, te;
  long i;
  for(i = 0; i < n; i++)
  {
    ts = random() % (ue - us);
    ts += us;

    te = ((long)((gaussrand()*stddev)+mean)) % (ue-ts+1);
    te = MAX(0, te);
    te += ts;
    te = MIN(te, ue);
    printf("%ld \t %ld\n", ts, te);
  }
}

/*
 * Gaussian random number generator
 * mean 0 and standard deviation 1
 * Taken from: http://cboard.cprogramming.com/cplusplus-programming/81955-fast-normal-random-number-generator.html
 */
static double
gaussrand()
{
  static double V1, V2, S;
  static int phase = 0;
  double X;

  if (phase == 0)
    {
      do
        {
          double U1 = (double) rand() / RAND_MAX;
          double U2 = (double) rand() / RAND_MAX;

          V1 = 2 * U1 - 1;
          V2 = 2 * U2 - 1;
          S = V1 * V1 + V2 * V2;
        }
      while (S >= 1 || S == 0);

      X = V1 * sqrt(-2 * log(S) / S);
    }
  else
    X = V2 * sqrt(-2 * log(S) / S);

  phase = 1 - phase;

  return X;
}
