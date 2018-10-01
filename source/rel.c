#include "rel.h"
#include <stdlib.h>
#include <stdio.h>
#include "nodeHeapAm.h"
#include "File.h"
#include "Buffer.h"
#include <math.h>
#include <limits.h>
#include <time.h>

static double
gaussrand();

List *
makeRandRel(long n, long l, timestmp lower, timestmp upper)
{
  timestmp ts, te;
  char *data;
  List *ret = NIL;
  long i;


  if (lower < 1)
    {
      printf("ERROR: lower must be larger than 1\n");
      return ret;
    }

  for (i = 0; i < n; i++)
    {
      ts = random() % (upper - lower);
      ts += lower;

      te = (random() % MIN((upper-ts), l) ) + 1;
          if((random() % 1000)<-1)
            {
                te = MIN( (upper-lower)/10, upper);
            }
      te += ts;
      data = private_malloc(50);
      sprintf(data, "TUPLE %ld", i);
      ret = lappend(ret, makeTTup(ts, te, data));

    }
  return ret;
}

HeapAm *
makeRandDiskRel(const char *name, size_t tupsze, long n, long l,
    timestmp lower, timestmp upper, FILE_TYPE type)
{
  File *f = file_create(name, type);
  Buffer *buff = buffer_create(f, REL_MEM, POLICY_LRU);
  HeapAm *ret = ExecInitHeapAm(buff, tupsze);
  long i;
  srand (time (NULL));

  if (lower < 1)
    {
      printf("ERROR: lower must be larger than 1\n");
      return ret;
    }

  ttup *tup = private_malloc(sizeof(ttup));
  tup->data = private_calloc(1, tupsze - sizeof(ttup));
  for (i = 0; i < n; i++)
    {
      tup->ts = rand() % (upper - lower);
      tup->ts += lower;

      tup->te = (rand() % MIN((upper-tup->ts), l)) + 1;

      //Increase long tuples

      if((random() % 100)<-100)
      {
          tup->te = MIN( (upper-lower)/10, upper);
      }

      tup->te += tup->ts;
      ExecHeapAmInsert(ret, tup);
    }
  freeTTup(tup);
  return ret;
}

HeapAm *
makeSeqDESCDiskRel(const char *name, size_t tupsze, long n, long l,
    FILE_TYPE type)
{
  File *f = file_create(name, type);
  Buffer *buff = buffer_create(f, REL_MEM, POLICY_LRU);
  HeapAm *ret = ExecInitHeapAm(buff, tupsze);
  long i;

  ttup *tup = private_malloc(sizeof(ttup));
  tup->data = private_calloc(1, tupsze);
  timestmp cur_te = n * (l);
  for (i = 0; i < n; i++)
  {
    tup->ts = cur_te - l + 1;
    tup->te = cur_te;
    cur_te = tup->ts - 1;
    ExecHeapAmInsert(ret, tup);
  }
  freeTTup(tup);
  return ret;
}

List *
makeGaussRandRel(long n, long mean, long stddev, timestmp lower, timestmp upper)
{
  timestmp ts, te;
  List *ret = NIL;
  long i;

  if (lower < 1)
    {
      printf("ERROR: lower must be larger than 1\n");
      return ret;
    }

  for (i = 0; i < n; i++)
    {
      ts = random() % (upper - lower);
      ts += lower;

      te = ((timestmp)((gaussrand()*stddev)+mean)) % (upper-ts+1);
      te = MAX(0, te);
      te += ts;
      ret = lappend(ret, makeTTup(ts, te, NULL));
    }
  return ret;
}

HeapAm *
makeGaussRandDiskRel(const char *name, size_t tupsze, long n, long mean, long stddev, timestmp lower, timestmp upper, FILE_TYPE type)
{
  File *f = file_create(name, type);
  Buffer *buff = buffer_create(f, REL_MEM, POLICY_LRU);
  HeapAm *ret = ExecInitHeapAm(buff, tupsze);
  long i;

  if (lower < 1)
    {
      printf("ERROR: lower must be larger than 1\n");
      return ret;
    }

  ttup *tup = private_malloc(sizeof(ttup));
  tup->data = private_calloc(1, tupsze - sizeof(ttup));
  for (i = 0; i < n; i++)
    {
      tup->ts = random() % (upper - lower);
      tup->ts += lower;

      tup->te = ((timestmp)((gaussrand()*stddev)+mean)) % (upper-tup->ts+1);
      tup->te = MAX(0, tup->te);
      tup->te += tup->ts;
      ExecHeapAmInsert(ret, tup);
    }
  freeTTup(tup);
  return ret;
}

List *
makeRelFromFile(char *filename)
{
	int counter =0;
  timestmp ts, te;
  ttup *tup = private_malloc(sizeof(ttup));
  tup->data = private_calloc(1, sizeof(ttup)+50 - sizeof(ttup));
  char* data=(char*)malloc(sizeof(char)+50);
  List *ret = NIL;

  long skipped = 0;
  FILE *f = fopen(filename, "r");
  /*off = LONG_MAX;
  while (fscanf(f, "%ld\t%ld%*[^\n]", &ts, &te) == 2)
    off = MIN(off, ts);
  fseek(f, 0, SEEK_SET);*/

  while (fscanf(f, "%ld\t%ld\t%s\n", &ts, &te, (char*)data) == 3)
  {

    if(ts > te){
    	fprintf(stderr,"error %ld %ld \n",ts, te);
      skipped++;
  }
    else

      ret = lappend(ret, makeTTup(ts, te, data));
   data = (char*)malloc(sizeof(char)+50);
   counter++;

  }
  fclose(f);


  if(skipped > 0)
    fprintf(stderr, "WARNING: File %s: %ld records skipped due to error!\n", filename, skipped);
  ret->counter=counter;
  return ret;
}

HeapAm *
makeDiskRelFromFile(const char *name, size_t tupsze, const char *from_filename, FILE_TYPE type)
{
  timestmp ts, te;

	fflush(stdout);

	//char data[tupsze - sizeof(ttup)];
  File *f = file_create(name, type);
  Buffer *buff = buffer_create(f, REL_MEM, POLICY_LRU);
  HeapAm *ret = ExecInitHeapAm(buff, tupsze);
  ttup *tup = private_malloc(sizeof(ttup));
  long skipped = 0;
  tup->data = private_calloc(1, tupsze - sizeof(ttup));

  FILE *fin = fopen(from_filename, "r");
  /*off = LONG_MAX;
  while (fscanf(fin, "%ld\t%ld%*[^\n]", &ts, &te) == 2)
    off = MIN(off, ts);
  fseek(fin, 0, SEEK_SET);*/
  while (fscanf(fin, "%ld\t%ld\t%s%*[^\n]", &ts, &te, (char *)tup->data) == 3)
    {
      tup->ts = ts;
      tup->te = te;
      //tup->data= data;
      if(tup->ts > tup->te){
    	  fprintf(stderr,"errodfrtup: %ld %ld \n",tup->ts, tup->te);
        skipped++;
      }
      else
        ExecHeapAmInsert(ret, tup);
    }
  fclose(fin);
  freeTTup(tup);

  if(skipped > 0)
    fprintf(stderr, "WARNING: File %s: %ld records skipped due to error!\n", from_filename, skipped);

  return ret;
}

void
freeRel(List *rel)
{
  ListCell *relCell;
  foreach(relCell, rel)
    {
      freeTTup((ttup *) lfirst(relCell));
    }
  list_free(rel);
}

void
freeDiskRel(HeapAm *rel)
{
  Buffer *buffToClose = rel->f;
  File *fileToClose = buffToClose->f;
  ExecEndHeapAM(rel);
  buffer_close(buffToClose);
  file_close(fileToClose);
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
