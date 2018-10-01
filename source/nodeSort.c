/*
 * nodeSort.c
 *
 *  Created on: May 8, 2013
 *      Author: dignoes
 */

#include "nodeSort.h"
#include "list.h"
#include "File.h"
#include "Buffer.h"
#include "nodeHeapAm.h"

#define NUM_RUN_FILES 10

static size_t makeSortedRuns(HeapAm *heapam, HeapAm **runs, int nrunfiles, size_t tupsze, size_t tup_per_run, int (*cmp) (const void*,const void*));
static size_t mergeSortedRuns(HeapAm **runsIn, HeapAm **runsOut, int nrunfiles, size_t tupsze, size_t tup_per_run, int (*cmp) (const void*,const void*));
static int get_min_tupidx(HeapAm **runs, int nruns, size_t *nruntups, int (*cmp) (const void*,const void*));

Sort *ExecInitSort(HeapAm *heapam, int (*cmp) (const void*,const void*), size_t tupsze, size_t mem_pages, FILE_TYPE type)
{
  Sort *ret = private_malloc(sizeof(Sort));

  ret->sort_done = 0;
  ret->heapam = heapam;
  ret->cmp = cmp;
  ret->tupsze = tupsze;
  ret->mem_pages = mem_pages;
  ret->res_heapam = NULL;
  ret->res_buf = NULL;
  ret->res_file = NULL;
  ret->filetype = type;

  ret->res_heapams = NULL;
  ret->res_bufs = NULL;
  ret->res_files = NULL;
  ret->res_runlens = NULL;
  ret->res_nruns = 0;
  ret->last_fetch = -1;

  DEBLOG("ExecInitSort")

  return ret;
}

/*
ttup *ExecSort(Sort *sort)
{
  if(!sort->sort_done)
  {
    int i = 0;
    File **files1 = private_malloc(NUM_RUN_FILES*sizeof(File *));
    Buffer **bufs1 = private_malloc(NUM_RUN_FILES*sizeof(Buffer *));
    HeapAm **heaps1 = private_malloc(NUM_RUN_FILES*sizeof(HeapAm *));
    File **files2 = private_malloc(NUM_RUN_FILES*sizeof(File *));
    Buffer **bufs2 = private_malloc(NUM_RUN_FILES*sizeof(Buffer *));
    HeapAm **heaps2 = private_malloc(NUM_RUN_FILES*sizeof(HeapAm *));
    for(i = 0; i < NUM_RUN_FILES; i++)
    {
      files1[i] = tmpfile_create(sort->filetype);
      bufs1[i] = buffer_create_void(files1[i]);
      heaps1[i] = ExecInitHeapAm(bufs1[i], sort->tupsze);
    }

    size_t tups_per_run = heaps1[0]->tups_per_page * sort->mem_pages;
    makeSortedRuns(sort->heapam, heaps1, NUM_RUN_FILES, sort->tupsze, tups_per_run, sort->cmp);

    while(buffer_fileNumPages(heaps1[1]->f) > 0)
    {
      DEBLOG("Sort iteration %d", i)
      for(i = 0; i < NUM_RUN_FILES; i++)
      {
        files2[i] = tmpfile_create(sort->filetype);
        bufs2[i] = buffer_create_void(files2[i]);
        heaps2[i] = ExecInitHeapAm(bufs2[i], sort->tupsze);
      }

      mergeSortedRuns(heaps1, heaps2, NUM_RUN_FILES, sort->tupsze, tups_per_run, sort->cmp);
      tups_per_run *= NUM_RUN_FILES;

      for(i = 0; i < NUM_RUN_FILES; i++)
      {
        ExecEndHeapAM(heaps1[i]);
        heaps1[i] = heaps2[i];
        buffer_close(bufs1[i]);
        bufs1[i] = bufs2[i];
        file_close(files1[i]);
        files1[i] = files2[i];
      }
    }

    for(i = 1; i < NUM_RUN_FILES; i++) // result is in 0
    {
      ExecEndHeapAM(heaps1[i]);
      buffer_close(bufs1[i]);
      file_close(files1[i]);
    }
    sort->sort_done = 1;

    sort->res_heapam = heaps1[0];
    sort->res_buf = bufs1[0];
    sort->res_file = files1[0];
    private_free(heaps1);
    private_free(bufs1);
    private_free(files1);
    private_free(heaps2);
    private_free(bufs2);
    private_free(files2);
    ExecHeapAmScanInit(sort->res_heapam);
  }

  return ExecHeapAmScan(sort->res_heapam);;
}*/

ttup *ExecSort(Sort *sort)
{
  if(!sort->sort_done)
  {
    int i = 0;
    File **files1 = private_malloc(NUM_RUN_FILES*sizeof(File *));
    Buffer **bufs1 = private_malloc(NUM_RUN_FILES*sizeof(Buffer *));
    HeapAm **heaps1 = private_malloc(NUM_RUN_FILES*sizeof(HeapAm *));
    File **files2 = private_malloc(NUM_RUN_FILES*sizeof(File *));
    Buffer **bufs2 = private_malloc(NUM_RUN_FILES*sizeof(Buffer *));
    HeapAm **heaps2 = private_malloc(NUM_RUN_FILES*sizeof(HeapAm *));
    for(i = 0; i < NUM_RUN_FILES; i++)
    {
      files1[i] = tmpfile_create(sort->filetype);
      bufs1[i] = buffer_create_void(files1[i]);
      heaps1[i] = ExecInitHeapAm(bufs1[i], sort->tupsze);
    }

    size_t tups_per_run = heaps1[0]->tups_per_page * sort->mem_pages;
    sort->res_nruns = makeSortedRuns(sort->heapam, heaps1, NUM_RUN_FILES, sort->tupsze, tups_per_run, sort->cmp);

    while(sort->res_nruns > NUM_RUN_FILES)
    {
      DEBLOG("Sort iteration %d", i)
      for(i = 0; i < NUM_RUN_FILES; i++)
      {
        files2[i] = tmpfile_create(sort->filetype);
        bufs2[i] = buffer_create_void(files2[i]);
        heaps2[i] = ExecInitHeapAm(bufs2[i], sort->tupsze);
      }

      sort->res_nruns = mergeSortedRuns(heaps1, heaps2, NUM_RUN_FILES, sort->tupsze, tups_per_run, sort->cmp);
      tups_per_run *= NUM_RUN_FILES;

      for(i = 0; i < NUM_RUN_FILES; i++)
      {
        ExecEndHeapAM(heaps1[i]);
        heaps1[i] = heaps2[i];
        buffer_close(bufs1[i]);
        bufs1[i] = bufs2[i];
        file_close(files1[i]);
        files1[i] = files2[i];
      }
    }

    /* remaining runs are pipelined */
    sort->res_files = private_malloc(sort->res_nruns*sizeof(File *));
    sort->res_bufs = private_malloc(sort->res_nruns*sizeof(Buffer *));
    sort->res_heapams = private_malloc(sort->res_nruns*sizeof(HeapAm *));
    sort->res_runlens = private_malloc(sort->res_nruns*sizeof(size_t));
    for(i = 0; i < ((int)sort->res_nruns); i++) /* remaining runs are in the first sort->res_runs files */
    {
      sort->res_files[i] = files1[i];
      sort->res_bufs[i] = bufs1[i];
      sort->res_heapams[i] = heaps1[i];
      sort->res_runlens[i] = tups_per_run;
      ExecHeapAmScanInit(sort->res_heapams[i]);
      ExecHeapAmScan(sort->res_heapams[i]);
    }
    for( ; i < NUM_RUN_FILES; i++)
    {
      ExecEndHeapAM(heaps1[i]);
      buffer_close(bufs1[i]);
      file_close(files1[i]);
    }
    private_free(heaps1);
    private_free(bufs1);
    private_free(files1);
    private_free(heaps2);
    private_free(bufs2);
    private_free(files2);

    sort->sort_done = 1;
    sort->last_fetch = -1;
  }

  if(sort->last_fetch > -1)
  {
    ExecHeapAmScan(sort->res_heapams[sort->last_fetch]);
    sort->res_runlens[sort->last_fetch]--;
  }
  sort->last_fetch = get_min_tupidx(sort->res_heapams, (int)sort->res_nruns, sort->res_runlens, sort->cmp);
  if(sort->last_fetch > -1)
    return &sort->res_heapams[sort->last_fetch]->tup;
  else
    return NULL;
}

/*
void ExecEndSort(Sort *sort)
{
  if(sort->res_heapam)
    ExecEndHeapAM(sort->res_heapam);
  if(sort->res_buf)
    buffer_close(sort->res_buf);
  if(sort->res_file)
    file_close(sort->res_file);
  private_free(sort);

  DEBLOG("ExecEndSort")
}*/

void ExecEndSort(Sort *sort)
{
  int i;
  for( i = 0; i < sort->res_nruns; i++)
  {
    ExecEndHeapAM(sort->res_heapams[i]);
    buffer_close(sort->res_bufs[i]);
    file_close(sort->res_files[i]);
  }
  if(sort->res_heapams)
    private_free(sort->res_heapams);
  if(sort->res_bufs)
    private_free(sort->res_bufs);
  if(sort->res_files)
    private_free(sort->res_files);
  if(sort->res_runlens)
    private_free(sort->res_runlens);

  private_free(sort);

  DEBLOG("ExecEndSort")
}


static int get_min_tupidx(HeapAm **runs, int nruns, size_t *nruntups, int (*cmp) (const void*,const void*))
{
  int idx = -1;
  int i;
  ttup *cur_min = NULL;
  for(i = 0; i < nruns; i++)
  {
    if(runs[i]->isNull || nruntups[i] <= 0)
      continue;
    if(cur_min == NULL)
    {
      cur_min = &runs[i]->tup;
      idx = i;
      continue;
    }
    if(cmp(cur_min, &runs[i]->tup) > 0)
    {
      cur_min = &runs[i]->tup;
      idx = i;
    }
  }
  return idx;
}

static size_t mergeSortedRuns(HeapAm **runsIn, HeapAm **runsOut, int nrunfiles, size_t tupsze, size_t tup_per_run, int (*cmp) (const void*,const void*))
{
  int toInsert = -1;
  int tofetch;
  size_t i;
  int still_tups = 1; /* are there still tuples to fetch */
  int still_runtups; /* are there still tuples to fetch in the current run */
  size_t nruntups[nrunfiles];
  size_t ret = 0;

  DEBLOG("mergeSortedRuns")

  for(i = 0; i < nrunfiles; i++)
  {
      ExecHeapAmScanInit(runsIn[i]);
      ExecHeapAmScan(runsIn[i]);
  }

  while(still_tups)
  {
    toInsert++;
    toInsert = toInsert % nrunfiles;
    for(i = 0; i < nrunfiles; i++)
      nruntups[i] = tup_per_run;

    still_runtups = 1;
    while(still_runtups)
    {
      tofetch = get_min_tupidx(runsIn, nrunfiles, nruntups, cmp);
      if(tofetch >= 0) /* still tuples in the current run */
      {
         ExecHeapAmInsert(runsOut[toInsert], &runsIn[tofetch]->tup);
         ExecHeapAmScan(runsIn[tofetch]);
         nruntups[tofetch]--;
      }
      else /* either no tuples in the current run or no tuples at all */
      {
        still_runtups = 0;
        still_tups = 0;
        /* check if there are some other tuples for a next run */
        for(i = 0; i < nrunfiles; i++)
        {
          if(!runsIn[i]->isNull)
            still_tups = 1;
        }
      }
    }
    ret++;
  }

  return ret;
}

/*
 * returns the number of runs produced
 */
static size_t makeSortedRuns(HeapAm *heapam, HeapAm **runs, int nrunfiles, size_t tupsze, size_t tup_per_run, int (*cmp) (const void*,const void*))
{
  List *run;
  ListCell *cell;
  size_t num_run_tups;
  ExecHeapAmScanInit(heapam);
  int toInsert = -1;
  size_t ret = 0;

  DEBLOG("makeSortedRuns")

  do {
    run = NIL;
    num_run_tups = 0;
    toInsert++;
    toInsert = toInsert % nrunfiles;

    while(num_run_tups < tup_per_run && ExecHeapAmScan(heapam))
    {
      ttup *tup = ttupdup(&heapam->tup, tupsze);
      run = lappend(run, tup);
      num_run_tups++;
    }
    run = list_sort(run, cmp);
    foreach(cell, run)
    {
      ExecHeapAmInsert(runs[toInsert], (ttup *)lfirst(cell));
      freeTTup(lfirst(cell));
    }
    list_free(run);
    DEBLOG("run produced")
    ret++;
  } while (!heapam->isNull);

  return ret;
}
