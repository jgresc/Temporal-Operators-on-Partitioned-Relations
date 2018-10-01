/*
 * nodeSort.h
 *
 *  Created on: May 8, 2013
 *      Author: dignoes
 */

#ifndef NODESORT_H_
#define NODESORT_H_

#include "glob.h"
#include "nodeHeapAm.h"

typedef struct Sort {
  HeapAm *heapam;
  int (*cmp) (const void*,const void*);
  size_t tupsze;
  size_t mem_pages;

  /*
   * result
   */
  HeapAm *res_heapam;
  Buffer *res_buf;
  File *res_file;

  size_t res_nruns;
  size_t *res_runlens;
  HeapAm **res_heapams;
  Buffer **res_bufs;
  File **res_files;
  int last_fetch;

  FILE_TYPE filetype;

  int sort_done;
} Sort;

Sort *ExecInitSort(HeapAm *heapam, int (*cmp) (const void*,const void*), size_t tupsze, size_t mem_pages, FILE_TYPE type);

ttup *ExecSort(Sort *sort);

void ExecEndSort(Sort *sort);

#endif /* NODESORT_H_ */
