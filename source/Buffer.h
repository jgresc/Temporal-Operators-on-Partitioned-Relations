/*
 * Buffer.h
 *
 *  Created on: Aug 4, 2012
 *      Author: dignoes
 */

#ifndef BUFFER_H_
#define BUFFER_H_

#include "stdio.h"
#include "stdlib.h"
#include "File.h"
#include "red_black_tree.h"
#include "stack.h"

typedef enum BUF_REPACE_POLICY {
  POLICY_LRU,
  POLICY_MRU,
} BUF_REPACE_POLICY;

typedef struct BufferPage {
  size_t page_num;
  int isDirty;
  int isPinned;

  struct BufferPage *previous;
  struct BufferPage *next;
  /*
   * follows the page
   */
} BufferPage;

typedef struct Buffer {
  bool isVoid; /* if set no buffering */
  size_t pagesze;
  File *f;

  BUF_REPACE_POLICY rep_policy;
  void *buf;
  size_t num_buf_pages;

  void *pinned_list;
  Stack *free_list;
  void *page_list;

  /*
   * indexes
   */
  rb_red_blk_tree *page_idx;



  size_t num_pinned; /* number of blocks pinned */

  /*
   * statistics
   */
  size_t num_read;
  size_t num_write;

  /*
   * number of virtual pages of the file
   * Some pages might not yet be written to disk
   */
  size_t num_virtual_pages;

  /*
   * last returned page
   */
  BufferPage *page;

} Buffer;

Buffer *buffer_create(File *f, size_t num_pages, BUF_REPACE_POLICY rep_policy);
Buffer *buffer_create_void(File *f);
void buffer_close(Buffer *f);
void buffer_clear(Buffer *f);

void buffer_readPage(void *dst, size_t page_num, Buffer *f);
void buffer_writePage(const void *src, size_t page_num, Buffer *f);
void buffer_flush(Buffer *f);
size_t buffer_fileNumPages(Buffer *f);
void buffer_clearIOStat(Buffer *f);
size_t buffer_numReads(Buffer *f);
size_t buffer_numWrites(Buffer *f);

void buffer_pinPage(Buffer *f, size_t page_num);

#endif /* BUFFER_H_ */
