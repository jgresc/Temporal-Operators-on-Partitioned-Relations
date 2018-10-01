/*
 * BufFile.c
 *
 *  Created on: Aug 4, 2012
 *      Author: dignoes
 */

#include "Buffer.h"
#include <limits.h>
#include <search.h>
#include <string.h>

static int buffer_cmp_size_t(const void *a, const void *b);
static BufferPage *swapIn(Buffer *f, size_t page_num);
static void swapOut(Buffer *f, BufferPage *bufBlock);

Buffer *buffer_create(File *f, size_t num_pages, BUF_REPACE_POLICY rep_policy)
{
  Buffer *ret = private_malloc(sizeof(Buffer));
  int i;

  ret->pagesze = f->pgesze;
  ret->isVoid = false;

  ret->f = f;

  ret->rep_policy = rep_policy;
  ret->num_buf_pages = num_pages;
  ret->buf = private_calloc(1, ret->num_buf_pages*(sizeof(BufferPage)+ret->pagesze));
  ret->num_pinned = 0;
  ret->page = NULL;

  /*
   * create indexes
   */
  ret->free_list = new_stack();
  ret->page_idx = RBTreeCreate(buffer_cmp_size_t, NullFunction, NullFunction, NullFunction, NullFunction);
  ret->page_list = NULL;

  for(i = 0; i < num_pages; i++)
    stack_push(ret->free_list, ret->buf+i*(sizeof(BufferPage)+ret->pagesze));

  ret->num_virtual_pages = file_numPages(ret->f);

  buffer_clearIOStat(ret);

  return ret;
}

Buffer *buffer_create_void(File *f)
{
  Buffer *ret = private_malloc(sizeof(Buffer));

  ret->pagesze = f->pgesze;
  ret->isVoid = true;
  ret->num_buf_pages = 0;
  ret->num_pinned = 0;

  ret->f = f;

  buffer_clearIOStat(ret);

  return ret;
}

void buffer_flush(Buffer *f)
{
  size_t i;
  BufferPage *b;
  for(i = 0; i < f->num_buf_pages; i++)
  {
    b = (BufferPage *)(f->buf+i*(sizeof(BufferPage)+f->pagesze));
    if(b->page_num != 0)
    {
      swapOut(f, b);
    }
  }
  file_flush(f->f);
}

void buffer_clear(Buffer *f)
{
  size_t i;
  BufferPage *b;
  rb_red_blk_node *idx_node;

  buffer_flush(f);
  for(i = 0; i < f->num_buf_pages; i++)
  {
    b = (BufferPage *)(f->buf+i*(sizeof(BufferPage)+f->pagesze));
    if(!b->isPinned && b->page_num != 0)
    {
      idx_node = RBExactQuery(f->page_idx, &b->page_num);
      RBDelete(f->page_idx, idx_node);
      b->page_num = 0;
      stack_push(f->free_list, b);
    }
  }
}

void buffer_close(Buffer *f)
{
  buffer_flush(f);

  if(!f->isVoid)
  {
    stack_free(f->free_list);
    RBTreeDestroy(f->page_idx);
    private_free(f->buf);
  }
  private_free(f);
}

void buffer_readPage(void *dst, size_t page_num, Buffer *f)
{
  if(f->isVoid)
  {
    file_readPage(dst, page_num, f->f);
    f->num_read++;
    return;
  }
  BufferPage *ret = swapIn(f, page_num);
  memcpy(dst, ((void*)ret)+sizeof(BufferPage), f->pagesze);
}

void buffer_pinPage(Buffer *f, size_t page_num)
{

  if(f->num_pinned + 1 >= f->num_buf_pages)
  {
    fprintf(stderr, "WARNING: Can not pin more than %ld pages, skip pin! (%s:%d)\n", f->num_pinned, __FILE__, __LINE__);
    return;
  }

  BufferPage *blk = swapIn(f, page_num);
  if(blk->isPinned)
    return;
  blk->isPinned = 1;
  f->num_pinned++;
  /* remove from page_list */
  if(blk->next == blk)
  {
    f->page_list = NULL;
  }
  else
  {
    blk->previous->next = blk->next;
    blk->next->previous = blk->previous;
  }
}

void buffer_writePage(const void *src, size_t page_num, Buffer *f)
{
  if(f->isVoid)
  {
    file_writePage(src, page_num, f->f);
    f->num_write++;
    return;
  }
  BufferPage *b = swapIn(f, page_num);
  memcpy(((void*)b)+sizeof(BufferPage), src, f->pagesze);
  b->isDirty = 1;
}


size_t buffer_fileNumPages(Buffer *f)
{
  if(f->isVoid)
    return file_numPages(f->f);
  return f->num_virtual_pages;
}

void buffer_clearIOStat(Buffer *f)
{
  f->num_read = 0;
  f->num_write = 0;
}

static void swapOut(Buffer *f, BufferPage *bufBlock)
{
  if(bufBlock->isDirty)
  {
    file_writePage(((void*)bufBlock)+sizeof(BufferPage), bufBlock->page_num, f->f);
    f->num_write++;
    bufBlock->isDirty = 0;
  }
}


/*
 * Returns the pointer to the BufPage indicated by page_num.
 * The page is read from underlying file if not in the buffer.
 * Swaps pages using BufFile's replacement policy.
 */
static BufferPage *swapIn(Buffer *f, size_t page_num)
{
  BufferPage *ret;
  rb_red_blk_node *idx_node;
  int isInBuffer = 0;
  int swapInFree = 0;
  int swapInBlock = 0;

  if(f->page && f->page->page_num == page_num)
    return f->page;

  if((idx_node = RBExactQuery(f->page_idx, &page_num)) != NULL)
  {
    /* page is in the buffer */
    ret = (BufferPage *)idx_node->info;
    isInBuffer = 1;
  }
  else if((ret = stack_pop(f->free_list)) != NULL)
  {
    /* we have a free page in the buffer */
    swapInFree = 1;
  }
  else
  {
    /* we need to swap out a page */
    switch (f->rep_policy) {
    case POLICY_LRU:
      ret = ((BufferPage *)f->page_list)->previous;
      break;
    case POLICY_MRU:
      ret = ((BufferPage *)f->page_list);
      break;
    }
    swapInBlock = 1;
  }

  if(isInBuffer)
  {
    if(!ret->isPinned)
    {
      /* remove from page_list */
      if(ret->next == ret)
      {
        f->page_list = NULL;
      }
      else
      {
        if(f->page_list == ret)
          f->page_list = ret->next;
        ret->previous->next = ret->next;
        ret->next->previous = ret->previous;
      }
    }
  }

  /*
   * not in buffer ...
   * either swap into a free or used page
   * if it is a virtual page, i.e., not materialized on disk then just create a null page
   */
  if(swapInFree || swapInBlock)
  {
    if(swapInBlock)
    {
      swapOut(f, ret);
      idx_node = RBExactQuery(f->page_idx, &ret->page_num);
      RBDelete(f->page_idx, idx_node);
    }
    size_t num_file_pages = file_numPages(f->f);
    if(page_num <= num_file_pages)
    {
      file_readPage(((void*)ret)+sizeof(BufferPage), page_num, f->f);
      f->num_read++;
      ret->isDirty = 0;
    }
    else
    {
      memcpy(((void*)ret)+sizeof(BufferPage), f->f->null_page, f->pagesze);
      ret->isDirty = 1;
      f->num_virtual_pages = MAX(f->num_virtual_pages, page_num);
    }
    ret->page_num = page_num;
    ret->isPinned = 0;
    RBTreeInsert(f->page_idx, &ret->page_num, ret);
    if(swapInBlock)
    {
      /* remove from page_list */
      if(ret->next == ret)
      {
        f->page_list = NULL;
      }
      else
      {
        if(f->page_list == ret)
          f->page_list = ret->next;
        ret->previous->next = ret->next;
        ret->next->previous = ret->previous;
      }
    }
  }

  if(!ret->isPinned)
  {
    /* prepend to page_list */
    if(f->page_list)
    {
      ret->next = f->page_list;
      ret->previous = ((BufferPage *)f->page_list)->previous;
      ret->next->previous = ret;
      ret->previous->next = ret;
      f->page_list = ret;
    }
    else
    {
      f->page_list = ret;
      ret->next = ret;
      ret->previous = ret;
    }
  }

  f->page = ret;
  return ret;
}

static int buffer_cmp_size_t(const void *a, const void *b)
{
  return (*(size_t*)a > *(size_t*)b) - (*(size_t*)a < *(size_t*)b);
}

size_t buffer_numReads(Buffer *f)
{
  return f->num_read;
}

size_t buffer_numWrites(Buffer *f)
{
  return f->num_write;
}
