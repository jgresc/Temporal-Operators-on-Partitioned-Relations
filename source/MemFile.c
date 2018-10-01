/*
 * MemFile.c
 *
 *  Created on: Aug 1, 2012
 *      Author: dignoes
 */

#include "MemFile.h"
#include <string.h>
#include "glob.h"

MemFile *memfile_create(char *filename)
{
  MemFile *ret = private_malloc(sizeof(MemFile));
  ret->p.type = TYPE_MEM;

  ret->p.pgesze = PAGE_SIZE / 8; /* 512 byte */
  ret->p.num_pages = 0;
  ret->inode = private_malloc(sizeof(INode));

  return ret;
}

MemFile *memfile_open(char *filename)
{
  fprintf(stderr, "ERROR: MemFile can not be opened must be created! (%s:%d)\n", __FILE__, __LINE__);
  exit(0);
}

void memfile_close(MemFile *f)
{
  size_t block_num;
  for(block_num = f->p.num_pages; block_num >= 1; block_num--)
  {
    if(block_num <= INODE_ORDER)
    {
      /* first level */
      private_free(f->inode->firstLevel[block_num-1]);
    }
    else if(block_num <= INODE_ORDER+INODE_ORDER*INODE_ORDER)
    {
      /* second level */
      int first_off = (block_num-1-INODE_ORDER)/INODE_ORDER;
      int secnd_off = (block_num-1-INODE_ORDER)%INODE_ORDER;
      private_free(*(f->inode->secondLevel[first_off]+secnd_off));
      if(secnd_off == 0)
        private_free(f->inode->secondLevel[first_off]);
    }
    else
    {
      /* third level */
      int first_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/(INODE_ORDER*INODE_ORDER);
      int secnd_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/INODE_ORDER%INODE_ORDER;
      int third_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)%INODE_ORDER;

      private_free(*(*(f->inode->thirdLevel[first_off]+secnd_off)+third_off));
      if(third_off == 0)
        private_free(*(f->inode->thirdLevel[first_off]+secnd_off));
      if(secnd_off == 0 && third_off == 0)
        private_free(f->inode->thirdLevel[first_off]);
    }

  }

  private_free(f->inode);
  private_free(f);
}

void memfile_readPage(void *dst, size_t block_num, MemFile *f)
{
  if(block_num <= f->p.num_pages)
  {

    if(block_num <= INODE_ORDER)
    {
      /* first level */
      memcpy(dst, f->inode->firstLevel[block_num-1], f->p.pgesze);
    }
    else if(block_num <= INODE_ORDER+INODE_ORDER*INODE_ORDER)
    {
      /* second level */
      int first_off = (block_num-1-INODE_ORDER)/INODE_ORDER;
      int secnd_off = (block_num-1-INODE_ORDER)%INODE_ORDER;
      memcpy(dst, *(f->inode->secondLevel[first_off]+secnd_off), f->p.pgesze);
    }
    else
    {
      /* third level */
      int first_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/(INODE_ORDER*INODE_ORDER);
      int secnd_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/INODE_ORDER%INODE_ORDER;
      int third_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)%INODE_ORDER;
      memcpy(dst, *(*(f->inode->thirdLevel[first_off]+secnd_off)+third_off), f->p.pgesze);
    }
    return;
  }

  fprintf(stderr, "ERROR: MemFile out of bound! (%s:%d)\n", __FILE__, __LINE__);
  exit(0);
}

static size_t _memfile_appendPage(const void *src, MemFile *f)
{
  size_t block_num = f->p.num_pages+1;

  if(block_num <= MAX_BLK_NUM)
  {
    if(block_num <= INODE_ORDER)
    {
      /* first level */
      f->inode->firstLevel[block_num-1] = private_malloc(f->p.pgesze);
      memcpy(f->inode->firstLevel[block_num-1], src, f->p.pgesze);
    }
    else if(block_num <= INODE_ORDER+INODE_ORDER*INODE_ORDER)
    {
      /* second level */
      int first_off = (block_num-1-INODE_ORDER)/INODE_ORDER;
      int secnd_off = (block_num-1-INODE_ORDER)%INODE_ORDER;
      if(secnd_off == 0)
        f->inode->secondLevel[first_off] = private_malloc(INODE_ORDER*sizeof(void *));

      *(f->inode->secondLevel[first_off]+secnd_off) = private_malloc(f->p.pgesze);
      memcpy(*(f->inode->secondLevel[first_off]+secnd_off), src, f->p.pgesze);
    }
    else
    {

      /* third level */
      int first_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/(INODE_ORDER*INODE_ORDER);
      int secnd_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/INODE_ORDER%INODE_ORDER;
      int third_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)%INODE_ORDER;

      if(secnd_off == 0 && third_off == 0)
        f->inode->thirdLevel[first_off] = private_malloc(INODE_ORDER*sizeof(void **));
      if(third_off == 0)
        *(f->inode->thirdLevel[first_off]+secnd_off) = private_malloc(INODE_ORDER*sizeof(void *));

      *(*(f->inode->thirdLevel[first_off]+secnd_off)+third_off) = private_malloc(f->p.pgesze);
      memcpy(*(*(f->inode->thirdLevel[first_off]+secnd_off)+third_off), src, f->p.pgesze);

    }
    f->p.num_pages++;
    return f->p.num_pages;
  }

  fprintf(stderr, "ERROR: MemFile can not exceed %d blocks!  (%s:%d)\n", MAX_BLK_NUM , __FILE__, __LINE__);
  exit(0);
}

void memfile_writePage(const void *src, size_t block_num, MemFile *f)
{

  while(block_num > f->p.num_pages + 1  || f->p.num_pages == 0)
    _memfile_appendPage(f->p.null_page, f);

  if(block_num == f->p.num_pages+1)
    _memfile_appendPage(src, f);
  else
  {
    if(block_num <= INODE_ORDER)
    {
      /* first level */
      memcpy(f->inode->firstLevel[block_num-1], src, f->p.pgesze);
    }
    else if(block_num <= INODE_ORDER+INODE_ORDER*INODE_ORDER)
    {
      /* second level */
      int first_off = (block_num-1-INODE_ORDER)/INODE_ORDER;
      int secnd_off = (block_num-1-INODE_ORDER)%INODE_ORDER;
      memcpy(*(f->inode->secondLevel[first_off]+secnd_off), src, f->p.pgesze);
    }
    else
    {
      /* third level */
      int first_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/(INODE_ORDER*INODE_ORDER);
      int secnd_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)/INODE_ORDER%INODE_ORDER;
      int third_off = (block_num-1-INODE_ORDER-INODE_ORDER*INODE_ORDER)%INODE_ORDER;
      memcpy(*(*(f->inode->thirdLevel[first_off]+secnd_off)+third_off), src, f->p.pgesze);
    }
  }
}

size_t memfile_numPages(MemFile *f)
{
  return f->p.num_pages;
}

