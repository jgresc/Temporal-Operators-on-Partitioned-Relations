/*
 * AsyncDiskFile.c
 *
 *  Created on: Sep 27, 2013
 *      Author: dignoes
 */

#include "AsyncDiskFile.h"

#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>

struct async_buf {
  size_t pnum;
  void *page;
  bool has_returned; /* true if the asynchronous call has already returned, false otherwise */
  struct aiocb aiocb;
};

static void wait_for_aio_op(const struct aiocb *aiocb);

AsyncDiskFile *asyncdiskfile_create(const char *filename)
{
  AsyncDiskFile *ret = NULL;

  ret = private_malloc(sizeof(AsyncDiskFile));
  ret->p.type = TYPE_DISK_ASYNC;
  ret->fd = open(filename, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  ret->p.pgesze = PAGE_SIZE;
  ret->p.curr_offset = 0;
  ret->p.num_pages = 0;
  ret->p.null_page = private_calloc(1, ret->p.pgesze);
  /* populate buffers */
  ret->readbuf = private_malloc(sizeof(struct async_buf));
  ((struct async_buf *)ret->readbuf)->pnum = 0;
  ((struct async_buf *)ret->readbuf)->has_returned = true;
  memset(&((struct async_buf *)ret->readbuf)->aiocb, 0, sizeof(struct aiocb));
  ((struct async_buf *)ret->readbuf)->aiocb.aio_fildes = ret->fd;
  ((struct async_buf *)ret->readbuf)->aiocb.aio_nbytes = ret->p.pgesze;
  ((struct async_buf *)ret->readbuf)->page = private_malloc(ret->p.pgesze);
  ((struct async_buf *)ret->readbuf)->aiocb.aio_buf = ((struct async_buf *)ret->readbuf)->page;
  ret->writebuf = private_malloc(sizeof(struct async_buf));
  ((struct async_buf *)ret->writebuf)->pnum = 0;
  ((struct async_buf *)ret->writebuf)->has_returned = true;
  memset(&((struct async_buf *)ret->writebuf)->aiocb, 0, sizeof(struct aiocb));
  ((struct async_buf *)ret->writebuf)->aiocb.aio_fildes = ret->fd;
  ((struct async_buf *)ret->writebuf)->aiocb.aio_nbytes = ret->p.pgesze;
  ((struct async_buf *)ret->writebuf)->page = private_malloc(ret->p.pgesze);
  ((struct async_buf *)ret->writebuf)->aiocb.aio_buf = ((struct async_buf *)ret->writebuf)->page;

  return ret;
}

AsyncDiskFile *asyncdiskfile_junktmpcreate(size_t pgsze)
{
  AsyncDiskFile *ret = NULL;
  char tempname[11];

  ret = private_malloc(sizeof(AsyncDiskFile));
  ret->p.type = TYPE_DISK_ASYNC;
  strcpy(tempname, "file");
  strcat(tempname, "XXXXXX");
  ret->fd = mkstemp(tempname);
  unlink(tempname);
  ret->p.pgesze = pgsze;
  ret->p.curr_offset = 0;
  ret->p.num_pages = 0;
  ret->p.null_page = private_calloc(1, ret->p.pgesze);
  /* populate buffers */
  ret->readbuf = private_malloc(sizeof(struct async_buf));
  ((struct async_buf *)ret->readbuf)->pnum = 0;
  ((struct async_buf *)ret->readbuf)->has_returned = true;
  memset(&((struct async_buf *)ret->readbuf)->aiocb, 0, sizeof(struct aiocb));
  ((struct async_buf *)ret->readbuf)->aiocb.aio_fildes = ret->fd;
  ((struct async_buf *)ret->readbuf)->aiocb.aio_nbytes = ret->p.pgesze;
  ((struct async_buf *)ret->readbuf)->page = private_malloc(ret->p.pgesze);
  ((struct async_buf *)ret->readbuf)->aiocb.aio_buf = ((struct async_buf *)ret->readbuf)->page;
  ret->writebuf = private_malloc(sizeof(struct async_buf));
  ((struct async_buf *)ret->writebuf)->pnum = 0;
  ((struct async_buf *)ret->writebuf)->has_returned = true;
  memset(&((struct async_buf *)ret->writebuf)->aiocb, 0, sizeof(struct aiocb));
  ((struct async_buf *)ret->writebuf)->aiocb.aio_fildes = ret->fd;
  ((struct async_buf *)ret->writebuf)->aiocb.aio_nbytes = ret->p.pgesze;
  ((struct async_buf *)ret->writebuf)->page = private_malloc(ret->p.pgesze);
  ((struct async_buf *)ret->writebuf)->aiocb.aio_buf = ((struct async_buf *)ret->writebuf)->page;

  return ret;
}

AsyncDiskFile *asyncdiskfile_tmpcreate()
{
  return asyncdiskfile_junktmpcreate(PAGE_SIZE);
}

void asyncdiskfile_close(AsyncDiskFile *f)
{
  asyncdiskfile_flush(f);
  close(f->fd);
  private_free(f->p.null_page);
  private_free(((struct async_buf *)f->readbuf)->page);
  private_free(f->readbuf);
  private_free(((struct async_buf *)f->writebuf)->page);
  private_free(f->writebuf);
  private_free(f);
}

void asyncdiskfile_readPage(void *dst, size_t page_num, AsyncDiskFile *f)
{
  struct async_buf *readbuf = (struct async_buf *)f->readbuf;
  struct async_buf *writebuf = (struct async_buf *)f->writebuf;
  if(page_num == readbuf->pnum)
  {
    /* Check it is actually read */
    if(!readbuf->has_returned)
    {
      wait_for_aio_op(&readbuf->aiocb);
      readbuf->has_returned = true;
    }
    memcpy(dst, readbuf->page, f->p.pgesze);
  }
  else if(page_num == writebuf->pnum)
  {
    memcpy(dst, writebuf->page, f->p.pgesze);
  }
  else
  {
    /* needs to be read synchronously */
    struct aiocb aiocb;
    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = f->fd;
    aiocb.aio_nbytes = f->p.pgesze;
    aiocb.aio_offset = (off_t)(page_num-1)*f->p.pgesze;
    aiocb.aio_buf = dst;
    if(aio_read(&aiocb) != 0)
    {
      fprintf(stderr, "aio_read error %s:%d:", __FILE__, __LINE__);
      perror("");
    }
    wait_for_aio_op(&aiocb);
  }
  /* data is now in dst, proceed with asynchronously reading next page */
  size_t nextpage = page_num + 1;
  if(nextpage <= f->p.num_pages && nextpage != readbuf->pnum && nextpage != writebuf->pnum)
  {
    if(!readbuf->has_returned)
    {
      /* cancel pending read, if any */
      aio_cancel(f->fd, &readbuf->aiocb);
    }
    readbuf->aiocb.aio_offset = (off_t)(nextpage-1)*f->p.pgesze;
    readbuf->pnum = nextpage;
    if(aio_read(&readbuf->aiocb) != 0)
    {
      fprintf(stderr, "aio_read error %s:%d:", __FILE__, __LINE__);
      perror("");
    }
    readbuf->has_returned = false;
  }
}

static void asyncdiskfile_appendPage(const void *src, AsyncDiskFile *f)
{
  struct async_buf *writebuf = (struct async_buf *)f->writebuf;
  /* make sure last write is finished */
  if(!writebuf->has_returned)
    wait_for_aio_op(&writebuf->aiocb);

  memcpy(writebuf->page, src, f->p.pgesze);
  writebuf->pnum = f->p.num_pages+1;
  writebuf->aiocb.aio_offset = (off_t)(writebuf->pnum-1)*f->p.pgesze;
  if(aio_write(&writebuf->aiocb) != 0)
  {
    fprintf(stderr, "aio_write error %s:%d:", __FILE__, __LINE__);
    perror("");
  }
  writebuf->has_returned = false;
  f->p.num_pages++;
}

void asyncdiskfile_writePage(const void *src, size_t page_num, AsyncDiskFile *f)
{
  while(page_num > f->p.num_pages + 1)
    asyncdiskfile_appendPage(f->p.null_page, f);

  if(page_num == f->p.num_pages+1)
    asyncdiskfile_appendPage(src, f);
  else
  {
    struct async_buf *writebuf = (struct async_buf *)f->writebuf;
    /* make sure last write is finished */
    if(!writebuf->has_returned)
      wait_for_aio_op(&writebuf->aiocb);

    memcpy(writebuf->page, src, f->p.pgesze);
    writebuf->pnum = page_num;
    writebuf->aiocb.aio_offset = (off_t)(page_num-1)*f->p.pgesze;
    if(aio_write(&writebuf->aiocb) != 0)
    {
      fprintf(stderr, "aio_write error %s:%d:", __FILE__, __LINE__);
      perror("");
    }
    writebuf->has_returned = false;
  }
}

size_t asyncdiskfile_numPages(AsyncDiskFile *f)
{
  return f->p.num_pages;
}

void asyncdiskfile_flush(AsyncDiskFile *f)
{
  struct async_buf *writebuf = (struct async_buf *)f->writebuf;
  /* make sure last write is finished */
  if(!writebuf->has_returned)
    wait_for_aio_op(&writebuf->aiocb);
}

static void wait_for_aio_op(const struct aiocb *aiocbp)
{
  if(aio_suspend(&aiocbp, 1, NULL) != 0)
    fprintf(stderr, "aio_suspend: %s\n", strerror(aio_error(aiocbp)));
  if(aio_return((struct aiocb *)aiocbp) != aiocbp->aio_nbytes)
    perror("aio_return: ");
}
