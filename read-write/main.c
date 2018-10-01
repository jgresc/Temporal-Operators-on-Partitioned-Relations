/*
 * main.c
 *
 *  Created on: Oct 3, 2013
 *      Author: dignoes
 */

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <aio.h>
#include <errno.h>
#include <stdlib.h>
#include "timing.h"


#define PGSZE 4096

static void write_async(const char *fname, size_t npages);
static void read_async(const char *fname, size_t npages);

static void write_sync(const char *fname, size_t npages);
static void read_sync(const char *fname, size_t npages);

void compute();

int main(int argc, char *argv[])
{
  long npages;
  void *thandle;

  if(argc < 4)
  {
    fprintf(stderr, "Usage: %s <s|a> <r|w> <size in bytes>\n", argv[0]);
    exit(-1);
  }

  npages = atol(argv[3]) / PGSZE;
  npages = atol(argv[3]) % PGSZE == 0 ? npages : npages+1;

  if(strcmp(argv[2], "r") == 0)
  {
    thandle = time_start();
    if(strcmp(argv[1], "a") == 0)
      read_async("x.dat", npages);
    else if(strcmp(argv[1], "s") == 0)
      read_sync("x.dat", npages);
    else
    {
      fprintf(stderr, "Usage: %s <r|w> <size in bytes>\n", argv[0]);
      exit(-1);
    }
    printf("Time: ");
    time_pprint(thandle);
    printf(" sec.\n");
    time_end(thandle);
  }
  else if(strcmp(argv[2], "w") == 0)
  {
    thandle = time_start();
    if(strcmp(argv[1], "a") == 0)
      write_async("x.dat", npages);
    else if(strcmp(argv[1], "s") == 0)
      write_sync("x.dat", npages);
    else
    {
      fprintf(stderr, "Usage: %s <r|w> <size in bytes>\n", argv[0]);
      exit(-1);
    }
    printf("Time: ");
    time_pprint(thandle);
    printf(" sec.\n");
    time_end(thandle);
  }
  else
  {
    fprintf(stderr, "Usage: %s <r|w> <size in bytes>\n", argv[0]);
    exit(-1);
  }

  return 0;
}

static void write_async(const char *fname, size_t npages)
{
  void *page = calloc(1, PGSZE);
  int fd;
  struct aiocb aiocb;
  size_t i;
  const struct aiocb *const aiocblist[] = {&aiocb};

  unlink(fname);
  fd = open(fname, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
  if(fd == -1)
  {
    fprintf(stderr, "open error %s:%d: ", __FILE__, __LINE__);
    perror("");
    exit(1);
  }

  memset(&aiocb, 0, sizeof(struct aiocb));

  aiocb.aio_fildes = fd;
  aiocb.aio_nbytes = PGSZE;
  aiocb.aio_offset = 0L;
  aiocb.aio_buf = page;

  size_t cnt = 0;
  for(i = 0; i < npages; i++)
  {
    *(size_t*) page = i;
    *(size_t*) (page + PGSZE - sizeof(size_t)) = i;

    if(aio_write(&aiocb) != 0)
    {
      perror("aio_write: ");
      exit(-1);
    }

    compute();

    if(aio_suspend(aiocblist, 1, NULL) != 0)
    {
      fprintf(stderr, "aio_suspend: %s\n", strerror(aio_error(&aiocb)));
      exit(-1);
    }
    if(aio_return(&aiocb) != PGSZE)
    {
      perror("aio_return: ");
      exit(-1);
    }
    aiocb.aio_offset += PGSZE;
    cnt++;
  }
  close(fd);
  free(page);
  size_t gb = cnt*PGSZE / 1073741824;
  size_t mb = (cnt*PGSZE - gb*1073741824) / 1048576;
  size_t kb = (cnt*PGSZE - gb*1073741824 - mb*1048576) / 1024;
  size_t b = cnt*PGSZE % 1024;

  printf("Correctly written using aio_write %ld Gb %ld Mb %ld kb %ld bytes\n", gb, mb, kb, b);
}

static void read_sync(const char *fname, size_t npages)
{
  void *page = calloc(1, PGSZE);
  int fd;
  size_t i;

  fd = open(fname, O_RDONLY | S_IRUSR | S_IWUSR);
  if(fd == -1)
  {
    fprintf(stderr, "open error %s:%d: ", __FILE__, __LINE__);
    perror("");
    exit(1);
  }

  size_t cnt = 0;
  for(i = 0; i < npages; i++)
  {
    if(read(fd, page, PGSZE) != PGSZE)
    {
      perror("read: ");
      exit(-1);
    }

    if((*(size_t*) page) != i || (*(size_t*) (page + PGSZE - sizeof(size_t))) != i)
    {
      fprintf(stderr, "Read/Write error expected %ld\n", i);
      exit(-1);
    }
    cnt++;
    compute();
  }
  close(fd);
  free(page);

  size_t gb = cnt*PGSZE / 1073741824;
  size_t mb = (cnt*PGSZE - gb*1073741824) / 1048576;
  size_t kb = (cnt*PGSZE - gb*1073741824 - mb*1048576) / 1024;
  size_t b = cnt*PGSZE % 1024;

  printf("Correctly read using read %ld Gb %ld Mb %ld kb %ld bytes\n", gb, mb, kb, b);
}

static void write_sync(const char *fname, size_t npages)
{
  void *page = calloc(1, PGSZE);
  int fd;
  size_t i;

  unlink(fname);
  fd = open(fname, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR);
  if(fd == -1)
  {
    fprintf(stderr, "open error %s:%d: ", __FILE__, __LINE__);
    perror("");
    exit(1);
  }

  size_t cnt = 0;
  for(i = 0; i < npages; i++)
  {

    *(size_t*) page = i;
    *(size_t*) (page + PGSZE - sizeof(size_t)) = i;
    if(write(fd, page, PGSZE) != PGSZE)
    {
      perror("write: ");
      exit(-1);
    }
    cnt++;
    compute();
  }
  close(fd);
  free(page);

  size_t gb = cnt*PGSZE / 1073741824;
  size_t mb = (cnt*PGSZE - gb*1073741824) / 1048576;
  size_t kb = (cnt*PGSZE - gb*1073741824 - mb*1048576) / 1024;
  size_t b = cnt*PGSZE % 1024;

  printf("Correctly written using write %ld Gb %ld Mb %ld kb %ld bytes\n", gb, mb, kb, b);
}

static void read_async(const char *fname, size_t npages)
{
  void *page = calloc(1, PGSZE);
  int fd;
  struct aiocb aiocb;
  size_t i;
  const struct aiocb *const aiocblist[] = {&aiocb};

  fd = open(fname, O_RDONLY | S_IRUSR | S_IWUSR);
  if(fd == -1)
  {
    fprintf(stderr, "open error %s:%d: ", __FILE__, __LINE__);
    perror("");
    exit(1);
  }

  memset(&aiocb, 0, sizeof(struct aiocb));

  aiocb.aio_fildes = fd;
  aiocb.aio_nbytes = PGSZE;
  aiocb.aio_offset = 0L;
  aiocb.aio_buf = page;

  size_t cnt = 0;
  for(i = 0; i < npages; i++)
  {
    if(aio_read(&aiocb) != 0)
    {
      perror("aio_read: ");
      exit(-1);
    }

    compute();

    if(aio_suspend(aiocblist, 1, NULL) != 0)
    {
      fprintf(stderr, "aio_suspend: %s\n", strerror(aio_error(&aiocb)));
      exit(-1);
    }
    if(aio_return(&aiocb) != PGSZE)
    {
      perror("aio_return: ");
      exit(-1);
    }

    if((*(size_t*) page) != i || (*(size_t*) (page + PGSZE - sizeof(size_t))) != i)
    {
      fprintf(stderr, "Read/Write error expected %ld\n", i);
      exit(-1);
    }
    aiocb.aio_offset += PGSZE;
    cnt++;
  }
  close(fd);
  free(page);

  size_t gb = cnt*PGSZE / 1073741824;
  size_t mb = (cnt*PGSZE - gb*1073741824) / 1048576;
  size_t kb = (cnt*PGSZE - gb*1073741824 - mb*1048576) / 1024;
  size_t b = cnt*PGSZE % 1024;

  printf("Correctly read using aio_read %ld Gb %ld Mb %ld kb %ld bytes\n", gb, mb, kb, b);
}

void compute()
{
  struct timespec req;
  req.tv_nsec = 100000;
  req.tv_sec = 0;

  nanosleep(&req, NULL);
}


