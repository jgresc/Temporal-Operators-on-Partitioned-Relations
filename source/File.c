
#include "File.h"
#include "MemFile.h"
#include "AsyncDiskFile.h"
#include <unistd.h>
#include <string.h>

static size_t _file_numPages(File *f);

File *file_create(const char *filename, FILE_TYPE type)
{
  File *ret = NULL;
  switch (type) {
    case TYPE_DISK:
      ret = private_malloc(sizeof(File));
      ret->type = TYPE_DISK;
      ret->f = fopen(filename, "wb+");
      ret->pgesze = PAGE_SIZE;
      ret->curr_offset = 0;
      ret->num_pages = 0;
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_DISK_ASYNC:
      return (File *)asyncdiskfile_create(filename);
      break;
    case TYPE_MEM:
      ret = (File *)memfile_create(filename);
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
  return ret;
}

File *tmpfile_create(FILE_TYPE type)
{
  File *ret = NULL;
  char tempname[11];
  switch (type) {
    case TYPE_DISK:
      ret = private_malloc(sizeof(File));
      ret->type = TYPE_DISK;
      strcpy(tempname, "file");
      strcat(tempname, "XXXXXX");
      ret->f = fdopen(mkstemp(tempname), "wb+");
      unlink(tempname);
      ret->pgesze = PAGE_SIZE;
      ret->curr_offset = 0;
      ret->num_pages = 0;
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_DISK_ASYNC:
      return (File *)asyncdiskfile_tmpcreate();
      break;
    case TYPE_MEM:
      ret = (File *)memfile_create("");
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
  return ret;
}

File *junktmpfile_create(size_t pgsze, FILE_TYPE type)
{
  File *ret = NULL;
  char tempname[11];
  switch (type) {
    case TYPE_DISK:
      ret = private_malloc(sizeof(File));
      ret->type = TYPE_DISK;
      strcpy(tempname, "file");
      strcat(tempname, "XXXXXX");
      ret->f = fdopen(mkstemp(tempname), "wb+");
      unlink(tempname);
      ret->pgesze = pgsze;
      ret->curr_offset = 0;
      ret->num_pages = 0;
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_DISK_ASYNC:
      return (File *)asyncdiskfile_junktmpcreate(pgsze);
      break;
    case TYPE_MEM:
      ret = (File *)memfile_create("");
      ret->pgesze = pgsze;
      ret->null_page = private_calloc(1, ret->pgesze);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
  return ret;
}

File *file_open(const char *filename)
{
  File *ret = private_malloc(sizeof(File));
  ret->type = TYPE_DISK;
  ret->f = fopen(filename, "r+");
  ret->pgesze = PAGE_SIZE;
  ret->num_pages = _file_numPages(ret);
  ret->curr_offset = (size_t) ftell(ret->f)/ret->pgesze;
  ret->null_page = private_calloc(1, ret->pgesze);

  return ret;
}

void file_close(File *f)
{
  switch (f->type) {
    case TYPE_DISK:
      fclose(f->f);
      private_free(f->null_page);
      private_free(f);
      break;
    case TYPE_DISK_ASYNC:
      asyncdiskfile_close((AsyncDiskFile *)f);
      break;
    case TYPE_MEM:
      private_free(f->null_page);
      memfile_close((MemFile *)f);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
}

void file_readPage(void *dst, size_t page_num, File *f)
{
  size_t off;
  switch (f->type) {
    case TYPE_DISK:
      off = (page_num-1)*f->pgesze;
      if(f->curr_offset != off)
        if(fseek(f->f, (long)off, SEEK_SET) != 0)
        {
          fprintf(stderr, "fseek error %s:%d:", __FILE__, __LINE__);
          perror("");
        }
      if(fread(dst, f->pgesze, 1, f->f) != 1)
      {
        fprintf(stderr, "fread error %s:%d:", __FILE__, __LINE__);
        perror("");
      }
      f->curr_offset = off+f->pgesze;
      break;
    case TYPE_DISK_ASYNC:
      asyncdiskfile_readPage(dst, page_num, (AsyncDiskFile *)f);
      break;
    case TYPE_MEM:
      memfile_readPage(dst, page_num, (MemFile *)f);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
}

static void file_appendPage(const void *src, File *f)
{
  size_t off;
  switch (f->type) {
    case TYPE_DISK:
      off = f->num_pages*f->pgesze;
      if(f->curr_offset != off)
        if(fseek(f->f, (long)off, SEEK_SET) != 0)
        {
          fprintf(stderr, "fseek error %s:%d:", __FILE__, __LINE__);
          perror("");
        }
      if(fwrite(src, f->pgesze, 1, f->f) != 1)
      {
        fprintf(stderr, "fwrite error %s:%d:", __FILE__, __LINE__);
        perror("");
      }
      f->curr_offset = off+f->pgesze;
      f->num_pages++;
      break;
    case TYPE_MEM:
      fprintf(stderr, "ERROR: Append should not be called on memfile (%s:%d)\n", __FILE__, __LINE__);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
}

void file_writePage(const void *src, size_t page_num, File *f)
{
  switch (f->type) {
    case TYPE_DISK:
      while(page_num > f->num_pages + 1)
        file_appendPage(f->null_page, f);

      if(page_num == f->num_pages+1)
        file_appendPage(src, f);
      else
      {
        size_t off = (page_num-1)*f->pgesze;
        if(f->curr_offset != off)
          if(fseek(f->f, (long)off, SEEK_SET) != 0)
          {
            fprintf(stderr, "fseek error %s:%d:", __FILE__, __LINE__);
            perror("");
          }
        if(fwrite(src, f->pgesze, 1, f->f) != 1)
        {
          fprintf(stderr, "fwrite error %s:%d:", __FILE__, __LINE__);
          perror("");
        }
        f->curr_offset = off+f->pgesze;
      }
      break;
    case TYPE_DISK_ASYNC:
      asyncdiskfile_writePage(src, page_num, (AsyncDiskFile *)f);
      break;
    case TYPE_MEM:
      memfile_writePage(src, page_num, (MemFile *)f);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
}

size_t file_numPages(File *f)
{
  return f->num_pages;
}

static size_t _file_numPages(File *f)
{
  switch (f->type) {
    case TYPE_DISK:
      fseek(f->f, 0L, SEEK_END);
      return (size_t) ftell(f->f)/f->pgesze;
      break;
    case TYPE_MEM:
      fprintf(stderr, "ERROR: Should not be called on memfile (%s:%d)\n", __FILE__, __LINE__);
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
  return 0;
}

void file_flush(File *f)
{
  switch (f->type) {
    case TYPE_DISK:
      fflush(f->f);
      break;
    case TYPE_DISK_ASYNC:
      asyncdiskfile_flush((AsyncDiskFile *)f);
      break;
    case TYPE_MEM:
      ;
      break;
    case TYPE_UNKNOWN:
      fprintf(stderr, "ERROR: File type unknown! (%s:%d)\n", __FILE__, __LINE__);
      break;
    default:
      break;
  }
}
