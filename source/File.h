
#ifndef FILE_H_
#define FILE_H_

#include <stdio.h>
#include <stdlib.h>
#include "glob.h"

#define PAGE_SIZE 4096

typedef enum FILE_TYPE {
  TYPE_UNKNOWN,
  TYPE_DISK,
  TYPE_MEM,
  TYPE_DISK_ASYNC,
} FILE_TYPE;

typedef struct File {
  FILE_TYPE type;
  size_t pgesze;
  FILE *f;

  size_t curr_offset;
  size_t num_pages;

  void *null_page;
} File;

File *file_create(const char *filename, FILE_TYPE type);
File *tmpfile_create(FILE_TYPE type);
File *junktmpfile_create(size_t pgsze, FILE_TYPE type);
File *file_open(const char *filename);
void file_close(File *f);

void file_readPage(void *dst, size_t page_num, File *f);
void file_writePage(const void *src, size_t page_num, File *f);
void file_flush(File *f);
size_t file_numPages(File *f);

#endif /*
FILE_H_ */
