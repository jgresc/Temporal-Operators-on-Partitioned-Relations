/*
 * AsyncDiskFile.h
 *
 *  Created on: Sep 27, 2013
 *      Author: dignoes
 */

#ifndef ASYNCDISKFILE_H_
#define ASYNCDISKFILE_H_

#include "glob.h"
#include "File.h"
#include "list.h"
#include <aio.h>


typedef struct AsyncDiskFile {
  File p;
  int fd;

  void *writebuf;
  void *readbuf;

} AsyncDiskFile;

AsyncDiskFile *asyncdiskfile_create(const char *filename);
AsyncDiskFile *asyncdiskfile_junktmpcreate(size_t pgsze);
AsyncDiskFile *asyncdiskfile_tmpcreate();
void asyncdiskfile_close(AsyncDiskFile *f);
void asyncdiskfile_readPage(void *dst, size_t page_num, AsyncDiskFile *f);
void asyncdiskfile_writePage(const void *src, size_t page_num, AsyncDiskFile *f);
size_t asyncdiskfile_numPages(AsyncDiskFile *f);
void asyncdiskfile_flush(AsyncDiskFile *f);


#endif /* ASYNCDISKFILE_H_ */
