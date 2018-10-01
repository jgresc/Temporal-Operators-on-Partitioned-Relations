/*
 * MemFile.h
 *
 *  Created on: Aug 1, 2012
 *      Author: dignoes
 */

#ifndef MEMFILE_H_
#define MEMFILE_H_

#include <stdio.h>
#include <stdlib.h>
#include "File.h"

#define INODE_ORDER 512

#define MAX_BLK_NUM (INODE_ORDER+INODE_ORDER*INODE_ORDER+INODE_ORDER*INODE_ORDER*INODE_ORDER)

typedef struct INode {
  //void *ptr_block[MAX_BLK_NUM]; /* pointers to blocks */

  void *firstLevel[INODE_ORDER]; /* 128 pointer to blocks */
  void **secondLevel[INODE_ORDER]; /* 128 pointer to 128 pointer to blocks */
  void ***thirdLevel[INODE_ORDER]; /* you can guess ;) */

} INode;

typedef struct MemFile {
  File p;
  INode *inode;
} MemFile;

MemFile *memfile_create();
MemFile *memfile_open();
void memfile_close(MemFile *f);

void memfile_readPage(void *dst, size_t block_num, MemFile *f);
void memfile_writePage(const void *src, size_t block_num, MemFile *f);
size_t memfile_numPages(MemFile *f);


#endif /* MEMFILE_H_ */
