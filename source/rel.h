#ifndef REL_H_
#define REL_H_

#include "glob.h"
#include "list.h"
#include "ttup.h"
#include "nodeHeapAm.h"

List *
makeRandRel(long n, long l, timestmp lower, timestmp upper);
HeapAm *
makeRandDiskRel(const char *name, size_t tupsze, long n, long l, timestmp lower, timestmp upper, FILE_TYPE type);
List *
makeGaussRandRel(long n, long mean, long stddev, timestmp lower, timestmp upper);
HeapAm *
makeGaussRandDiskRel(const char *name, size_t tupsze, long n, long mean, long stddev, timestmp lower, timestmp upper, FILE_TYPE type);
List *
makeRelFromFile(char *from_filename);
HeapAm *
makeDiskRelFromFile(const char *name, size_t tupsze, const char *from_filename, FILE_TYPE type);
HeapAm *
makeSeqDESCDiskRel(const char *name, size_t tupsze, long n, long l, FILE_TYPE type);
void
freeRel(List *rel);
void
freeDiskRel(HeapAm *rel);

#endif /* REL_H_ */
