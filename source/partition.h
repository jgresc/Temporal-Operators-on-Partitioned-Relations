#ifndef PARTITION_H_
#define PARTITION_H_

#include "glob.h"
#include "ttup.h"

#include "File.h"
#include "Buffer.h"
#include "File.h"
#include "nodeHeapAm.h"



typedef struct partition {
  int id;
  ttup *last;
  ttup *prev_last;
  int num_tuples;
  File *partitionFile;
  Buffer *partitionBuffer;
  HeapAm *partitionStorage;

} partition;


typedef struct partitionMM {
  int id;
  ttup *last;
  ttup *prev_last;
  int num_tuples;
  File *partitionFile;
  Buffer *partitionBuffer;
  HeapAm *partitionStorage;

} partitionMM;


partition *makePartition(int num, HeapAm *rel, char *ofn);


#endif /* PARTITION_H_ */
