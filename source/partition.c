#include "partition.h"
#include <string.h>



partition *makePartition(int num, HeapAm *rel, char *ofn)
{
	char buff[0x200];
	partition *ret = private_malloc(sizeof(partition));
	ret->id=num;
  ret->num_tuples =0;

  snprintf(buff, 199, "%s.part%d.txt", ofn, num);

  ret->partitionFile= file_create(buff, rel->f->f->type);
  ret->partitionBuffer = buffer_create(ret->partitionFile, WORK_MEM, POLICY_LRU);
  ret->partitionStorage = ExecInitHeapAm(ret->partitionBuffer, rel->htupsze);
  return ret;
}


partition *makePartitionMM(int num, HeapAm *rel, char *ofn)
{
	char buff[0x200];
	partition *ret = private_malloc(sizeof(partition));
	ret->id=num;
  ret->num_tuples =0;

  snprintf(buff, 199, "%s.part%d.txt", ofn, num);

  ret->partitionFile= file_create(buff, rel->f->f->type);
  ret->partitionBuffer = buffer_create(ret->partitionFile, WORK_MEM, POLICY_LRU);
  ret->partitionStorage = ExecInitHeapAm(ret->partitionBuffer, rel->htupsze);
  return ret;
}



partition *
partitiondup(const partition *part)
{
  partition *ret = private_malloc(sizeof(partition));
  ret->id = part->id;
  ret->last = part->last;
  ret->num_tuples = part->num_tuples;
  ret->partitionBuffer=part->partitionBuffer;
  ret->partitionFile=part->partitionFile;
  ret->partitionStorage=part->partitionStorage;
  return ret;
}
