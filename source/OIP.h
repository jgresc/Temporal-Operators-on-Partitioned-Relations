#ifndef OIP_H_
#define OIP_H_

#include "glob.h"
#include "list.h"
#include "ttup.h"
#include "nodeHeapAm.h"
#include "nodeSort.h"
#include "nodePartitionAM.h"

typedef struct OIP {

  /* config */
  int k;
  timestmp d;
  timestmp o;

  /* partition list */
  List *l;

} OIP;

OIP *
oip_create(List *rel, int k, timestmp d, timestmp off);
OIP *
oip_create_disk(HeapAm *rel, PartitionAm *storage, int k,  timestmp d, timestmp o);
void
oip_close(OIP *oip);
void
oip_close_disk(OIP *oip);
void
oip_select(OIP *oip, timestmp lower, timestmp upper, long *match, long *fhits);
void
oip_join(OIP *outer_oip, OIP *inner_oip, long *match, long *fhits);
void
oip_join_parallel(OIP *outer_oip, OIP *inner_oip, long *match, long *fhits, int num_threads);
void
oip_join_disk(OIP *outer_oip, PartitionAm *outer_storage, OIP *inner_oip, PartitionAm *inner_storage, long *match, long *fhits);
void
oip_tuple_join(List *outer, OIP *inner_oip, long *match, long *fhits);
size_t
oip_num_partitions(OIP *oip);

#endif /* OIP_H_ */
