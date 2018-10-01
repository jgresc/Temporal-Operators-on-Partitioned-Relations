#ifndef RIT_H_
#define RIT_H_

#include "glob.h"
#include "btree.h"
#include "rel.h"

typedef struct RIT {

  BTree *lowerIdx;
  BTree *upperIdx;

  timestmp root;
  timestmp minstep;

  bool onDisk;
  HeapAm *rel; /* relation to fetch tuples from */
  List *mem_rel; /* list with tids */
  bool isClusetered;

} RIT;

RIT *
rit_create(List *rel);
RIT *
rit_create_disk(HeapAm *rel, HeapAm *cluster_storage);
void
rit_close(RIT *rit);
void
rit_select(RIT *rit, timestmp lower, timestmp upper, long *match, long *fhits);
void
rit_UpDownJoin(RIT *outer_rit, RIT *inner_rit, long *match, long *fhits);

#endif /* RIT_H_ */
