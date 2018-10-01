#ifndef SEQSCAN_H_
#define SEQSCAN_H_

#include "glob.h"
#include "list.h"
#include "ttup.h"

void
seqscan_select(List *rel, timestmp lower, timestmp upper, long *match, long *fhits);
void
seqscan_join(List *outer_rel, List *inner_rel, long *match, long *fhits);

#endif /* SEQSCAN_H_ */
