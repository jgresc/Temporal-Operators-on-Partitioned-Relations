#ifndef TTUP_H_
#define TTUP_H_

#include "glob.h"


typedef struct ttup {
  timestmp ts;
  timestmp te;
  void *data;
} ttup;

ttup *
makeTTup(timestmp ts, timestmp te, void *data);


void
freeTTup(ttup *tup);

ttup *
ttupdup(const ttup *tup, size_t tupsze);

#endif /* TTUP_H_ */
