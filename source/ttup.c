#include "ttup.h"
#include <string.h>


ttup *makeTTup(timestmp ts, timestmp te, void *data)
{
  ttup *ret = private_malloc(sizeof(ttup));
  ret->ts = ts;
  ret->te = te;
  ret->data = data;
  return ret;
}


void freeTTup(ttup *tup)
{
  if(tup->data != NULL)
    private_free(tup->data);
  private_free(tup);
}

ttup *
ttupdup(const ttup *tup, size_t tupsze)
{
  ttup *ret = private_malloc(sizeof(ttup));
  ret->ts = tup->ts;
  ret->te = tup->te;
  ret->data = private_malloc(tupsze-2*sizeof(timestmp));
  memcpy(ret->data, tup->data, tupsze-2*sizeof(timestmp));
  return ret;
}
