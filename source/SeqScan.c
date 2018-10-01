#include "SeqScan.h"

void seqscan_select(List *rel, timestmp lower, timestmp upper, long *match, long *fhits)
{
  ListCell *tup_cell;
  ttup *tup;

  foreach(tup_cell, rel)
  {
    tup = (ttup *)lfirst(tup_cell);
    if(tup->ts <= upper && tup->te >= lower)
      (*match)++;
    else
      (*fhits)++;
  }
}

void seqscan_join(List *outer_rel, List *inner_rel, long *match, long *fhits)
{
  ListCell *outer_tup_cell, *inner_tup_cell;
  ttup *outer_tup, *inner_tup;

  foreach(outer_tup_cell, outer_rel)
  {
    outer_tup = (ttup *)lfirst(outer_tup_cell);
    foreach(inner_tup_cell, inner_rel)
    {
      inner_tup = (ttup *)lfirst(inner_tup_cell);
      if(inner_tup->ts <= outer_tup->te && inner_tup->te >= outer_tup->ts)
        (*match)++;
      else
        (*fhits)++;
    }
  }
}
