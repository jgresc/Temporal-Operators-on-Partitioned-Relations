#ifndef NODEHEAPAM_H_
#define NODEHEAPAM_H_

#include "glob.h"
#include "ttup.h"
#include "Buffer.h"
#include "Page.h"

typedef struct HeapAm {
  Buffer *f;

  size_t htupsze;
  size_t tups_per_page;

  /*
   * one page for fetch, scan, and insert is buffered independently of the underlying buffer
   */
  void *page;
  size_t pageno;
  int pageIsDirty;

  /*
   * Context for scan
   */
  HTID nextToScan;
  int scan_done;

  HTID markedPos;

  /*
   * tupleslot
   */
  ttup tup;
  int isNull;
} HeapAm;


HeapAm *ExecInitHeapAm(Buffer *f, size_t htupsze);

void ExecHeapAmScanInit(HeapAm *heap);

ttup *ExecHeapAmScan(HeapAm *heap);

void ExecMarkPos(HeapAm *heap);

void ExecRestorPos(HeapAm *heap);

ttup *ExecHeapAmScanWithTID(HeapAm *heap, HTID *tid);

void ExecHeapAmScanEnd(HeapAm *heap);

ttup *ExecHeapAmFetch(HeapAm *heap, HTID htid);

HTID ExecHeapAmInsert(HeapAm *heap, ttup *htup);

void ExecEndHeapAM(HeapAm *heap);

#endif /* NODEHEAPAM_H_ */
