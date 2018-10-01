/*
 * nodePartitionAM.h
 *
 *  Created on: May 30, 2013
 *      Author: dignoes
 */

#ifndef NODEPARTITIONAM_H_
#define NODEPARTITIONAM_H_

#include "glob.h"
#include "ttup.h"
#include "Buffer.h"
#include "Page.h"

typedef struct PartitionAm {
  Buffer *f;

  size_t tupsze;
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

  /*
   * tupleslot
   */
  ttup tup;
  int isNull;
} PartitionAm;


PartitionAm *ExecInitPartitionAm(Buffer *f, size_t htupsze);

void *ExecPartitionAmMakePartition(PartitionAm *pam);

void ExecPartitionAmScanInit(PartitionAm *pam, void *partition);

ttup *ExecPartitionAmScan(PartitionAm *pam);

void ExecPartitionAmScanEnd(PartitionAm *pam, void *partition);

HTID ExecPartitionAmInsert(PartitionAm *pam, ttup *htup, void *partition);

void ExecEndPartitionAm(PartitionAm *pam);

/* additional functions */

bool ExecPartitionCheckHasOverflow(PartitionAm *pam, void *partition);
bool ExecPartitionCheckInsertCausesOverflow(PartitionAm *pam, void *partition);
ttup *ExecPartitionAmScanWithTID(PartitionAm *pam, HTID *tid);
void ExecPartitionAmReplace(PartitionAm *pam, ttup *htup, HTID tid);

#endif /* NODEPARTITIONAM_H_ */
