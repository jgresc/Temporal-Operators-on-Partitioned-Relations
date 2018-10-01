
#include "nodePartitionAM.h"
#include <string.h>

typedef struct partition_struct
{
  size_t first_block;
  size_t last_block;
  size_t num_tups;
} partition_struct;

static void write_page_to_buffer(PartitionAm *heap);

PartitionAm *ExecInitPartitionAm(Buffer *f, size_t tupsze)
{
  PartitionAm *ret = private_malloc(sizeof(PartitionAm));

  ret->f = f;
  ret->tupsze = tupsze;
  ret->tups_per_page = (ret->f->pagesze-sizeof(Page)) / ret->tupsze;
  ret->page = private_malloc(f->pagesze);
  ret->pageno = 0;
  ret->pageIsDirty = 0;
  ret->scan_done = 1;

  return ret;
}

void *ExecPartitionAmMakePartition(PartitionAm *pam)
{
  partition_struct *ret = private_malloc(sizeof(partition_struct));
  ret->first_block = buffer_fileNumPages(pam->f) + 1;
  if(ret->first_block == pam->pageno)
    ret->first_block++;
  write_page_to_buffer(pam);
  PAGE_N_ENTRIES(pam->page) = 0;
  PAGE_OVERFLOW(pam->page) = 0;
  pam->pageno = ret->first_block;
  pam->pageIsDirty = 1;
  ret->last_block = ret->first_block;
  ret->num_tups = 0;
  return ret;
}

void ExecPartitionAmScanInit(PartitionAm *pam, void *partition)
{
  write_page_to_buffer(pam);
  pam->nextToScan.pageno = ((partition_struct*) partition)->first_block;
  pam->nextToScan.htupno = 1;
  if(buffer_fileNumPages(pam->f) < pam->nextToScan.pageno)
    pam->scan_done = 1;
  else
  {
    pam->scan_done = 0;
    buffer_readPage(pam->page, pam->nextToScan.pageno, pam->f);
    pam->pageno = pam->nextToScan.pageno;
  }
}

void ExecPartitionAmScanEnd(PartitionAm *pam, void *partition)
{
  pam->scan_done = 1;
}

ttup *ExecPartitionAmScan(PartitionAm *pam)
{
  if(pam->scan_done)
    return NULL;

  if(PAGE_N_ENTRIES(pam->page) < pam->nextToScan.htupno)
  {
    if(PAGE_OVERFLOW(pam->page) == 0 || buffer_fileNumPages(pam->f) < PAGE_OVERFLOW(pam->page))
    {
      pam->scan_done = 1;
      pam->isNull = 1;
      return NULL;
    }
    else
    {
      pam->nextToScan.pageno = PAGE_OVERFLOW(pam->page);
      pam->nextToScan.htupno = 1;
      buffer_readPage(pam->page, pam->nextToScan.pageno, pam->f);
      pam->pageno = pam->nextToScan.pageno;
    }
  }
  ttup *tup = (ttup *)PAGE_ENTRY_N(pam->page, pam->tupsze, pam->nextToScan.htupno);
  pam->tup.ts = tup->ts;
  pam->tup.te = tup->te;
  pam->tup.data = ((void *)tup)+2*sizeof(timestmp);
  pam->nextToScan.htupno++;
  pam->isNull = 0;
  return &pam->tup;
}

ttup *ExecPartitionAmScanWithTID(PartitionAm *pam, HTID *tid)
{
  if(pam->scan_done)
    return NULL;

  if(PAGE_N_ENTRIES(pam->page) < pam->nextToScan.htupno)
  {
    if(PAGE_OVERFLOW(pam->page) == 0 || buffer_fileNumPages(pam->f) < PAGE_OVERFLOW(pam->page))
    {
      pam->scan_done = 1;
      pam->isNull = 1;
      return NULL;
    }
    else
    {
      pam->nextToScan.pageno = PAGE_OVERFLOW(pam->page);
      pam->nextToScan.htupno = 1;
      buffer_readPage(pam->page, pam->nextToScan.pageno, pam->f);
      pam->pageno = pam->nextToScan.pageno;
    }
  }
  ttup *tup = (ttup *)PAGE_ENTRY_N(pam->page, pam->tupsze, pam->nextToScan.htupno);
  pam->tup.ts = tup->ts;
  pam->tup.te = tup->te;
  pam->tup.data = ((void *)tup)+2*sizeof(timestmp);
  tid->pageno = pam->pageno;
  tid->htupno = pam->nextToScan.htupno;
  pam->nextToScan.htupno++;
  pam->isNull = 0;
  return &pam->tup;
}

bool ExecPartitionCheckHasOverflow(PartitionAm *pam, void *partition)
{
  return ((partition_struct *)partition)->first_block != ((partition_struct *)partition)->last_block;
}

bool ExecPartitionCheckInsertCausesOverflow(PartitionAm *pam, void *partition)
{
  if(((partition_struct *)partition)->num_tups == 0)
    return 0;
  return (((partition_struct *)partition)->num_tups) % pam->tups_per_page == 0;
}


HTID ExecPartitionAmInsert(PartitionAm *pam, ttup *htup, void *partition)
{
  HTID ret;
  if(!pam->scan_done)
  {
    printf("Insert not possible, a scan is going on!\n");
    ret.htupno = 0;
    ret.pageno = 0;
    return ret;
  }

  size_t pageToInsert = ((partition_struct *)partition)->last_block;

  int append = 0;

  if(pam->pageno != pageToInsert)
  {
    write_page_to_buffer(pam);
    buffer_readPage(pam->page, pageToInsert, pam->f);
    pam->pageno = pageToInsert;
  }
  if(PAGE_N_ENTRIES(pam->page) >= pam->tups_per_page)
  {
    pageToInsert = buffer_fileNumPages(pam->f) + 1;
    if(pageToInsert == pam->pageno)
      pageToInsert++;
    PAGE_OVERFLOW(pam->page) = pageToInsert;
    pam->pageIsDirty = 1;
    append = 1;
  }

  if(append)
  {
    write_page_to_buffer(pam);
    PAGE_N_ENTRIES(pam->page) = 0;
    PAGE_OVERFLOW(pam->page) = 0;
    pam->pageno = pageToInsert;
    ((partition_struct *)partition)->last_block = pageToInsert;
  }

  /*
   * insert
   */
  PAGE_N_ENTRIES(pam->page)++;
  ttup *tup = (ttup *)PAGE_ENTRY_N(pam->page, pam->tupsze, PAGE_N_ENTRIES(pam->page));
  tup->ts = htup->ts;
  tup->te = htup->te;
  memcpy(((void*)tup)+2*sizeof(timestmp), htup->data, pam->tupsze-2*sizeof(timestmp));
  ret.pageno = pageToInsert;
  ret.htupno = PAGE_N_ENTRIES(pam->page);
  pam->pageIsDirty = 1;
  ((partition_struct *)partition)->num_tups++;

  return ret;
}

void ExecPartitionAmReplace(PartitionAm *pam, ttup *htup, HTID tid)
{
  if(pam->pageno != tid.pageno)
  {
    write_page_to_buffer(pam);
    buffer_readPage(pam->page, tid.pageno, pam->f);
    pam->pageno = tid.pageno;
  }
  if(PAGE_N_ENTRIES(pam->page) >= tid.htupno)
  {
    ttup *tup = (ttup *)PAGE_ENTRY_N(pam->page, pam->tupsze, tid.htupno);
    tup->ts = htup->ts;
    tup->te = htup->te;
    memcpy(((void*)tup)+2*sizeof(timestmp), htup->data, pam->tupsze-2*sizeof(timestmp));
    pam->pageIsDirty = 1;
  }
  else
  {
    fprintf(stderr, "ERROR: x %s:%d\n", __FILE__, __LINE__);
  }
}

static void write_page_to_buffer(PartitionAm *pam)
{
  if(pam->pageIsDirty)
  {
    buffer_writePage(pam->page, pam->pageno, pam->f);
    pam->pageIsDirty = 0;
  }
}

void ExecEndPartitionAm(PartitionAm *pam)
{
  write_page_to_buffer(pam);
  private_free(pam->page);
  private_free(pam);
}

