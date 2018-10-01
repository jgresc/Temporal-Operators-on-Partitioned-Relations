
#include "nodeHeapAm.h"
#include <string.h>

static void write_heapPage_to_buffer(HeapAm *heap);

HeapAm *ExecInitHeapAm(Buffer *f, size_t htupsze)
{
  HeapAm *ret = private_malloc(sizeof(HeapAm));

  ret->f = f;
  ret->htupsze = htupsze;
  ret->tups_per_page = (ret->f->pagesze-sizeof(Page)) / ret->htupsze;
  ret->page = private_malloc(f->pagesze);
  ret->pageno = 0;
  ret->pageIsDirty = 0;
  ret->scan_done = 1;

  return ret;
}

void ExecHeapAmScanInit(HeapAm *heap)
{
  write_heapPage_to_buffer(heap);
  heap->nextToScan.pageno = 1;
  heap->nextToScan.htupno = 1;
  if(buffer_fileNumPages(heap->f) == 0)
  {
    heap->scan_done = 1;
    heap->isNull = 1;
  }
  else
  {
    heap->scan_done = 0;
    buffer_readPage(heap->page, heap->nextToScan.pageno, heap->f);
    heap->pageno = heap->nextToScan.pageno;
  }
}

void ExecHeapAmScanEnd(HeapAm *heap)
{
  heap->scan_done = 1;
}

ttup *ExecHeapAmScan(HeapAm *heap)
{
  if(heap->scan_done)
    return NULL;

  if(PAGE_N_ENTRIES(heap->page) < heap->nextToScan.htupno)
  {
    if(buffer_fileNumPages(heap->f) <= heap->nextToScan.pageno)
    {
      heap->scan_done = 1;
      heap->isNull = 1;
      return NULL;
    }
    else
    {
      heap->nextToScan.pageno++;
      heap->nextToScan.htupno = 1;
      buffer_readPage(heap->page, heap->nextToScan.pageno, heap->f);
      heap->pageno = heap->nextToScan.pageno;
    }
  }
  ttup *tup = (ttup *)PAGE_ENTRY_N(heap->page, heap->htupsze, heap->nextToScan.htupno);
  heap->tup.ts = tup->ts;
  heap->tup.te = tup->te;
  heap->tup.data = ((void *)tup)+2*sizeof(timestmp);
  heap->nextToScan.htupno++;
  heap->isNull = 0;
  return &heap->tup;
}

ttup *ExecHeapAmScanWithTID(HeapAm *heap, HTID *tid)
{
  if(heap->scan_done)
    return NULL;

  if(PAGE_N_ENTRIES(heap->page) < heap->nextToScan.htupno)
  {
    if(buffer_fileNumPages(heap->f) <= heap->nextToScan.pageno)
    {
      heap->scan_done = 1;
      heap->isNull = 1;
      return NULL;
    }
    else
    {
      heap->nextToScan.pageno++;
      heap->nextToScan.htupno = 1;
      buffer_readPage(heap->page, heap->nextToScan.pageno, heap->f);
      heap->pageno = heap->nextToScan.pageno;
    }
  }
  ttup *tup = (ttup *)PAGE_ENTRY_N(heap->page, heap->htupsze, heap->nextToScan.htupno);
  heap->tup.ts = tup->ts;
  heap->tup.te = tup->te;
  heap->tup.data = ((void *)tup)+2*sizeof(timestmp);
  tid->pageno = heap->pageno;
  tid->htupno = heap->nextToScan.htupno;
  heap->nextToScan.htupno++;
  heap->isNull = 0;
  return &heap->tup;
}

ttup *ExecHeapAmFetch(HeapAm *heap, HTID htid)
{
  if(!heap->scan_done)
  {
    printf("Fetch not possible, a scan is going on!\n");
    return NULL;
  }

  if(buffer_fileNumPages(heap->f) >= htid.pageno)
  {
    if(heap->pageno != htid.pageno)
    {
      write_heapPage_to_buffer(heap->page);
      buffer_readPage(heap->page, htid.pageno, heap->f);
      heap->pageno = htid.pageno;
    }

    if(PAGE_N_ENTRIES(heap->page) >= htid.htupno)
    {
      ttup *tup = (ttup *)PAGE_ENTRY_N(heap->page, heap->htupsze, htid.htupno);
      heap->tup.ts = tup->ts;
      heap->tup.te = tup->te;
      heap->tup.data = ((void *)tup)+2*sizeof(timestmp);
      heap->isNull = 0;
      return &heap->tup;
    }
  }
  heap->isNull = 1;
  return NULL;
}

HTID ExecHeapAmInsert(HeapAm *heap, ttup *htup)
{
  HTID ret;
  if(!heap->scan_done)
  {
    printf("Insert not possible, a scan is going on!\n");
    ret.htupno = 0;
    ret.pageno = 0;
    return ret;
  }

  size_t pageToInsert = buffer_fileNumPages(heap->f);
  if(pageToInsert < heap->pageno)
    pageToInsert = heap->pageno;

  int append = 0;
  if(pageToInsert > 0)
  {
    if(heap->pageno != pageToInsert)
    {
      write_heapPage_to_buffer(heap);
      buffer_readPage(heap->page, pageToInsert, heap->f);
      heap->pageno = pageToInsert;
    }
    if(PAGE_N_ENTRIES(heap->page) >= heap->tups_per_page)
    {
      append = 1;
      pageToInsert++;
    }
  }
  else
  {
    append = 1;
    pageToInsert = 1;
  }

  if(append)
  {
    write_heapPage_to_buffer(heap);
    PAGE_N_ENTRIES(heap->page) = 0;
    heap->pageno = pageToInsert;
  }

  /*
   * insert
   */
  PAGE_N_ENTRIES(heap->page)++;
  ttup *tup = (ttup *)PAGE_ENTRY_N(heap->page, heap->htupsze, PAGE_N_ENTRIES(heap->page));
  tup->ts = htup->ts;
  tup->te = htup->te;
  memcpy(((void*)tup)+2*sizeof(timestmp), htup->data, heap->htupsze-2*sizeof(timestmp));
  ret.pageno = pageToInsert;
  ret.htupno = PAGE_N_ENTRIES(heap->page);
  heap->pageIsDirty = 1;

  return ret;
}

static void write_heapPage_to_buffer(HeapAm *heap)
{
  if(heap->pageIsDirty)
  {
    buffer_writePage(heap->page, heap->pageno, heap->f);
    heap->pageIsDirty = 0;
  }
}

void ExecEndHeapAM(HeapAm *heap)
{
  write_heapPage_to_buffer(heap);
  private_free(heap->page);
  private_free(heap);
}

void ExecMarkPos(HeapAm *heap)
{
  heap->markedPos = heap->nextToScan;
}

void ExecRestorPos(HeapAm *heap)
{
  if(heap->pageno != heap->markedPos.pageno)
  {
    buffer_readPage(heap->page, heap->markedPos.pageno, heap->f);
    heap->pageno = heap->markedPos.pageno;
  }
  heap->nextToScan = heap->markedPos;
  heap->scan_done = 0;
}
