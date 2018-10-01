
#ifndef PAGE_H_
#define PAGE_H_

typedef struct Page {
  size_t noEntries;
  size_t overflow;
} Page;

#define PAGE_ENTRY_N(page, entry_sze, n) \
  (((void *)page)+sizeof(Page) + (n-1)*entry_sze)

#define PAGE_N_ENTRIES(page) \
  (*(size_t*)page)

#define PAGE_OVERFLOW(page) \
  (*(size_t*)(page+sizeof(size_t)))

typedef struct HTID {
  size_t pageno;
  size_t htupno;
} HTID;

#endif /* PAGE_H_ */
