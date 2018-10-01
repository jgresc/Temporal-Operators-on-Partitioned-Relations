#ifndef TIMING_H_
#define TIMING_H_

#include <sys/time.h>

/*
 * starts a timer and returns its handle
 */
void *time_start();

/*
 * prints the currently elapsed time of the timer
 * referenced by handle in seconds
 */
void time_pprint(void *handle);

/*
 * ends the timer referenced by handle and
 * returns the elapsed time in microseconds
 */
long long time_end(void *handle);

#endif /* TIMING_H_ */
