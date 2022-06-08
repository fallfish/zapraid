#ifndef __POLLER_H__
#define __POLLER_H__

#include "common.h"

int handleIoCompletions(void *args);
int ioWorker(void *args);

int handleContextCompletions(void *args);
int dispatchWorker(void *args);

int handleRequestCompletions(void *args);
int controllerWorker(void *args);
#endif
