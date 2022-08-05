#ifndef __POLLER_H__
#define __POLLER_H__

#include "common.h"
void handleContext(RequestContext *context);

int handleIoCompletions(void *args);
int handleRequestCompletions(void *args);
int ioWorker(void *args);

int handleContextCompletions(void *args);
int dispatchWorker(void *args);

int ecWorker(void *args);
int indexWorker(void *args);
int completionWorker(void *args);

void handleEventsCompletionsOneEvent(void *args);
#endif
