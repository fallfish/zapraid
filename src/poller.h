#ifndef __POLLER_H__
#define __POLLER_H__

#include "common.h"

struct DrainArgs {
  RAIDController *ctrl;
  bool success;
  bool ready;
};

struct UpdatePbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

struct QueryPbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

void tryDrainController(void *args);

// Used under spdk thread library
void handleEventCompletion(void *args);
int ioWorker(void *args);
int dispatchWorker(void *args);
int ecWorker(void *args);
int indexWorker(void *args);
int completionWorker(void *args);

void enqueueRequest(void *args);
void queryPba(void *args);
void updatePba(void *args);

// Used under spdk event library
void registerIoCompletionRoutine(void *arg1, void *arg2);
void registerDispatchRoutine(void *arg1, void *arg2);
void handleEventCompletion2(void *arg1, void *arg2);

void enqueueRequest2(void *arg1, void *arg2);
void queryPba2(void *arg1, void *arg2);
void updatePba2(void *arg1, void *arg2);

// For devices
void zoneWrite2(void *arg1, void *arg2);
void zoneRead2(void *arg1, void *arg2);
void zoneAppend2(void *arg, void *arg2);
void zoneReset2(void *arg, void *arg2);
void zoneFinish2(void *arg, void *arg2);

void zoneWrite(void *args);
void zoneRead(void *args);
void zoneAppend(void *args);
void zoneReset(void *args);
void zoneFinish(void *args);


#endif
