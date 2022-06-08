#include "poller.h"
#include <vector>
#include <queue>
#include "common.h"
#include "device.h"
#include "zone_group.h"
#include "spdk/thread.h"
#include "raid_controller.h"
#include <sys/time.h>



static void dummy_disconnect_handler(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

int handleIoCompletions(void *args)
{
//  printf("Function polling\n");
  struct spdk_nvme_poll_group* pollGroup = (struct spdk_nvme_poll_group*)args;
  struct timeval s, e;
  gettimeofday(&s, NULL);
  int r = spdk_nvme_poll_group_process_completions(pollGroup, 0, dummy_disconnect_handler);
  gettimeofday(&e, NULL);
  return r > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int ioWorker(void *args)
{
  IoThread *ioThread = (IoThread*)args;
  struct spdk_thread *thread = ioThread->thread;
  spdk_set_thread(thread);
  spdk_poller_register(handleIoCompletions, ioThread->group, 0);
  while (true) {
    struct timeval s, e;
    gettimeofday(&s, NULL);
    spdk_thread_poll(thread, 0, 0);
    gettimeofday(&e, NULL);
    double elapsed = e.tv_sec + e.tv_usec / 1000000. - (s.tv_sec + s.tv_usec / 1000000.);
  }
}

static bool contextReady(RequestContext *ctx)
{
  return ctx->successBytes == ctx->targetBytes;
}

static void handleUserContext(RequestContext *context, std::vector<RequestContext*> &needCheck)
{
  ContextStatus &status = context->status;
  assert(contextReady(context));
  if (status == WRITE_REAPING) {
    for (uint32_t i = 0; i < context->size / Configuration::GetBlockSize(); ++i) {
      context->ctrl->UpdateIndex(context->lba + i * Configuration::GetBlockSize(), context->pbaArray[i]);
    }
    status = WRITE_COMPLETE;
  } else if (status == READ_REAPING) {
    status = READ_COMPLETE;
  } else {
    assert(0);
  }
  if (context->cb_fn != nullptr) {
    context->cb_fn(context->cb_args);
  }
//  printf("Context %p set available!\n", context);
  context->available = true;
}

void handleGcContext(RequestContext *context, std::vector<RequestContext*> &needCheck)
{
  ContextStatus &status = context->status;
  if (status == WRITE_REAPING) {
    status = WRITE_COMPLETE;
  } else if (status == READ_REAPING) {
    status = READ_COMPLETE;
  } else {
    assert(0);
  }
  context->available = true;
}

static void handleStripeUnitContext(RequestContext *context, std::vector<RequestContext*> &needCheck)
{
  context->zoneGroup->Assert();
  ContextStatus &status = context->status;
  switch (status) {
    case WRITE_REAPING:
      if (context->successBytes == context->targetBytes) {
        if (context->append) {
          context->zoneGroup->UpdateSyncPoint(context->zoneId,
              context->stripeId,
              context->offset);
        }

        if (context->associatedStripe) {
          context->associatedStripe->successBytes += context->targetBytes;
          if (context->associatedStripe->successBytes == context->associatedStripe->targetBytes) {
            // stripe finish, notify the corresponding user request
            for (auto contextInStripe : context->associatedStripe->ioContext) {
              RequestContext *parent = contextInStripe->associatedRequest;
              if (parent) {
                parent->pbaArray[(contextInStripe->lba - parent->lba) / Configuration::GetBlockSize()] = contextInStripe->GetPba();
                parent->successBytes += contextInStripe->targetBytes;
                if (contextReady(parent)) {
                  needCheck.emplace_back(parent);
//                  printf("Context %p bring back its parent %p!\n", context, parent);
                }
                assert(parent->successBytes <= parent->targetBytes);
              }
            }
          }
        }

        context->zoneGroup->WriteComplete(context);
        status = WRITE_COMPLETE;
        context->available = true;
      }
      break;
    case READ_REAPING:
    case DEGRADED_READ_REAPING:
      if (context->needDegradedRead) {
        assert(status != READ_REAPING);
        context->needDegradedRead = false;
        SystemMode mode = Configuration::GetSystemMode();
        if (mode == ZONE_APPEND_WITH_META || mode == ZONE_APPEND_WITH_REDIRECTION) {
          status = DEGRADED_READ_META;
          context->zoneGroup->ReadStripeMeta(context);
        } else {
          status = DEGRADED_READ_REAPING;
          context->zoneGroup->ReadStripe(context);
        }
      } else if (context->successBytes == context->targetBytes) {
        context->zoneGroup->ReadComplete(context);
        context->associatedRequest->successBytes += context->targetBytes;
        if (contextReady(context->associatedRequest)) {
          needCheck.emplace_back(context->associatedRequest);
//          printf("Context %p bring back its parent %p!\n", context, context->associatedRequest);
        }
        assert(context->associatedRequest->successBytes <= context->associatedRequest->targetBytes);

        status = READ_COMPLETE;
        context->available = true;
      }
      break;
    case DEGRADED_READ_SUB:
      assert(context->associatedRequest);
      context->associatedRequest->successBytes += context->targetBytes;
      if (contextReady(context->associatedRequest)) {
        needCheck.emplace_back(context->associatedRequest);
      }
      break;
    case DEGRADED_READ_META:
      if (contextReady(context)) {
        status = DEGRADED_READ_REAPING;
        context->zoneGroup->ReadStripe(context);
      }
      break;
    default:
      printf("Error in context handling!\n");
      exit(-1);
  }
  context->zoneGroup->Assert();
}

void handleContext(RequestContext *context, std::vector<RequestContext*> &needCheck)
{
  ContextType type = context->type;
  if (type == USER) {
    handleUserContext(context, needCheck);
  } else if (type == STRIPE_UNIT) {
    handleStripeUnitContext(context, needCheck);
  } else if (type == GC) {
    handleGcContext(context, needCheck);
  }
}

int handleEventsCompletion(void *args)
{
  RAIDController *ctrl = (RAIDController*)args;
  std::queue<RequestContext*>& q = ctrl->GetRequestQueue();
  std::mutex& qMutex = ctrl->GetRequestQueueMutex();
  int r = 0;

  qMutex.lock();
  std::vector<RequestContext *> needCheck, needEnqueue;
  needCheck.clear();
  needEnqueue.clear();
  while (!q.empty()) {
    RequestContext *req = q.front();
    needCheck.emplace_back(req);
    q.pop();

    ++r;
  }
  qMutex.unlock();

  needEnqueue.clear();
  for (RequestContext *req : needCheck) {
    std::vector<RequestContext*> needEnqueueTmp;
    needEnqueueTmp.clear();
    assert(req);
    handleContext(req, needEnqueueTmp);
    for (auto req1 : needEnqueueTmp) {
      assert(req1);
    }
    needEnqueue.insert(needEnqueue.end(), needEnqueueTmp.begin(), needEnqueueTmp.end());
  }

  qMutex.lock();
  for (RequestContext *req : needEnqueue) {
    q.push(req);
  }
  qMutex.unlock();

  return r > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int handleEventsDispatch(void *args)
{
  RAIDController *ctrl = (RAIDController*)args;
  std::vector<RequestContext*> eventsToDispatch;
  std::swap(eventsToDispatch, ctrl->GetEventsToDispatch());

  for (RequestContext *ctx : eventsToDispatch) {
    if (ctx->req_type == 'W') {
      ctrl->WriteInDispatchThread(ctx);
    } else {
      ctrl->ReadInDispatchThread(ctx);
    }
  }

  return eventsToDispatch.size() > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int scheduleGc(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  bool hasProgress = raidController->ProceedGc();

  return hasProgress ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int dispatchWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetDispatchThread();
  spdk_set_thread(thread);
  spdk_poller_register(handleEventsCompletion, raidController, 0);
  spdk_poller_register(handleEventsDispatch, raidController, 0);
//  spdk_poller_register(scheduleGc, raidController, 0);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}
