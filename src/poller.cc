#include "poller.h"
#include <vector>
#include <queue>
#include "common.h"
#include "device.h"
#include "segment.h"
#include "spdk/thread.h"
#include "raid_controller.h"
#include <sys/time.h>

struct UpdatePbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

void updatePba(void *args) {
  UpdatePbaArgs *qArgs = (UpdatePbaArgs*)args;
  RequestContext *ctx = qArgs->ctx;
  assert(ctx->status == WRITE_INDEX_UPDATING);
  for (uint32_t i = 0; i < ctx->size / Configuration::GetBlockSize(); ++i) {
    uint64_t lba = ctx->lba + i * Configuration::GetBlockSize();
    qArgs->ctrl->UpdateIndex(lba, ctx->pbaArray[i]);
  }
  free(qArgs);
  ctx->status = WRITE_COMPLETE;
  if (ctx->cb_fn != nullptr) {
    ctx->cb_fn(ctx->cb_args);
  }
  ctx->available = true;
  // ctx->Queue();
}

static void dummy_disconnect_handler(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

int handleIoCompletions(void *args)
{
  struct spdk_nvme_poll_group* pollGroup = (struct spdk_nvme_poll_group*)args;
  int r = spdk_nvme_poll_group_process_completions(pollGroup, 0, dummy_disconnect_handler);
  return r > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int ioWorker(void *args)
{
  IoThread *ioThread = (IoThread*)args;
  struct spdk_thread *thread = ioThread->thread;
  spdk_set_thread(thread);
  spdk_poller_register(handleIoCompletions, ioThread->group, 0);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

static bool contextReady(RequestContext *ctx)
{
  return ctx->successBytes == ctx->targetBytes;
}

static void handleUserContext(RequestContext *context)
{
  ContextStatus &status = context->status;
  assert(contextReady(context));
  if (status == WRITE_REAPING) {
    UpdatePbaArgs *args = (UpdatePbaArgs*)calloc(1, sizeof(UpdatePbaArgs));
    args->ctrl = context->ctrl;
    args->ctx = context;
    status = WRITE_INDEX_UPDATING;
    if (spdk_thread_send_msg(args->ctrl->GetIndexThread(), updatePba, args) < 0) {
      printf("Failed!\n");
      exit(-1);
    }
  } else if (status == READ_REAPING) {
    status = READ_COMPLETE;
    if (context->cb_fn != nullptr) {
      context->cb_fn(context->cb_args);
    }
    context->available = true;
  }
}

void handleGcContext(RequestContext *context)
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

void handleIndexContext(RequestContext *context)
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


static void handleStripeUnitContext(RequestContext *context)
{
  static double part1 = 0, part2 = 0, part3 = 0;
  static int count = 0;
  struct timeval s, e;

  ContextStatus &status = context->status;
  switch (status) {
    case WRITE_REAPING:
      if (context->successBytes == context->targetBytes) {
        if (context->append) {
          context->segment->UpdateNamedMetadata(context->zoneId,
              context->stripeId,
              context->offset);
        }
        RequestContext *parent = context->associatedRequest;
        if (parent) {
          parent->pbaArray[(context->lba - parent->lba) / Configuration::GetBlockSize()] = context->GetPba();
          parent->successBytes += context->targetBytes;
          if (contextReady(parent)) {
            handleUserContext(parent);
          }
          assert(parent->successBytes <= parent->targetBytes);
        }
        context->segment->WriteComplete(context);
        status = WRITE_COMPLETE;
        context->available = true;
      }
      break;
    case READ_REAPING:
    case DEGRADED_READ_REAPING:
      if (context->needDegradedRead) {
        context->needDegradedRead = false;
        SystemMode mode = Configuration::GetSystemMode();
        if (mode == NAMED_META || mode == REDIRECTION) {
          status = DEGRADED_READ_META;
          context->segment->ReadStripeMeta(context);
        } else {
          status = DEGRADED_READ_REAPING;
          context->segment->ReadStripe(context);
        }
      } else if (context->successBytes == context->targetBytes) {
        context->segment->ReadComplete(context);
        context->associatedRequest->successBytes += Configuration::GetBlockSize();
        if (contextReady(context->associatedRequest)) {
          handleUserContext(context->associatedRequest);
        }
        assert(context->associatedRequest->successBytes <= context->associatedRequest->targetBytes);

        status = READ_COMPLETE;
        context->available = true;
      }
      break;
    case DEGRADED_READ_SUB:
      assert(context->associatedRequest);
      context->associatedRequest->successBytes += context->targetBytes;
      context->segment->ReadComplete(context);
      
      if (contextReady(context->associatedRequest)) {
        handleStripeUnitContext(context->associatedRequest);
      }
      break;
    case DEGRADED_READ_META:
      if (contextReady(context)) {
        status = DEGRADED_READ_REAPING;
        context->segment->ReadStripe(context);
      }
      break;
    case RESET_REAPING:
      context->associatedRequest->targetBytes += 1;
      status = RESET_COMPLETE;
      if (contextReady(context->associatedRequest)) {
        handleUserContext(context->associatedRequest);
      }
      context->available = true;
      break;
    case FINISH_REAPING:
      status = FINISH_COMPLETE;
      context->available = true;
      break;
    default:
      printf("Error in context handling!\n");
      assert(0);
      exit(-1);
  }
}

void handleContext(RequestContext *context)
{
//  if (context->associatedRequest != nullptr) {
//    RequestContext *ctx = context->associatedRequest;
//    struct timeval b;
//    gettimeofday(&b, NULL);
//    double diff = gettimediff(ctx->timeA, b);
//    if (diff > 0.000010)
//    {
//      printf("Time exceed %p, PointB: %f\n", ctx, diff);
//    }
//  }
  ContextType type = context->type;
  if (type == USER) {
    handleUserContext(context);
  } else if (type == STRIPE_UNIT) {
    handleStripeUnitContext(context);
  } else if (type == GC) {
    handleGcContext(context);
  } else if (type == INDEX) {
    handleIndexContext(context);
  }
}

int handleEventsCompletion(void *args)
{
  RAIDController *ctrl = (RAIDController*)args;
  std::queue<RequestContext*>& q = ctrl->GetRequestQueue();
  std::mutex& qMutex = ctrl->GetRequestQueueMutex();
  int r = 0;

  qMutex.lock();
  std::queue<RequestContext *> needCheck;
  std::swap(needCheck, q);
  qMutex.unlock();

  while (!needCheck.empty()) {
    RequestContext *c = needCheck.front();
    needCheck.pop();
    handleContext(c);
  }

  return r > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}


void handleEventsCompletionsOneEvent(void *args)
{
  RequestContext *slot = (RequestContext*)args;
  handleContext(slot);
}

int handleEventsDispatch(void *args)
{
  RAIDController *ctrl = (RAIDController*)args;
  std::vector<RequestContext*> eventsToDispatch;
  if (ctrl->GetEventsToDispatch().size() != 0) {
    std::swap(eventsToDispatch, ctrl->GetEventsToDispatch());

    for (RequestContext *ctx : eventsToDispatch) {
      if (ctx->req_type == 'W') {
        ctrl->WriteInDispatchThread(ctx);
      } else {
        ctrl->ReadInDispatchThread(ctx);
      }
    }
  }

  return eventsToDispatch.size() > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int handleBackgroundTasks(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  bool hasProgress = false;
//  bool hasProgress = raidController->ProceedGc();
  hasProgress |= raidController->CheckSegments();

  return hasProgress ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int dispatchWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetDispatchThread();
  spdk_set_thread(thread);
  spdk_poller_register(handleEventsDispatch, raidController, 0);
  spdk_poller_register(handleBackgroundTasks, raidController, 1);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int ecWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetEcThread();
  printf("Ec: %p\n", thread);
  spdk_set_thread(thread);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int indexWorker(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetIndexThread();
  printf("Index: %p\n", thread);
  spdk_set_thread(thread);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int completionWorker(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetCompletionThread();
  spdk_set_thread(thread);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}
