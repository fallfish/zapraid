#include "messages_and_functions.h"
#include <vector>
#include <queue>
#include "common.h"
#include "device.h"
#include "segment.h"
#include "spdk/thread.h"
#include "raid_controller.h"
#include <sys/time.h>

void handleContext(RequestContext *context);

void updatePba2(void *arg1, void *arg2)
{
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg2);
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t numBlocks = ctx->size / Configuration::GetBlockSize();
  for (uint32_t i = 0; i < numBlocks; ++i) {
    uint64_t lba = ctx->lba + i * blockSize;
    ctrl->UpdateIndex(lba, ctx->pbaArray[i]);
  }
  ctx->status = WRITE_COMPLETE;
  if (ctx->cb_fn != nullptr) {
    ctx->cb_fn(ctx->cb_args);
  }
  ctx->available = true;
}

void updatePba(void *args) {
  UpdatePbaArgs *qArgs = reinterpret_cast<UpdatePbaArgs*>(args);
  RAIDController *ctrl = qArgs->ctrl;
  RequestContext *ctx = qArgs->ctx;
  free(qArgs);

  assert(ctx->status == WRITE_INDEX_UPDATING);
  updatePba2(ctrl, ctx);
}

static void dummy_disconnect_handler(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

int handleIoCompletions(void *args)
{
  struct spdk_nvme_poll_group* pollGroup = (struct spdk_nvme_poll_group*)args;
  int r = 0;
  r = spdk_nvme_poll_group_process_completions(pollGroup, 0, dummy_disconnect_handler);
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
  RAIDController *ctrl = context->ctrl;
  assert(contextReady(context));
  if (status == WRITE_REAPING) {
    status = WRITE_INDEX_UPDATING;
    if (!Configuration::GetEventFrameworkEnabled()) {
      UpdatePbaArgs *args = (UpdatePbaArgs*)calloc(1, sizeof(UpdatePbaArgs));
      args->ctrl = ctrl;
      args->ctx = context;
      thread_send_msg(ctrl->GetIndexThread(), updatePba, args);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 updatePba2, ctrl, context);
    }
  } else if (status == READ_REAPING) {
    ctrl->RemoveRequestFromGcEpochIfNecessary(context);
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
        StripeWriteContext *stripe = context->associatedStripe;
        if (stripe != nullptr) {
          stripe->successBytes += context->successBytes;
          if (stripe->successBytes != stripe->targetBytes) {
            break;
          }

          // Acknowledge the completion only after the whole stripe persists
          for (auto context_ : stripe->ioContext) {
            RequestContext *parent = context_->associatedRequest;
            if (parent) {
              parent->pbaArray[(context_->lba - parent->lba) / Configuration::GetBlockSize()] = context_->GetPba();
              parent->successBytes += context_->targetBytes;
              assert(parent->successBytes <= parent->targetBytes);
              if (contextReady(parent)) {
                handleContext(parent);
              }
            }
            context_->segment->WriteComplete(context_);
            status = WRITE_COMPLETE;
            context_->available = true;
          }
        }
      }
      break;
    case READ_REAPING:
    case DEGRADED_READ_REAPING:
      if (context->needDegradedRead) {
        context->needDegradedRead = false;
        status = DEGRADED_READ_REAPING;
        context->segment->ReadStripe(context);
      } else if (context->successBytes == context->targetBytes) {
        context->segment->ReadComplete(context);
        context->associatedRequest->successBytes += Configuration::GetBlockSize();
        assert(context->associatedRequest->successBytes <= context->associatedRequest->targetBytes);
        if (contextReady(context->associatedRequest)) {
          handleContext(context->associatedRequest);
        }

        status = READ_COMPLETE;
        context->available = true;
        context->segment->ReclaimReadContext(context->associatedRead);
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
      status = RESET_COMPLETE;
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
  ContextType type = context->type;
  if (type == USER) {
    handleUserContext(context);
  } else if (type == STRIPE_UNIT) {
    handleStripeUnitContext(context);
  } else if (type == GC) {
    handleGcContext(context);
  }
}

void handleEventCompletion2(void *arg1, void *arg2)
{
  RequestContext *slot = (RequestContext*)arg1;
  handleContext(slot);
}

void handleEventCompletion(void *args)
{
  handleEventCompletion2(args, nullptr);
}

int handleEventsDispatch(void *args)
{
  bool busy = false;
  RAIDController *ctrl = (RAIDController*)args;

  uint32_t count = 0;
  std::queue<RequestContext*>& writeQ = ctrl->GetWriteQueue();
  while (!writeQ.empty()) {
    RequestContext *ctx = writeQ.front();
    ctrl->WriteInDispatchThread(ctx);

    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      writeQ.pop();
    } else {
      break;
    }
  }

  count = 0;
  std::queue<RequestContext*>& readPrepareQ = ctrl->GetReadPrepareQueue();
  while (!readPrepareQ.empty()) {
    RequestContext *ctx = readPrepareQ.front();
    ctrl->ReadInDispatchThread(ctx);
    readPrepareQ.pop();
    busy = true;
  }

  count = 0;
  std::queue<RequestContext*>& readReapingQ = ctrl->GetReadReapingQueue();
  while (!readReapingQ.empty()) {
    RequestContext *ctx = readReapingQ.front();
    ctrl->ReadInDispatchThread(ctx);

    uint32_t recordedOffset = ctx->curOffset;
    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      readReapingQ.pop();
    } else {
      break;
    }
  }

  return busy ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int handleBackgroundTasks(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  bool hasProgress = false;
  hasProgress |= raidController->ProceedGc();
  hasProgress |= raidController->CheckSegments();

  return hasProgress ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int dispatchWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetDispatchThread();
  spdk_set_thread(thread);
  spdk_poller_register(handleEventsDispatch, raidController, 1);
  spdk_poller_register(handleBackgroundTasks, raidController, 1);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int ecWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetEcThread();
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

void executeRequest(void *arg1, void *arg2)
{
  Request *req = reinterpret_cast<Request*>(arg1);
  req->controller->Execute(
    req->offset, req->size, req->data,
    req->type == 'W', req->cb_fn, req->cb_args);
  free(req);
}

void registerIoCompletionRoutine(void *arg1, void *arg2)
{
  IoThread *ioThread = (IoThread*)arg1;
  spdk_poller_register(handleIoCompletions, ioThread->group, 0);
}

void registerDispatchRoutine(void *arg1, void *arg2)
{
  RAIDController *raidController = reinterpret_cast<RAIDController*>(arg1);
  spdk_poller_register(handleEventsDispatch, raidController, 1);
  spdk_poller_register(handleBackgroundTasks, raidController, 1);
}

void enqueueRequest2(void *arg1, void *arg2)
{
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg1);
  if (ctx->req_type == 'W') {
    ctx->ctrl->EnqueueWrite(ctx);
  } else {
    if (ctx->status == READ_PREPARE) {
      ctx->ctrl->EnqueueReadPrepare(ctx);
    } else if (ctx->status == READ_REAPING) {
      ctx->ctrl->EnqueueReadReaping(ctx);
    }
  }
}

void enqueueRequest(void *args)
{
  enqueueRequest2(args, nullptr);
}

void queryPba2(void *arg1, void *arg2)
{
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg2);
  for (uint32_t i = 0; i < ctx->size / Configuration::GetBlockSize(); ++i) {
    PhysicalAddr phyAddr;
    uint64_t lba = ctx->lba + i * Configuration::GetBlockSize();
    if (ctrl->LookupIndex(lba, &phyAddr)) {
      ctx->pbaArray[i] = phyAddr;
    } else {
      ctx->pbaArray[i].segment = nullptr;
    }
  }
  ctx->status = READ_REAPING;

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(ctrl->GetDispatchThread(), enqueueRequest, ctx);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               enqueueRequest2, ctx, nullptr);
  }
}

void queryPba(void *args)
{
  QueryPbaArgs *qArgs = (QueryPbaArgs*)args;
  RAIDController *ctrl = qArgs->ctrl;
  RequestContext *ctx = qArgs->ctx;
  free(qArgs);

  queryPba2(ctrl, ctx);
}

void zoneWrite2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = 0;
  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_ns_cmd_write_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_ns_cmd_write(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device write error!\n");
    printf("%d %ld %d %s\n", rc, ioCtx.offset, errno, strerror(errno));
  }
  assert(rc == 0);
}

void zoneWrite(void *args)
{
  zoneWrite2(args, nullptr);
}

void zoneRead2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = 0;
  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_ns_cmd_read_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_ns_cmd_read(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device read error!\n");
    printf("%d %lu %d %s\n", rc, ioCtx.offset, errno, strerror(errno));
  }
  assert(rc == 0);
}

void zoneRead(void *args)
{
  zoneRead2(args, nullptr);
}

void zoneAppend2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = 0;
  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_zns_zone_append_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_zns_zone_append(ioCtx.ns, ioCtx.qpair,
                                      ioCtx.data, ioCtx.offset, ioCtx.size,
                                      ioCtx.cb, ioCtx.ctx,
                                      ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device append error!\n");
  }
  assert(rc == 0);
}

void zoneAppend(void *args)
{
  zoneAppend2(args, nullptr);
}

void zoneReset2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();
  int rc = spdk_nvme_zns_reset_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device reset error!\n");
  }
  assert(rc == 0);
}

void zoneReset(void *args)
{
  zoneReset2(args, nullptr);
}

void zoneFinish2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = spdk_nvme_zns_finish_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device close error!\n");
  }
  assert(rc == 0);
}

void zoneFinish(void *args)
{
  zoneFinish2(args, nullptr);
}

void tryDrainController(void *args)
{
  DrainArgs *drainArgs = (DrainArgs *)args;
  drainArgs->ctrl->CheckSegments();
  drainArgs->ctrl->ReclaimContexts();
  drainArgs->ctrl->ProceedGc();
  drainArgs->success = drainArgs->ctrl->GetNumInflightRequests() == 0 && !drainArgs->ctrl->ExistsGc();

  drainArgs->ready = true;
}

void progressGcIndexUpdate2(void *arg1, void *arg2)
{
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  GcTask *task = ctrl->GetGcTask();
  std::vector<uint64_t> lbas;
  std::vector<std::pair<PhysicalAddr, PhysicalAddr>> pbas;

  auto it = task->mappings.begin();
  uint32_t count = 0;
  while (it != task->mappings.end()) {
    lbas.emplace_back(it->first);
    pbas.emplace_back(it->second);

    it = task->mappings.erase(it);

    count += 1;
    if (count == 256) break;
  }
  ctrl->GcBatchUpdateIndex(lbas, pbas);
  task->stage = INDEX_UPDATING_BATCH;
}

void progressGcIndexUpdate(void *args)
{
  progressGcIndexUpdate2(args, nullptr);
}

