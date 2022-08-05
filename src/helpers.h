#ifndef __NAMELESS_RAID_HELPER__
#define __NAMELESS_RAID_HELPER__
struct DrainArgs {
  RAIDController *ctrl;
  bool success;
  bool ready;
};

void tryDrainController(void *args)
{
  DrainArgs *drainArgs = (DrainArgs *)args;
  drainArgs->ctrl->ReclaimContexts();
  drainArgs->ctrl->ProceedGc();
  drainArgs->success = drainArgs->ctrl->GetNumInflightRequests() == 0 && !drainArgs->ctrl->ExistsGc();

  drainArgs->ready = true;
}


struct TryReadFromSegmentArgs {
  RequestContext *requestContext;
  PhysicalAddr phyAddr;
  uint32_t pos;
  bool ready;
  bool success;
};

void tryReadFromSegment(void *args)
{
  TryReadFromSegmentArgs *args1 = (TryReadFromSegmentArgs *)args;
  args1->success = args1->phyAddr.segment->Read(args1->requestContext,
                                                args1->pos, args1->phyAddr);
  printf("ctx: %p, success: %d\n", args1->requestContext, args1->success);
  args1->ready = true;
}

struct TryAppendToSegmentArgs {
  Segment *segment;
  RequestContext *requestContext;
  uint32_t pos;
  bool ready;
  bool success;
};

void tryAppendToSegment(void *args)
{
  TryAppendToSegmentArgs *args1 = (TryAppendToSegmentArgs *)args;
  Segment *segment = args1->segment;
  RequestContext *reqCtx = args1->requestContext;

  if (reqCtx != nullptr) {
    args1->success = segment->Append(reqCtx, args1->pos);
  } else {
    segment->FlushStripe();
  }
  printf("ctx: %p, success: %d\n", reqCtx, args1->success);
  args1->ready = true;
}

void enqueueRequest(void *args)
{
  RequestContext *ctx = (RequestContext*)args;
  if (ctx->req_type == 'W') {
    ctx->ctrl->WriteInDispatchThread(ctx);
  } else {
    ctx->ctrl->ReadInDispatchThread(ctx);
  }
}

struct QueryPbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

void queryPba(void *args) {
  QueryPbaArgs *qArgs = (QueryPbaArgs*)args;
  RAIDController *ctrl = qArgs->ctrl;
  RequestContext *ctx = qArgs->ctx;
  free(qArgs);

  for (uint32_t i = 0; i < ctx->size / Configuration::GetBlockSize(); ++i) {
    PhysicalAddr phyAddr;
    uint64_t lba = ctx->lba + i * Configuration::GetBlockSize();
    if (ctrl->LookupIndex(lba, &phyAddr)) {
      ctx->pbaArray[i] = phyAddr;
    } else {
      ctx->pbaArray[i].segment = nullptr;
    }
  }
  ctx->status = READ_INDEX_READY;
  if (spdk_thread_send_msg(ctrl->GetDispatchThread(), enqueueRequest, ctx) < 0) {
    printf("Failed!\n");
    exit(-1);
  }
}
#endif // NAMELESS_RAID_HELPER
