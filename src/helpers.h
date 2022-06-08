struct ResetZoneGroupArgs {
  ZoneGroup *zoneGroup;
  bool ready;
};

void resetZoneGroup(void *args)
{
  ResetZoneGroupArgs *args1 = (ResetZoneGroupArgs*)args;
  args1->zoneGroup->Reset();
  args1->ready = true;
}

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


struct TryReadFromZoneGroupArgs {
  RequestContext *requestContext;
  PhysicalAddr phyAddr;
  uint32_t pos;
  bool ready;
  bool success;
};

void tryReadFromZoneGroup(void *args)
{
  TryReadFromZoneGroupArgs *args1 = (TryReadFromZoneGroupArgs *)args;
  args1->success = args1->phyAddr.zoneGroup->Read(args1->requestContext,
                                                args1->pos, args1->phyAddr);
  printf("ctx: %p, success: %d\n", args1->requestContext, args1->success);
  args1->ready = true;
}

struct TryAppendToZoneGroupArgs {
  ZoneGroup *zoneGroup;
  RequestContext *requestContext;
  uint32_t pos;
  bool ready;
  bool success;
};

void tryAppendToZoneGroup(void *args)
{
  TryAppendToZoneGroupArgs *args1 = (TryAppendToZoneGroupArgs *)args;
  ZoneGroup *zoneGroup = args1->zoneGroup;
  RequestContext *reqCtx = args1->requestContext;

  if (reqCtx != nullptr) {
    args1->success = zoneGroup->Append(reqCtx, args1->pos);
  } else {
    zoneGroup->FlushStripe();
  }
  printf("ctx: %p, success: %d\n", reqCtx, args1->success);
  args1->ready = true;
}

void enqueueRequest(void *args)
{
  RequestContext *ctx = (RequestContext*)args;
  ctx->ctrl->EnqueueEvent(ctx);
}
