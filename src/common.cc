#include "common.h"
#include "segment.h"
#include "raid_controller.h"
#include "poller.h"
#include <sys/time.h>
#include <queue>

void thread_send_msg(spdk_thread *thread, spdk_msg_fn fn, void *args)
{
  if (spdk_thread_send_msg(thread, fn, args) < 0) {
    printf("Thread send message failed: thread_name %s!\n", spdk_thread_get_name(thread));
    exit(-1);
  }
}

void event_call(uint32_t core_id, spdk_event_fn fn, void *arg1, void *arg2)
{
  struct spdk_event *event = spdk_event_allocate(
      core_id, fn, arg1, arg2);
  if (event == nullptr) {
    printf("Failed to allocate event: core id %u!\n", core_id);
  }
  spdk_event_call(event);
}

void PhysicalAddr::PrintOffset() {
  uint32_t deviceId = segment->GetZones()[zoneId]->GetDeviceId();
  uint32_t pba = segment->GetZones()[zoneId]->GetSlba();
  printf("phyAddr: deviceId: %u, stripeId: %u, pba: %u\n",
         deviceId, stripeId, pba + offset);
}

void RequestContext::Clear()
{
  available = true;
  successBytes = 0;
  targetBytes = 0;
  lba = 0;
  cb_fn = nullptr;
  cb_args = nullptr;
  curOffset = 0;

  timestamp = ~0ull;

  append = false;

  associatedRequest = nullptr;
  associatedStripe = nullptr;
  associatedRead = nullptr;
  ctrl = nullptr;
  segment = nullptr;

  needDegradedRead = false;
  pbaArray.clear();
}

double RequestContext::GetElapsedTime()
{
//  printf("Get elapsed time: %p %lf %lf\n", this, mStime, mCtime);
  return ctime - stime;
}

PhysicalAddr RequestContext::GetPba()
{
  PhysicalAddr addr;
  addr.segment = segment;
  addr.zoneId = zoneId;
  addr.stripeId = stripeId;
  addr.offset = offset;
  return addr;
}

void RequestContext::Queue()
{
  struct spdk_thread *th = ctrl->GetDispatchThread();
  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(ctrl->GetDispatchThread(), handleEventCompletion, this);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               handleEventCompletion2, this, nullptr);
  }
}

void RequestContext::PrintStats()
{
  printf("RequestStats: %d %d %lu %d, iocontext: %p %p %lu %d\n", type, status, lba, size, ioContext.data, ioContext.metadata, ioContext.offset, ioContext.size);
}

double timestamp()
{
  return 0;
  // struct timeval s;
  // gettimeofday(&s, NULL);
  // return s.tv_sec + s.tv_usec / 1000000.0;
}

double gettimediff(struct timeval s, struct timeval e)
{
  return e.tv_sec + e.tv_usec / 1000000. - (s.tv_sec + s.tv_usec / 1000000.);
}

void RequestContext::CopyFrom(const RequestContext &o) {
  type = o.type;
  status = o.status;

  lba = o.lba;
  size = o.size;
  req_type = o.req_type;
  data = o.data;
  meta = o.meta;
  pbaArray = o.pbaArray;
  successBytes = o.successBytes;
  targetBytes = o.targetBytes;
  curOffset = o.curOffset;
  cb_fn = o.cb_fn;
  cb_args = o.cb_args;

  available = o.available;
  
  ctrl = o.ctrl;
  segment = o.segment;
  zoneId = o.zoneId;
  stripeId = o.stripeId;
  offset = o.offset;
  append = o.append;

  stime = o.stime;
  ctime = o.ctime;

  associatedRequest = o.associatedRequest;
  associatedStripe = o.associatedStripe;
  associatedRead = o.associatedRead;

  needDegradedRead = o.needDegradedRead;
  needDegradedRead = o.needDecodeMeta;

  ioContext = o.ioContext;
  gcTask = o.gcTask;
}

// Pools
RequestContextPool::RequestContextPool(uint32_t cap) {
  capacity = cap;
  contexts = new RequestContext[capacity];
  for (uint32_t i = 0; i < capacity; ++i) {
    contexts[i].Clear();
    contexts[i].dataBuffer = (uint8_t *)spdk_zmalloc(
        Configuration::GetBlockSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    contexts[i].metadataBuffer = (uint8_t *)spdk_zmalloc(
        Configuration::GetMetadataSize(),
        Configuration::GetMetadataSize(),
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    availableContexts.emplace_back(&contexts[i]);
  }
}

RequestContext *RequestContextPool::getRequestContext(bool force) {
  RequestContext *ctx = nullptr;
  if (availableContexts.empty() && force == false) {
    ctx = nullptr;
  } else {
    if (!availableContexts.empty()) {
      ctx = availableContexts.back();
      availableContexts.pop_back();
      ctx->Clear();
      ctx->available = false;
    } else {
      ctx = new RequestContext();
      ctx->dataBuffer = (uint8_t *)spdk_zmalloc(
          Configuration::GetBlockSize(), 4096,
          NULL, SPDK_ENV_SOCKET_ID_ANY,
          SPDK_MALLOC_DMA);
      ctx->metadataBuffer = (uint8_t *)spdk_zmalloc(
          Configuration::GetMetadataSize(),
          Configuration::GetMetadataSize(),
          NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
      ctx->Clear();
      ctx->available = false;
      printf("Request Context runs out.\n");
    }
  }
  return ctx;
}

void RequestContextPool::returnRequestContext(RequestContext *slot) {
  assert(slot->available);
  if (slot < contexts || slot >= contexts + capacity) {
  // test whether the returned slot is pre-allocated
    spdk_free(slot->dataBuffer);
    spdk_free(slot->metadataBuffer);
    delete slot;
  } else {
    assert(availableContexts.size() <= capacity);
    availableContexts.emplace_back(slot);
  }
}

ReadContextPool::ReadContextPool(uint32_t cap, RequestContextPool *rp) {
  capacity = cap;
  requestPool = rp;

  contexts = new ReadContext[capacity];
  for (uint32_t i = 0; i < capacity; ++i) {
    contexts[i].data = new uint8_t *[Configuration::GetStripeSize() /
                                      Configuration::GetBlockSize()];
    contexts[i].metadata = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    contexts[i].ioContext.clear();
    availableContexts.emplace_back(&contexts[i]);
  }
}

ReadContext* ReadContext::GetContext() {
  while (availableContexts.empty()) {
    Recycle();
  }

  ReadContext *context = availableContexts.back();
  inflightContexts.emplace_back(context);
  availableContexts.pop_back();

  return context;
}

void ReadContext::Recycle() {
  for (auto it = inflightContexts.begin();
      it != inflightContexts.end();
      ) {
    if (checkReadAvailable(*it)) {
      availableContexts.emplace_back(*it);
      it = inflightContexts.erase(it);
    } else {
      ++it;
    }
  }
}

bool ReadContext::checkReadAvailable(ReadContext *readContext)
{
  bool isAvailable = true;
  for (auto ctx : readContext->ioContext) {
    if (!ctx->available) {
      isAvailable = false;
    }
  }
  if (isAvailable) {
    for (auto ctx : readContext->ioContext) {
      requestPool->returnRequestContext(ctx);
    }
    readContext->ioContext.clear();
  }
  return isAvailable;
}

StripeWriteContextPool::StripeWriteContextPool(uint32_t cap, struct RequestContextPool *rp) {
  capacity = cap;
  rPool = rp;

  contexts = new StripeWriteContext[capacity];
  availableContexts.clear();
  inflightContexts.clear();
  for (int i = 0; i < capacity; ++i) {
    contexts[i].data = new uint8_t *[Configuration::GetStripeSize() /
                                      Configuration::GetBlockSize()];
    contexts[i].metadata = new uint8_t *[Configuration::GetStripeSize() /
                                      Configuration::GetBlockSize()];
    contexts[i].ioContext.clear();
    availableContexts.emplace_back(&contexts[i]);
  }
}

StripeWriteContext* StripeWriteContext::GetContext() {
  if (availableContexts.empty()) {
    Recycle();
    if (availableContexts.empty()) {
      return nullptr;
    }
  }

  StripeWriteContext *stripe = availableContexts.back();
  inflightContexts.emplace_back(stripe);
  availableContexts.pop_back();

  return stripe;
}

void StripeWriteContext::Recycle() {
  for (auto it = inflightContexts.begin();
      it != inflightContexts.end();
      ) {
    if (checkStripeAvailable(*it)) {
      assert((*it)->ioContext.empty());
      availableContexts.emplace_back(*it);
      it = inflightContexts.erase(it);
    } else {
      ++it;
    }
  }
}

bool StripeWriteContext::NoInflightStripes() {
  return inflightContexts.empty();
}

bool StripeWriteContext::checkStripeAvailable(StripeWriteContext *stripe) {
  bool isAvailable = true;

  for (auto slot : stripe->ioContext) {
    isAvailable = slot && slot->available ? isAvailable : false;
  }

  if (isAvailable) {
    for (auto slot : stripe->ioContext) {
      rPool->returnRequestContext(slot);
    }
    stripe->ioContext.clear();
  }

  return isAvailable;
}