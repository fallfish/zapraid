#include "common.h"
#include "segment.h"
#include "raid_controller.h"
#include <sys/time.h>
#include <queue>

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
  std::queue<RequestContext*>& q = ctrl->GetRequestQueue();
  std::mutex& qMutex = ctrl->GetRequestQueueMutex();
  qMutex.lock();
  q.push(this);
  qMutex.unlock();
}

void RequestContext::PrintStats()
{
  printf("RequestStats: %d %d %lu %d, iocontext: %p %p %lu %d\n", type, status, lba, size, ioContext.data, ioContext.metadata, ioContext.offset, ioContext.size);
}

double timestamp()
{
  struct timeval s;
  gettimeofday(&s, NULL);
  return s.tv_sec + s.tv_usec / 1000000.0;
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
