#include "raid_controller.h"
#include "zone.h"

#include <sys/time.h>
#include <algorithm>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/rpc.h>
#include <spdk/event.h>
#include <spdk/init.h>
#include <spdk/string.h>
#include <isa-l.h>
#include <rte_mempool.h>
#include <rte_errno.h>
#include <thread>

#include "poller.h"

static void busyWait(bool *ready)
{
  while (!*ready) {
    if (spdk_get_thread() == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(0));
    }
  }
}

static std::vector<Device*> g_devices;

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {
  for (int nsid = 1; nsid <= 1; nsid++) {
    Device* device = new Device();
    device->Init(ctrlr, nsid);
    g_devices.emplace_back(device);
  }

  return;
};

static auto quit(void *args)
{
  exit(0);
}

void RAIDController::initEcThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetEcThreadCoreId(), true);
  mEcThread = spdk_thread_create("ECThread", &cpumask);
  printf("Create EC processing thread %s %d\n", spdk_thread_get_name(mEcThread), spdk_thread_get_id(mEcThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetEcThreadCoreId(), ecWorker, this);
  if (rc < 0) {
    printf("Failed to launch ec thread error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initIndexThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetIndexThreadCoreId(), true);
  mIndexThread = spdk_thread_create("IndexThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mIndexThread),
         spdk_thread_get_id(mIndexThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetIndexThreadCoreId(), indexWorker, this);
  if (rc < 0) {
    printf("Failed to launch index completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initCompletionThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetCompletionThreadCoreId(), true);
  mCompletionThread = spdk_thread_create("CompletionThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mCompletionThread),
         spdk_thread_get_id(mCompletionThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetCompletionThreadCoreId(), completionWorker, this);
  if (rc < 0) {
    printf("Failed to launch completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initDispatchThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetDispatchThreadCoreId(), true);
  mDispatchThread = spdk_thread_create("DispatchThread", &cpumask);
  printf("Create dispatch thread %s %lu\n",
         spdk_thread_get_name(mDispatchThread),
         spdk_thread_get_id(mDispatchThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetDispatchThreadCoreId(), dispatchWorker, this);
  if (rc < 0) {
    printf("Failed to launch dispatch thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
  }
}

void RAIDController::initIoThread()
{
  struct spdk_cpuset cpumask;
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_set_cpu(&cpumask, Configuration::GetIoThreadCoreId(threadId), true);
    mIoThread[threadId].thread = spdk_thread_create("IoThread", &cpumask);
    assert(mIoThread[threadId].thread != nullptr);
    mIoThread[threadId].controller = this;
    int rc = spdk_env_thread_launch_pinned(Configuration::GetIoThreadCoreId(threadId), ioWorker, &mIoThread[threadId]);
    printf("ZNS_RAID io thread %s %lu\n", spdk_thread_get_name(mIoThread[threadId].thread), spdk_thread_get_id(mIoThread[threadId].thread));
    if (rc < 0) {
      printf("Failed to launch IO thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
    }
  }
}

void RAIDController::Init(bool need_env)
{
  int ret = 0;
  if (need_env) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.core_mask = "0xfff";
    if (spdk_env_init(&opts) < 0) {
      fprintf(stderr, "Unable to initialize SPDK env.\n");
      exit(-1);
    }

    ret = spdk_thread_lib_init(nullptr, 0);
    if (ret < 0) {
      fprintf(stderr, "Unable to initialize SPDK thread lib.\n");
      exit(-1);
    }
  }

  printf("Probe devices\n");
  ret = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL); 
  if (ret < 0) {
    fprintf(stderr, "Unable to probe devices\n");
    exit(-1);
  }

  // init devices
  mDevices = g_devices;
  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    mDevices[i]->SetDeviceId(i);
    mDevices[i]->InitZones();
    mNumTotalZones += mDevices[i]->GetNumZones();
  }

  // Preallocate contexts for user requests
  mRequestContextPoolForUserRequests = new RequestContextPool(128);
  mRequestContextPoolForSegments = new RequestContextPool(4096);

  mReadContextPool = new ReadContextPool(128, mRequestContextPoolForSegments);

  // Initialize address map
  mAddressMap = new std::unordered_map<uint64_t, PhysicalAddr>();

  // Create poll groups for the io threads and perform initialization
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    mIoThread[threadId].group = spdk_nvme_poll_group_create(NULL, NULL);
  }
  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    struct spdk_nvme_qpair** ioQueues = mDevices[i]->GetIoQueues();
    for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
      spdk_nvme_ctrlr_disconnect_io_qpair(ioQueues[threadId]);
      int rc = spdk_nvme_poll_group_add(mIoThread[threadId].group, ioQueues[threadId]);
      assert(rc == 0);
    }
    mDevices[i]->ConnectIoPairs();
  }

  if (Configuration::GetIsBrandNew()) {
    for (uint32_t i = 0; i < mDevices.size(); ++i) {
      mDevices[i]->EraseWholeDevice();
    }
  } else {
    // Mount from an existing new array
    // Reconstruct existing segments
//    std::map<uint64_t, std::pair<uint32_t, uint8_t*>> zonesAndHeaders;
//    for (uint32_t i = 0; i < mDevices.size(); ++i) {
//      auto zonesAndHeadersDevice = mDevices[i]->ReadZoneHeaders();
//      zonesAndHeaders.insert(zonesAndHeadersDevice.begin(), zonesAndHeadersDevice.end());
//    }
//
//    std::map<uint32_t, std::vector<uint32_t, SegmentMetadata*>> potentialSegments; // Segment ID to SegmentMetadata
//    for (auto zoneAndHeader : zonesAndHeaders) {
//      uint64_t zslba = zoneAndHeader.first;
//      uint32_t wp = zoneAndHeader.second.first;
//      SegmentMetadata *segMeta = (SegmentMetadata*)zoneAndHeader.second.second;
//      potentialSegments[segMeta->segmentId].emplace_back(std::pair(segMeta));
//    }
  }

  if (Configuration::GetEventFrameworkEnabled()) {
    event_call(Configuration::GetDispatchThreadCoreId(),
               registerDispatchRoutine, this, nullptr);
    for (uint32_t threadId = 0;
         threadId < Configuration::GetNumIoThreads(); 
         ++threadId) {
      event_call(Configuration::GetIoThreadCoreId(threadId),
                 registerIoCompletionRoutine, &mIoThread[threadId], nullptr);
    }
  } else {
    initEcThread();
    initDispatchThread();
    initIndexThread();
    initCompletionThread();
    initIoThread();
  }

  // Preallocate segments
  mNumOpenSegments = Configuration::GetNumOpenSegments();

  mStripeWriteContextPools = new StripeWriteContextPool *[mNumOpenSegments + 2];
  for (uint32_t i = 0; i < mNumOpenSegments + 2; ++i) {
    if (Configuration::GetSystemMode() == NAMED_WRITE) {
      mStripeWriteContextPools[i] = new StripeWriteContextPool(1, mRequestContextPoolForSegments);
    } else {
      mStripeWriteContextPools[i] = new StripeWriteContextPool(64, mRequestContextPoolForSegments);
    }
  }

  mOpenSegments.resize(mNumOpenSegments);
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    createSegmentIfNeeded(&mOpenSegments[i], i);
  }
  if (Configuration::GetSystemMode() == REDIRECTION) {
    createSegmentIfNeeded(&mSpareSegment, mNumOpenSegments);
  }

  // init Gc
  initGc();

  Configuration::PrintConfigurations();
}

RAIDController::~RAIDController()
{
  for (auto segment : mSealedSegments) {
    printf("Zone group: %p\n", segment);
//    segment->Reset();
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    for (uint32_t i = 0; i < Configuration::GetNumIoThreads(); ++i) {
      thread_send_msg(mIoThread[i].thread, quit, nullptr);
    }
    thread_send_msg(mDispatchThread, quit, nullptr);
    thread_send_msg(mEcThread, quit, nullptr);
    thread_send_msg(mIndexThread, quit, nullptr);
    thread_send_msg(mCompletionThread, quit, nullptr);
  }
}


void RAIDController::initGc()
{
  mGcTask.dataBuffer = (uint8_t*)spdk_zmalloc(
                     8 * Configuration::GetBlockSize(), 4096,
                     NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.metaBuffer = (uint8_t*)spdk_zmalloc(
                     8 * Configuration::GetMetadataSize(),
                     4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.stage = IDLE;
}

uint32_t RAIDController::GcBatchUpdateIndex(
    const std::vector<uint64_t> &lbas,
    const std::vector<std::pair<PhysicalAddr, PhysicalAddr>> &pbas)
{
  uint32_t numSuccessUpdates = 0;
  assert(lbas.size() == pbas.size());
  for (int i = 0; i < lbas.size(); ++i) {
    uint64_t lba = lbas[i];
    PhysicalAddr oldPba = pbas[i].first;
    PhysicalAddr newPba = pbas[i].second;
    assert(mAddressMap->find(lba) != mAddressMap->end());
    if ((mAddressMap->find(lba))->second == oldPba) {
      numSuccessUpdates += 1;
      UpdateIndex(lba, newPba);
    }
  }
  return numSuccessUpdates;
}

void RAIDController::UpdateIndex(uint64_t lba, PhysicalAddr pba)
{
  // Invalidate the old block
  if (mAddressMap->find(lba) != mAddressMap->end()) {
    PhysicalAddr oldPba = (*mAddressMap)[lba];
    oldPba.segment->InvalidateBlock(pba.zoneId, pba.offset);
    mNumInvalidBlocks += 1;
  }
  assert(pba.segment != nullptr);
  (*mAddressMap)[lba] = pba;  
//  printf("Update lba %lu with pba %u %u %u\n", lba, pba.segment, pba.zoneId, pba.offset);
  pba.segment->FinishBlock(pba.zoneId, pba.offset, lba);
  mNumBlocks += 1;
}

void RAIDController::Write(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  doExecute(offset, size, data, true, cb_fn, cb_args);
}

void RAIDController::Read(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  doExecute(offset, size, data, false, cb_fn, cb_args);
}

void RAIDController::ReclaimContexts()
{
  int numSuccessfulReclaims = 0;
  for (auto it = mInflightRequestContext.begin();
            it != mInflightRequestContext.end(); ) {
    if ((*it)->available) {
      (*it)->Clear();
      mRequestContextPoolForUserRequests->returnRequestContext(*it);
      it = mInflightRequestContext.erase(it);
    } else {
      ++it;
    }
  }
}

void RAIDController::Flush()
{
  bool remainWrites;
  do {
    remainWrites = false;
    for (auto it = mInflightRequestContext.begin();
              it != mInflightRequestContext.end(); ) {
      if ((*it)->available) {
        (*it)->Clear();
        mRequestContextPoolForUserRequests->returnRequestContext(*it);
        it = mInflightRequestContext.erase(it);
      } else {
        if ((*it)->req_type == 'W') {
          remainWrites = true;
        }
        ++it;
      }
    }
  } while (remainWrites);
}

RequestContext* RAIDController::getContextForUserRequest()
{
  RequestContext *ctx = mRequestContextPoolForUserRequests->getRequestContext(false);
  while (ctx == nullptr) {
    ReclaimContexts();
    ctx = mRequestContextPoolForUserRequests->getRequestContext(false);
  }

  mInflightRequestContext.insert(ctx);
  ctx->Clear();
  ctx->available = false;
  ctx->meta = nullptr;
  ctx->ctrl = this;
  return ctx;
}

void RAIDController::doExecute(
    uint64_t offset, uint32_t size, void *data, bool is_write,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  RequestContext *ctx = getContextForUserRequest();
  ctx->type = USER;
  ctx->data = (uint8_t*)data;
  ctx->lba = offset;
  ctx->size = size;
  ctx->targetBytes = size;
  ctx->cb_fn = cb_fn;
  ctx->cb_args = cb_args;
  if (is_write) {
    ctx->req_type = 'W';
    ctx->status = WRITE_REAPING;
  } else {
    ctx->req_type = 'R';
    ctx->status = READ_PREPARE;
  }

  // printf("PointA: %p\n", ctx);
//  gettimeofday(&ctx->timeA, NULL);
  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mDispatchThread, enqueueRequest, ctx);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               enqueueRequest2, ctx, nullptr);
  }

  return ;
}

void RAIDController::WriteInDispatchThread(RequestContext *ctx)
{
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t curOffset = ctx->curOffset;
  uint32_t size = ctx->size;
  uint32_t pos = curOffset;
  if (ctx->timestamp == ~0ull) {
    ctx->timestamp = mGlobalTimestamp++;
  }
  if (curOffset == 0) {
    ctx->pbaArray.resize(size / blockSize);
  }

  for ( ; pos < size; pos += blockSize) {
    uint32_t openGroupId = mNextAppendOpenSegment;
    mNextAppendOpenSegment = (openGroupId + 1) % mNumOpenSegments;
    bool success = false;

    for (uint32_t trys = 0; trys < mNumOpenSegments; trys += 1) {
      assert(mOpenSegments[openGroupId]);

      success = mOpenSegments[openGroupId]->Append(ctx, pos);
      if (mOpenSegments[openGroupId]->IsFull()) {
        printf("Going to open a new segment!\n");
        mSegmentsToSeal.emplace_back(mOpenSegments[openGroupId]);
        mOpenSegments[openGroupId] = nullptr;
        createSegmentIfNeeded(&mOpenSegments[openGroupId], openGroupId);
      }
      if (success) {
        break;
      }
      openGroupId = (openGroupId + 1) % mNumOpenSegments; 
    }

    if (Configuration::GetSystemMode() == REDIRECTION && !success) {
      // If enabled spare, try spare
      success = mSpareSegment->Append(ctx, pos);
      if (mSpareSegment->IsFull()) {
        mSegmentsToSeal.emplace_back(mSpareSegment);
        mSpareSegment = nullptr;
      }
      createSegmentIfNeeded(&mSpareSegment, mNumOpenSegments + 1);
    }

    if (!success) {
      EnqueueEvent(ctx);
      break;
    }
  }
  ctx->curOffset = pos;
}

bool RAIDController::LookupIndex(uint64_t lba, PhysicalAddr *pba)
{
  auto it = mAddressMap->find(lba);
  if (it != mAddressMap->end()) {
    *pba = it->second;
//    printf("Lookup lba %lu with (*pba) %p %u %u\n", lba, (*pba).segment, (*pba).zoneId, (*pba).offset);
    return true;
  } else {
    pba->segment = nullptr;
//    printf("Lookup lba %lu with pba not found\n", lba);
    return false;
  }
}

void RAIDController::ReadInDispatchThread(RequestContext *ctx)
{
  uint64_t slba = ctx->lba;
  int size = ctx->size;
  void *data = ctx->data;
  uint32_t numBlocks = size / Configuration::GetBlockSize();

  if (ctx->status == READ_PREPARE) {
    ctx->status = READ_INDEX_QUERYING;
    ctx->pbaArray.resize(numBlocks);
    if (!Configuration::GetEventFrameworkEnabled()) {
      QueryPbaArgs *args = (QueryPbaArgs *)calloc(1, sizeof(QueryPbaArgs));
      args->ctrl = this;
      args->ctx = ctx;
      thread_send_msg(mIndexThread, queryPba, args);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 queryPba2, this, ctx);
    }
  } else if (ctx->status == READ_INDEX_READY) {
    ctx->status = READ_REAPING;
    for (uint32_t i = 0; i < numBlocks; ++i) {
      Segment *segment = ctx->pbaArray[i].segment;
      if (segment == nullptr) {
        uint8_t *block = (uint8_t *)data + i * Configuration::GetBlockSize();
        memset(block, 0, Configuration::GetBlockSize());
        ctx->successBytes += Configuration::GetBlockSize();
        if (ctx->successBytes == ctx->targetBytes) {
          ctx->Queue();
        }
      } else {
        segment->Read(ctx, i * Configuration::GetBlockSize(), ctx->pbaArray[i]);
      }
    }
  }
}

bool RAIDController::scheduleGc()
{
  if (mNumBlocks == 0 || 1.0 * mNumInvalidBlocks / mNumBlocks < 0.15) {
    return false;
  }
  printf("NumBlocks: %ld, NumInvalidBlocks: %ld, ratio: %lf\n", mNumBlocks, mNumInvalidBlocks, (double)mNumInvalidBlocks / mNumBlocks);

  // Use Greedy algorithm to pick segments
  std::vector<Segment*> groups;
  for (Segment *segment : mSealedSegments) {
    if (!segment->CheckOutstandingWrite()) {
      groups.emplace_back(segment);
    }
  }
  if (groups.size() == 0) {
    return false;
  }
  std::sort(groups.begin(), groups.end(), [](const Segment *lhs, const Segment *rhs) {
      double score1 = (double)lhs->GetNumInvalidBlocks() / rhs->GetNumBlocks();
      double score2 = (double)lhs->GetNumInvalidBlocks() / rhs->GetNumBlocks(); 
      return score1 < score2;
      });

  mGcTask.inputSegment = groups[0];

  mSealedSegments.erase(std::find(mSealedSegments.begin(), mSealedSegments.end(), groups[0]));

  mGcTask.maxZoneId = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  mGcTask.maxOffset = Configuration::GetZoneCapacity();

  printf("Schedule new GC! %ld %ld\n", mNumBlocks, mNumInvalidBlocks);

  return true;
}

bool RAIDController::ProceedGc()
{
  bool hasProgress = false;
  if (!Configuration::GetEnableGc()) {
    return hasProgress;
  }

  if (mGcTask.stage == IDLE) { // IDLE
    if (scheduleGc()) {
      hasProgress = true;
      printf("GC: idle to scheduling\n");
      mGcTask.stage = INIT;
    }
  }

  if (mGcTask.stage == INIT) {
    initializeGcTask();
  }

  if (mGcTask.stage == REWRITING) {
    hasProgress |= progressGcWriter();
    hasProgress |= progressGcReader();

    if (mGcTask.curZoneId == mGcTask.maxZoneId) {
      if (mGcTask.numWriteFinish == mGcTask.numWriteSubmitted) {
//        printf("Number of valid rewritten blocks: %lu, num blocks in zone group: %lu\n", mGcTask.mappings.size(), mGcTask.inputSegment->GetNumBlocks());
        mGcTask.stage = REWRITE_COMPLETE;
      }
    }
  } 
  
  if (mGcTask.stage == REWRITE_COMPLETE) {
    hasProgress = true;
    mGcTask.stage = INDEX_UPDATING;
  }

  if (mGcTask.stage == INDEX_UPDATE_COMPLETE) {
    mNumInvalidBlocks -= mGcTask.inputSegment->GetNumBlocks();
    mNumBlocks -= mGcTask.inputSegment->GetNumBlocks();
    assert(mNumInvalidBlocks > 0);
    printf("Complete finishing\n");
    mGcTask.inputSegment->Reset(nullptr);
    mGcTask.stage = IDLE;
  }

  return hasProgress;
}

void RAIDController::Drain()
{
  printf("Perform draining on the system.\n");
  DrainArgs args;
  args.ctrl = this;
  args.success = false;
  while (!args.success) {
    args.ready = false;
    thread_send_msg(mDispatchThread, tryDrainController, &args);
    busyWait(&args.ready);
  }
}

int RAIDController::GetNumInflightRequests()
{
  return mInflightRequestContext.size();
}

bool RAIDController::ExistsGc()
{
  return mGcTask.stage != IDLE;
}

void RAIDController::createSegmentIfNeeded(Segment **segment, uint32_t spId)
{
  if (*segment != nullptr) return;

  Segment *seg = new Segment(this, mNextAssignedSegmentId++,
                             mRequestContextPoolForSegments, mReadContextPool,
                             mStripeWriteContextPools[spId]);
  for (uint32_t i = 0; i < Configuration::GetStripeSize() / Configuration::GetStripeUnitSize(); ++i) {
    Zone* zone = mDevices[i]->OpenZone();
    if (zone == nullptr) {
      printf("No available zone in device %d, storage space is exhuasted!\n", i);
    }
    seg->AddZone(zone);
  }
  seg->FinalizeCreation();

  *segment = seg;
}

void RAIDController::sealSegmentIfNeeded(Segment **segment)
{
  if ((*segment)->IsFull()) {
    printf("Seal old zone group: %p\n", *segment);
//    (*segment)->Seal();
    mSealedSegments.emplace_back(*segment);
    *segment = nullptr;
  }
}

std::queue<RequestContext*>& RAIDController::GetRequestQueue()
{
  return mRequestQueue;
}

std::mutex& RAIDController::GetRequestQueueMutex()
{
  return mRequestQueueMutex;
}

struct spdk_thread *RAIDController::GetIoThread(int id)
{
  return mIoThread[id].thread;
}

struct spdk_thread *RAIDController::GetDispatchThread()
{
  return mDispatchThread;
}

struct spdk_thread *RAIDController::GetEcThread()
{
  return mEcThread;
}

struct spdk_thread *RAIDController::GetIndexThread()
{
  return mIndexThread;
}

struct spdk_thread *RAIDController::GetCompletionThread()
{
  return mCompletionThread;
}

std::vector<RequestContext*>& RAIDController::GetEventsToDispatch()
{
  return mEventsToDispatch;
}

void RAIDController::EnqueueEvent(RequestContext *ctx)
{
  mEventsToDispatch.emplace_back(ctx);
}

void RAIDController::initializeGcTask()
{
  mGcTask.curZoneId = 0;
  mGcTask.nextOffset = 0;
  mGcTask.stage = REWRITING;

  mGcTask.writerPos = 0;
  mGcTask.readerPos = 0;

  mGcTask.numWriteSubmitted = 0;
  mGcTask.numWriteFinish = 0;

  mGcTask.mappings.clear();

  // Initialize the status of the context pool
  for (uint32_t i = 0; i < 8; ++i) {
    mGcTask.contextPool[i].available = true;
    mGcTask.contextPool[i].ctrl = this;
    mGcTask.contextPool[i].pbaArray.resize(1);
    mGcTask.contextPool[i].gcTask = &mGcTask;
    mGcTask.contextPool[i].type = GC;
    mGcTask.contextPool[i].lba = ~0ull;
    mGcTask.contextPool[i].data = (uint8_t*)mGcTask.dataBuffer + i * Configuration::GetStripeUnitSize();
    mGcTask.contextPool[i].meta = (uint8_t*)mGcTask.metaBuffer + i * Configuration::GetMetadataSize();
    mGcTask.contextPool[i].targetBytes = Configuration::GetBlockSize();
    mGcTask.contextPool[i].status = WRITE_COMPLETE;
  }
}


bool RAIDController::progressGcReader()
{
  bool hasProgress = false;
  // Find contexts that are available, schedule read for valid blocks
  RequestContext *nextReader = &mGcTask.contextPool[mGcTask.readerPos];
  while (nextReader->available && (nextReader->status == WRITE_COMPLETE)) {
    hasProgress = true;
    if (nextReader->lba != ~0ull) {
      // The sign of valid lba means a successful rewrite a valid block
      // So we update the information here
      mGcTask.numWriteFinish += 1;
      mGcTask.mappings[nextReader->lba].second = nextReader->pbaArray[0];
    }

    nextReader->available = false;
    nextReader->lba = 0;
    if (mGcTask.curZoneId != mGcTask.maxZoneId) {
      nextReader->req_type = 'R';
      nextReader->status = READ_REAPING;
      nextReader->successBytes = 0;

      bool valid = false;
      do {
        nextReader->segment = mGcTask.inputSegment;
        nextReader->zoneId = mGcTask.curZoneId;
        nextReader->offset = mGcTask.nextOffset;

        mGcTask.inputSegment->ReadValid(
            nextReader, 0, nextReader->GetPba(), &valid);

        mGcTask.nextOffset += 1;
        if (mGcTask.nextOffset == mGcTask.maxOffset) {
          mGcTask.nextOffset = 0;
          mGcTask.curZoneId += 1;
        }
      } while (!valid && mGcTask.curZoneId != mGcTask.maxZoneId);
    }
    mGcTask.readerPos = (mGcTask.readerPos + 1) % 8;
    nextReader = &mGcTask.contextPool[mGcTask.readerPos];
  }

  return hasProgress;
}

bool RAIDController::progressGcWriter()
{
  bool hasProgress = false;
  // Process blocks that are read and valid, and rewrite them 
  RequestContext *nextWriter = &mGcTask.contextPool[mGcTask.writerPos];
  while (nextWriter->available && nextWriter->status == READ_COMPLETE) {
    uint64_t lba = ((BlockMetadata*)nextWriter->meta)->fields.protectedField.lba;
    if (lba == ~0ull) {
      fprintf(stderr, "GC write does not expect block with invalid lba!\n");
      exit(-1);
    }
    assert(lba != ~0ull);

    PhysicalAddr oldPba = nextWriter->GetPba();
    RequestContext backup; // Backup prevents from context lost due to retry
    backup.CopyFrom(*nextWriter);
    nextWriter->lba = lba;
    nextWriter->req_type = 'W';
    nextWriter->status = WRITE_REAPING;
    nextWriter->successBytes = 0;
    nextWriter->available = false;
    nextWriter->timestamp = ((BlockMetadata*)nextWriter->meta)->fields.protectedField.timestamp;

    createSegmentIfNeeded(&mGcTask.outputSegment, mNumOpenSegments + 1);
    if (!mGcTask.outputSegment->Append(nextWriter, 0)) {
      nextWriter->CopyFrom(backup);
      break;
    }
    sealSegmentIfNeeded(&mGcTask.outputSegment);

    mGcTask.mappings[lba] = std::make_pair(oldPba, PhysicalAddr());
    mGcTask.numWriteSubmitted += 1;

    mGcTask.writerPos = (mGcTask.writerPos + 1) % 8;
    nextWriter = &mGcTask.contextPool[mGcTask.writerPos];

    hasProgress = true;
  }
  return hasProgress;
}

bool RAIDController::CheckSegments()
{
  bool stateChanged = false;
  for (Segment *segment : mOpenSegments) {
    if (segment != nullptr) {
      stateChanged |= segment->StateTransition();
    }
  }
  if (mSpareSegment != nullptr) {
    stateChanged |= mSpareSegment->StateTransition();
  }

  std::vector<Segment*> sealedSegments;
  for (auto it = mSegmentsToSeal.begin();
      it != mSegmentsToSeal.end();
      )
  {
    stateChanged |= (*it)->StateTransition();
    if ((*it)->GetStatus() == SEGMENT_SEALED) {
      printf("Sealed!\n");
      it = mSegmentsToSeal.erase(it);
      mSealedSegments.push_back(*it);
    } else {
      ++it;
    }
  }

  return stateChanged;
}

GcTask* RAIDController::GetGcTask()
{
  return &mGcTask;
}
