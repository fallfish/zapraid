#include "raid_controller.h"
#include "zone.h"

#include <sys/time.h>
#include <map>
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
#include "helpers.h"

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

void RAIDController::initDispatchThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, 2, true);
  mDispatchThread = spdk_thread_create("DispatchThread", &cpumask);
  printf("ZNS_RAID dispatch thread %s %d\n", spdk_thread_get_name(mDispatchThread), spdk_thread_get_id(mDispatchThread));
  int rc = spdk_env_thread_launch_pinned(2, dispatchWorker, this);
  if (rc < 0) {
    printf("Failed to launch dispatch thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
  }
}

void RAIDController::initIoThread()
{
  struct spdk_cpuset cpumask;
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_set_cpu(&cpumask, threadId + 3, true);
    mIoThread[threadId].thread = spdk_thread_create("IoThread", &cpumask);
    assert(mIoThread[threadId].thread != nullptr);
    mIoThread[threadId].controller = this;
    int rc = spdk_env_thread_launch_pinned(threadId + 3, ioWorker, &mIoThread[threadId]);
    printf("ZNS_RAID io thread %s %d\n", spdk_thread_get_name(mIoThread[threadId].thread), spdk_thread_get_id(mIoThread[threadId].thread));
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
    opts.core_mask = "0x3f";
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
  mRequestContextPool = new RequestContext[mNumRequestContext];
  mAvailableRequestContext.clear();
  mInflightRequestContext.clear();
  for (uint32_t i = 0; i < mNumRequestContext; ++i) {
    mRequestContextPool[i].Clear();
    mAvailableRequestContext.emplace_back(&mRequestContextPool[i]);
  }

  // Preallocate zone group pointers
  mOpenZoneGroups.resize(mNumOpenZoneGroups);
  for (uint32_t i = 0; i < mNumOpenZoneGroups; ++i) {
    mOpenZoneGroups[i] = nullptr;
  }

  // Initialize address map
  mAddressMap = new std::map<LogicalAddr, PhysicalAddr>();

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

  initDispatchThread();
  initIoThread();

  // init Gc
  initGc();

  printf("Finish init zns raid, system mode: %d\n", Configuration::GetSystemMode());
}

RAIDController::~RAIDController()
{
  for (auto zoneGroup : mSealedZoneGroups) {
    printf("Zone group: %p\n", zoneGroup);
//    zoneGroup->Reset();
  }
  for (uint32_t i = 0; i < Configuration::GetNumIoThreads(); ++i) {
    spdk_thread_send_msg(mIoThread[i].thread, quit, nullptr);
  }
  spdk_thread_send_msg(mDispatchThread, quit, nullptr);
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

std::vector<Device*>& RAIDController::GetDevices()
{
  return mDevices;
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
    oldPba.zoneGroup->InvalidateBlock(pba.zoneId, pba.offset);
    mNumInvalidBlocks += 1;
  }
  assert(pba.zoneGroup != nullptr);
  // Update the new mapping
  (*mAddressMap)[lba] = pba;  
  pba.zoneGroup->FinishBlock(pba.zoneId, pba.offset, lba);
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
  for (auto it = mInflightRequestContext.begin();
            it != mInflightRequestContext.end(); ) {
    if ((*it)->available) {
      (*it)->Clear();
      mAvailableRequestContext.emplace_back((*it));
      it = mInflightRequestContext.erase(it);
    } else {
      ++it;
    }
  }
}

RequestContext* RAIDController::getRequestContext()
{
  RequestContext *ctx = nullptr;
  while (mAvailableRequestContext.empty()) {
    ReclaimContexts();
  }

  ctx = mAvailableRequestContext.back();
  mAvailableRequestContext.pop_back();
  mInflightRequestContext.emplace_back(ctx);

  ctx->Clear();
  ctx->available = false;
  ctx->ctrl = this;
  return ctx;
}

void RAIDController::doExecute(
    uint64_t offset, uint32_t size, void *data, bool is_write,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  RequestContext *ctx = getRequestContext();
  ctx->type = USER;
  ctx->data = data;
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
    ctx->status = READ_REAPING;
  }
  spdk_thread_send_msg(mDispatchThread, enqueueRequest, ctx);
  return ;
}

void RAIDController::WriteInDispatchThread(RequestContext *ctx)
{
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t curOffset = ctx->curOffset;
  uint32_t size = ctx->size;
  uint32_t pos = curOffset;

  if (curOffset == 0) {
    ctx->pbaArray.resize(size / blockSize);
  }

  for ( ; pos < size; pos += blockSize) {
    uint32_t openGroupId = 0;
    bool success = false;
    for (uint32_t trys = 0; trys < mNumOpenZoneGroups; trys += 1) {
      createZoneGroupIfNeeded(&mOpenZoneGroups[openGroupId]);
      success = mOpenZoneGroups[openGroupId]->Append(ctx, pos);
      sealZoneGroupIfNeeded(&mOpenZoneGroups[openGroupId]);
      if (success) {
        break;
      }
      openGroupId = (openGroupId + 1) % mNumOpenZoneGroups; 
    }
    if (!success) {
      EnqueueEvent(ctx);
      break;
    }
  }
  ctx->curOffset = pos;
}

bool RAIDController::lookupIndex(uint64_t lba, PhysicalAddr *phyAddr)
{
//  std::lock_guard<std::mutex> l(mIndexLock);
  auto it = mAddressMap->find(lba);
  if (it != mAddressMap->end()) {
    *phyAddr = it->second;
    return true;
  }
  return false;
}

void RAIDController::ReadInDispatchThread(RequestContext *ctx)
{
  uint64_t slba = ctx->lba;
  int size = ctx->size;
  void *data = ctx->data;

  std::vector<std::pair<uint64_t, PhysicalAddr>> validLbas;
  for (uint32_t pos = 0; pos < size; pos += Configuration::GetBlockSize()) {
    uint64_t lba = slba + pos;
    uint8_t *block = (uint8_t*)data + pos;
    bool success = false;

    PhysicalAddr phyAddr;
    if (!lookupIndex(lba, &phyAddr)) {
      memset(block, 0, Configuration::GetBlockSize());
      ctx->successBytes += Configuration::GetBlockSize();
      if (ctx->successBytes == ctx->targetBytes) {
        ctx->Queue();
      }
    } else {
      validLbas.emplace_back(std::make_pair(lba, phyAddr));
    }
  }

  for (auto pr : validLbas) {
    uint64_t lba = pr.first;
    PhysicalAddr phyAddr = pr.second;
    ZoneGroup *zoneGroup = phyAddr.zoneGroup;
    zoneGroup->Read(ctx, lba - slba, phyAddr);
  }
}

bool RAIDController::scheduleGc()
{
  if (mNumBlocks == 0 || 1.0 * mNumInvalidBlocks / mNumBlocks < 0.15) {
    return false;
  }

  // Use Greedy algorithm to pick segments
  std::vector<ZoneGroup*> groups;
  groups.insert(groups.end(), mSealedZoneGroups.begin(), mSealedZoneGroups.end());
  std::sort(groups.begin(), groups.end(), [](const ZoneGroup *lhs, const ZoneGroup *rhs) {
      double score1 = (double)lhs->GetNumInvalidBlocks() / rhs->GetNumBlocks();
      double score2 = (double)lhs->GetNumInvalidBlocks() / rhs->GetNumBlocks(); 
      return score1 < score2;
      });

  mGcTask.inputZoneGroup = groups[0];

  mSealedZoneGroups.erase(std::find(mSealedZoneGroups.begin(), mSealedZoneGroups.end(), groups[0]));

  mGcTask.maxZoneId = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  mGcTask.maxOffset = Configuration::GetZoneCapacity();

  printf("Schedule new GC! %ld %ld\n", mNumBlocks, mNumInvalidBlocks);

  return true;
}

bool RAIDController::ProceedGc()
{
  bool hasProgress = false;
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

  if (mGcTask.stage == GC_RUNNING) {
    hasProgress |= progressGcWriter();
    hasProgress |= progressGcReader();

    if (mGcTask.curZoneId == mGcTask.maxZoneId) {
      if (mGcTask.numWriteFinish == mGcTask.numWriteSubmitted) {
        printf("Number of valid rewritten blocks: %lu\n", mGcTask.mappings.size());
        mGcTask.stage = COMPLETE;
      } else {
        if (mGcTask.outputZoneGroup != nullptr) {
          mGcTask.outputZoneGroup->FlushStripe();
        }
      }
    }
  } 
  
  if (mGcTask.stage == COMPLETE) {
    hasProgress = true;
    progressGcIndexUpdate();
    if (mGcTask.mappings.size() == 0) {
      mNumInvalidBlocks -= mGcTask.inputZoneGroup->GetNumBlocks();
      mNumBlocks -= mGcTask.inputZoneGroup->GetNumBlocks();
      printf("Complete finishing\n");
      mGcTask.inputZoneGroup->Reset();
      mGcTask.stage = IDLE;
    }
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
    spdk_thread_send_msg(mDispatchThread, tryDrainController, &args);
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

void RAIDController::createZoneGroupIfNeeded(ZoneGroup **zoneGroup)
{
  if (*zoneGroup != nullptr) return;

  *zoneGroup = new ZoneGroup(this);
  printf("Create new zone group: %p\n", *zoneGroup);
  for (uint32_t i = 0; i < Configuration::GetStripeSize() / Configuration::GetStripeUnitSize(); ++i) {
    Zone* zone = mDevices[i]->OpenZone();
    if (zone == nullptr) {
      printf("No available zone in device %d, storage space is exhuasted!\n", i);
    }
    printf("Allocate new zone: %p %p\n", zone, mDevices[i]);
    (*zoneGroup)->AddZone(zone);
  }
}

void RAIDController::sealZoneGroupIfNeeded(ZoneGroup **zoneGroup)
{
  if ((*zoneGroup)->IsFull()) {
    printf("Seal old zone group: %p\n", *zoneGroup);
//    (*zoneGroup)->Seal();
    mSealedZoneGroups.emplace_back(*zoneGroup);
    *zoneGroup = nullptr;
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
  mGcTask.stage = GC_RUNNING;

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
        nextReader->zoneGroup = mGcTask.inputZoneGroup;
        nextReader->zoneId = mGcTask.curZoneId;
        nextReader->offset = mGcTask.nextOffset;

        mGcTask.inputZoneGroup->ReadValid(
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
    uint64_t lba = ((BlockMetadata*)nextWriter->meta)->d.lba;
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

    createZoneGroupIfNeeded(&mGcTask.outputZoneGroup);
    if (!mGcTask.outputZoneGroup->Append(nextWriter, 0)) {
      nextWriter->CopyFrom(backup);
      break;
    }
    sealZoneGroupIfNeeded(&mGcTask.outputZoneGroup);

    mGcTask.mappings[lba] = std::make_pair(oldPba, PhysicalAddr());
    mGcTask.numWriteSubmitted += 1;

    mGcTask.writerPos = (mGcTask.writerPos + 1) % 8;
    nextWriter = &mGcTask.contextPool[mGcTask.writerPos];

    hasProgress = true;
  }
  return hasProgress;
}

void RAIDController::progressGcIndexUpdate()
{
  std::vector<uint64_t> lbas;
  std::vector<std::pair<PhysicalAddr, PhysicalAddr>> pbas;

  for (uint16_t i = 0; i < 16; ++i) {
    if (mGcTask.mappings.size() == 0) {
      break;
    }
    auto it = mGcTask.mappings.begin();
    lbas.emplace_back(it->first);
    pbas.emplace_back(it->second);
    mGcTask.mappings.erase(it);
  }

  GcBatchUpdateIndex(lbas, pbas);
}
