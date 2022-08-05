#include "device.h"

#include <rte_errno.h>
#include <sys/time.h>
#include "raid_controller.h"
#include "zone.h"
#include "poller.h"

#include "spdk/nvme.h"

static void writeComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  slot->successBytes += Configuration::GetStripeUnitSize();
  slot->Queue();

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write I/O failed, aborting run\n");
  slot->PrintStats();
    exit(1);
  }
};

static void readComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  slot->successBytes += Configuration::GetStripeUnitSize();
  slot->Queue();

  if (spdk_nvme_cpl_is_error(completion)) {
    slot->PrintStats();
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Read I/O failed, aborting run\n");
    exit(1);
  }
};

static void resetComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  slot->Queue();

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Reset I/O failed, aborting run\n");
    exit(1);
  }
};


static void finishComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  slot->Queue();

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Finish I/O failed, aborting run\n");
    exit(1);
  }
};

static void appendComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  slot->successBytes += Configuration::GetStripeUnitSize();
//  printf("Offset: %x %x %d %d %d\n", slot->offset, completion->cdw0, slot->offset, completion->cdw0, slot->offset & completion->cdw0);
  slot->offset = slot->offset & completion->cdw0;
  slot->Queue();

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Append I/O failed, aborting run\n");
    exit(1);
  }
};

static void write(void *args)
{
  RequestContext *slot = (RequestContext*)args;
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

static void read(void *args)
{
  RequestContext *slot = (RequestContext*)args;
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
    printf("%d %d %d %s\n", rc, ioCtx.offset, errno, strerror(errno));
  }
  assert(rc == 0);
}

static void append(void *args)
{
  RequestContext *slot = (RequestContext*)args;
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

static void reset(void *args)
{
  auto ioCtx = ((RequestContext*)args)->ioContext;
  ((RequestContext*)args)->stime = timestamp();
  int rc = spdk_nvme_zns_reset_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device reset error!\n");
  }
  assert(rc == 0);
}

static void finish(void *args)
{
  RequestContext *slot = (RequestContext*)args;
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = spdk_nvme_zns_finish_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device close error!\n");
  }
  assert(rc == 0);
}

inline uint64_t Device::bytes2Block(uint64_t bytes)
{
  return bytes >> 12;
}

inline uint64_t Device::bytes2ZoneNum(uint64_t bytes)
{
  return bytes2Block(bytes) / mZoneSize;
}

void Device::Init(struct spdk_nvme_ctrlr *ctrlr, int nsid)
{
  mController = ctrlr;
  mNamespace = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);

  mZoneSize = spdk_nvme_zns_ns_get_zone_size_sectors(mNamespace);
  mNumZones = spdk_nvme_zns_ns_get_num_zones(mNamespace);
  mZoneCapacity = 1077 * 1024 / 4; // 1,077 * 1024 * 1024 / 4096;
  Configuration::SetZoneCapacity(mZoneCapacity);
  printf("Zone size: %d, zone cap: %d, num of zones: %d\n", mZoneSize, mZoneCapacity, mNumZones);

  struct spdk_nvme_io_qpair_opts opts;
  spdk_nvme_ctrlr_get_default_io_qpair_opts(mController, &opts, sizeof(opts));
  opts.delay_cmd_submit = true;
  opts.create_only = true;
  mIoQueues = new struct spdk_nvme_qpair*[Configuration::GetNumIoThreads()];
  for (int i = 0; i < Configuration::GetNumIoThreads(); ++i) {
    mIoQueues[i] = nullptr;
    mIoQueues[i] = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
    assert(mIoQueues[i]);
  }
}

void Device::ConnectIoPairs()
{
  for (int i = 0; i < Configuration::GetNumIoThreads(); ++i) {
    if (spdk_nvme_ctrlr_connect_io_qpair(mController, mIoQueues[i]) < 0) {
      printf("Connect ctrl failed!\n");
    }
  }
}

void Device::EraseWholeDevice()
{
  bool done = false;
  auto resetComplete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  spdk_nvme_zns_reset_zone(mNamespace, mIoQueues[0], 0, true, resetComplete, &done);

  while (!done) {
    spdk_nvme_qpair_process_completions(mIoQueues[0], 0);
  }
}

void Device::InitZones()
{
  mZones = new Zone[mNumZones];
  for (int i = 0; i < mNumZones; ++i) {
    mZones[i].Init(this, i * mZoneSize, mZoneCapacity, mZoneSize);
    mAvailableZones.emplace_back(&mZones[i]);
  }
}

Zone* Device::OpenZone()
{
  assert(!mAvailableZones.empty());
  Zone* zone = mAvailableZones.back();

  mUsedZones[zone->GetSlba()] = zone;
  mAvailableZones.pop_back();

  return zone;
}

void Device::ResetZone(Zone* zone, void *ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[0];
  slot->ioContext.cb = resetComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.offset = zone->GetSlba();
  slot->ioContext.flags = 0;
  if (spdk_thread_send_msg(slot->ctrl->GetIoThread(0), reset, slot) < 0) {
    printf("send %p failed, error %s\n", slot, rte_strerror(rte_errno));
  }

  mUsedZones.erase(zone->GetSlba());
  mAvailableZones.emplace_back(zone);
}

void Device::FinishZone(Zone *zone, void *ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[0];
  slot->ioContext.cb = finishComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.offset = zone->GetSlba();
  slot->ioContext.flags = 0;

  if (Configuration::GetBypassDevice()) {
    slot->Queue();
    return;
  }

  if (spdk_thread_send_msg(slot->ctrl->GetIoThread(0), finish, slot) < 0) {
    printf("send %p failed, error %s\n", slot, rte_strerror(rte_errno));
  }
}

void Device::Write(uint64_t offset, uint32_t size, void* ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[0];
  slot->ioContext.data = slot->data;
  slot->ioContext.metadata = slot->meta;
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size); 
  slot->ioContext.cb = writeComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  if (Configuration::GetBypassDevice()) {
    slot->successBytes += Configuration::GetBlockSize();
    slot->Queue();
    return;
  }

  if (spdk_thread_send_msg(slot->ctrl->GetIoThread(0), write, slot) < 0) {
    printf("Size of slot: %lu\n", sizeof(*slot));
    assert(0);
  }
}

void Device::Append(uint64_t offset, uint32_t size, void* ctx)
{
  static int curThread = 0; // gNumPollThreads - 1;
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[curThread];
  slot->ioContext.data = slot->data;
  slot->ioContext.metadata = slot->meta;
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size); 
  slot->ioContext.cb = appendComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  if (Configuration::GetBypassDevice()) {
    slot->offset = 0;
    slot->successBytes += Configuration::GetBlockSize();
    slot->Queue();
    return;
  }

  if (spdk_thread_send_msg(slot->ctrl->GetIoThread(curThread), append, slot) < 0) {
    printf("send %p failed, error %s\n", slot, rte_strerror(rte_errno));
    assert(0);
  } 
  curThread = (curThread + 1) % Configuration::GetNumIoThreads();
}

void Device::Read(uint64_t offset, uint32_t size, void* ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[0];
  slot->ioContext.data = slot->data;
  slot->ioContext.metadata = slot->meta;
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size); 
  slot->ioContext.cb = readComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  if (Configuration::GetBypassDevice()) {
    slot->successBytes += Configuration::GetBlockSize();
    slot->Queue();
    return;
  }
  if (spdk_thread_send_msg(slot->ctrl->GetIoThread(0), read, slot) < 0) {
    printf("send %p failed, error %s\n", slot, rte_strerror(rte_errno));
  }
}

void Device::AddAvailableZone(Zone *zone)
{
  assert(mUsedZones.find(zone->GetSlba()) != mUsedZones.end());
  mAvailableZones.push_back(zone);
}

uint32_t Device::GetNumZones()
{
  return mNumZones;
}

std::map<uint64_t, std::pair<uint32_t, uint8_t*>> Device::ReadZoneHeaders()
{
//  bool done = false;
//  auto complete = [](void *arg, const struct spdk_nvme_cpl *completion) {
//    bool *done = (bool*)arg;
//    *done = true;
//  }
//
//  std::map<uint64_t, <uint32_t, uint8_t*>> zones;
//
//  // Read zone report
//  struct spdk_nvme_zns_zone_report *report;
//  uint32_t report_bytes = sizeof(report->descs[0]) * mNumZones + sizeof(*report);
//  report = (struct spdk_nvme_zns_zone_report *)calloc(1, report_bytes);
//  spdk_nvme_zns_report_zones(mNamespace, mIoQueues[0], &states, 4096, 0,
//                             SPDK_NVME_ZRA_LIST_ALL, false, complete, &done);
//  while (!done) ;
//
//  for (uint32_t i = 0; i < report->nr_zones; ++i) {
//    struct spdk_nvme_zns_desc *zdesc = &report->descs[i];
//    uint32_t wp = ~0u;
//    zslbaAndWp.first = zdesc->zslba;
//
//    if (zdesc->zs == SPDK_NVME_ZONE_STATE_FULL) {
//      // This zone belongs to a sealed segment
//      wp = ~0ull;
//    } else if (zdesc->zs == SPDK_NVME_ZONE_STATE_IOPEN 
//               || zdesc->zs == ZONE_STATE_EOPEN) {
//      wp = zdesc->wp;
//    } else {
//      continue;
//    }
//    uint8_t *buffer = spdk_zmalloc(Configuration::GetBlockSize(), 4096,
//                                   NULL, SPDK_ENV_SOCKET_ID_ANY,
//                                   SPDK_MALLOC_DMA);
//    zones[zdesc->zslba] = std::make_pair(wp, buffer);
//  }
//
//  // Read zone headers
//  for (auto z : sealedZones) {
//    done = false;
//    uint64_t zslba = z.first;
//    uint32_t *buffer = z.second;
//    spdk_nvme_ns_cmd_read(mNamespace, mIoQueues[0],
//                          buffer, zslba, 1,
//                          complete, &done, 0);
//    while (!done);
//  }
//
//  for (auto z : openZones) {
//    done = false;
//    uint64_t zslba = z.first.first;
//    uint32_t *buffer = z.second;
//    spdk_nvme_ns_cmd_read(mNamespace, mIoQueues[0],
//                          buffer, zslba, 1,
//                          complete, &done, 0);
//    while (!done);
//  }

//  return zones;
}
