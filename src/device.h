#ifndef __DEVICE_H__
#define __DEVICE_H__

#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"
#include "common.h"
#include <unordered_map>
#include <vector>
#include <set>

class Zone;
class Device {
public:
  void Init(struct spdk_nvme_ctrlr *ctrlr, int nsid);
  void InitZones(uint32_t numNeededZones, uint32_t numReservedZones);
  void EraseWholeDevice();
  void ConnectIoPairs();

  // I/O operations
  void Write(uint64_t offset, uint32_t size, void *ctx); // zone write
  void Append(uint64_t offset, uint32_t size, void *ctx); // zone append
  void Read(uint64_t offset, uint32_t size, void *ctx); // zone read

  // admin commands
  void ResetZone(Zone *zone, void *ctx);
  void FinishZone(Zone *zone, void *ctx); // seal
  bool HasAvailableZone();
  Zone* OpenZone();
  Zone* OpenZoneBySlba(uint64_t slba);
  void  ReturnZone(Zone*);

  void AddAvailableZone(Zone *zone);

  void SetDeviceId(uint32_t deviceId) { mDeviceId = deviceId; }
  uint32_t GetDeviceId() { return mDeviceId; }

  struct spdk_nvme_ctrlr* GetController() { return mController; }
  struct spdk_nvme_ns* GetNamespace() { return mNamespace; }
  struct spdk_nvme_qpair** GetIoQueues() { return mIoQueues; }
  struct spdk_nvme_qpair* GetIoQueues(uint32_t id) { return mIoQueues[id]; }

  uint64_t GetZoneCapacity();
  uint32_t GetNumZones();

  ReadZoneHeaders(std::map<uint64_t, std::pair<uint32_t, uint8_t*>> &zones);

private:
  void issueIo2(spdk_event_fn event_fn, RequestContext *slot);
  void issueIo(spdk_msg_fn msg_fn, RequestContext *slot);
  uint64_t bytes2Block(uint64_t bytes);
  uint64_t bytes2ZoneNum(uint64_t bytes);

  struct spdk_nvme_ctrlr *mController;
  struct spdk_nvme_ns *mNamespace;

  struct spdk_nvme_qpair* *mIoQueues;

  uint64_t mZoneSize; // in blocks
  uint64_t mZoneCapacity; // in blocks
  uint32_t mNumZones; // in blocks

  uint32_t mDeviceId;

  std::set<Zone*> mAvailableZones;
  Zone* mZones;
};

#endif
