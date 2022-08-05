#ifndef __DEVICE_H__
#define __DEVICE_H__

#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"
#include "common.h"
#include <unordered_map>
#include <deque>
#include <vector>

class Zone;
class Device {
public:
  void Init(struct spdk_nvme_ctrlr *ctrlr, int nsid);
  void InitZones();
  void EraseWholeDevice();
  void ConnectIoPairs();

  // I/O operations
  void Write(uint64_t offset, uint32_t size, void *ctx); // zone write
  void Append(uint64_t offset, uint32_t size, void *ctx); // zone append
  void Read(uint64_t offset, uint32_t size, void *ctx); // zone read

  // admin commands
  void ResetZone(Zone *zone, void *ctx);
  void FinishZone(Zone *zone, void *ctx); // seal
  Zone* OpenZone();

  void AddAvailableZone(Zone *zone);

  void SetDeviceId(uint32_t deviceId) { mDeviceId = deviceId; }
  uint32_t GetDeviceId() { return mDeviceId; }

  struct spdk_nvme_ctrlr* GetController() { return mController; }
  struct spdk_nvme_ns* GetNamespace() { return mNamespace; }
  struct spdk_nvme_qpair** GetIoQueues() { return mIoQueues; }

  uint32_t GetNumZones();

  std::map<uint64_t, std::pair<uint32_t, uint8_t*>> ReadZoneHeaders();

private:
  uint64_t bytes2Block(uint64_t bytes);
  uint64_t bytes2ZoneNum(uint64_t bytes);

  struct spdk_nvme_ctrlr *mController;
  struct spdk_nvme_ns *mNamespace;

  struct spdk_nvme_qpair* *mIoQueues;

  uint64_t mZoneSize; // in blocks
  uint64_t mZoneCapacity; // in blocks
  uint32_t mNumZones; // in blocks

  uint32_t mDeviceId;

  struct spdk_thread *mPollThread;

  std::unordered_map<int, Zone*> mUsedZones;
  std::vector<Zone*> mAvailableZones;
  Zone* mZones;
};

#endif
