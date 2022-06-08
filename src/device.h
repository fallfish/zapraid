#ifndef __DEVICE_H__
#define __DEVICE_H__

#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"
#include "common.h"
#include <map>
#include <vector>

class Zone;
class Device {
public:
  void Init(struct spdk_nvme_ctrlr *ctrlr, int nsid);
  void InitZones();

  void ConnectIoPairs();
  void Write(uint64_t offset, uint32_t size, void *ctx);
  void Append(uint64_t offset, uint32_t size, void *ctx);
  void Read(uint64_t offset, uint32_t size, void *ctx);

  Zone* OpenZone();
  void AddAvailableZone(Zone *zone);
  void ResetZone(Zone *zone, void *ctx);
  void FinishZone(Zone *zone, void *ctx);
  void SetDeviceId(uint32_t deviceId) { mDeviceId = deviceId; }
  uint32_t GetDeviceId() { return mDeviceId; }
  struct spdk_nvme_ctrlr* GetController() { return mController; }
  struct spdk_nvme_ns* GetNamespace() { return mNamespace; }
  struct spdk_nvme_qpair** GetIoQueues() { return mIoQueues; }

  struct spdk_thread *GetPollThread() { return mPollThread; }
  void StartPolling(int coreId);
  uint32_t GetNumZones();


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

  std::map<int, Zone*> mUsedZones;
  std::vector<Zone*> mAvailableZones;
  Zone* mZones;
};

#endif
