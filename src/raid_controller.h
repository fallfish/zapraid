#ifndef __RAID_CONTROLLER_H__
#define __RAID_CONTROLLER_H__

#include <vector>
#include <queue>
#include <map>
#include "common.h"
#include "device.h"
#include "segment.h"
#include "persistent_metadata.h"
#include "spdk/thread.h"

class Segment;

class RAIDController {
public:
  ~RAIDController();
  void Init(bool need_env);
  void Write(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);
  void Read(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);


  struct spdk_nvme_poll_group* GetPollGroup();
  std::vector<Device*>& GetDevices();
  void Drain();
  std::queue<RequestContext*>& GetRequestQueue();
  std::mutex& GetRequestQueueMutex();

  void UpdateIndexNeedLock(uint64_t lba, PhysicalAddr phyAddr);
  void UpdateIndex(uint64_t lba, PhysicalAddr phyAddr);
  int GetNumInflightRequests();
  bool ProceedGc();
  bool ExistsGc();
  bool CheckSegments();

  void WriteInDispatchThread(RequestContext *ctx);
  void ReadInDispatchThread(RequestContext *ctx);
  void EnqueueEvent(RequestContext *ctx);

  uint32_t GcBatchUpdateIndex(const std::vector<uint64_t> &lbas, const std::vector<std::pair<PhysicalAddr, PhysicalAddr>> &pbas);

  struct spdk_thread *GetIoThread(int id);
  struct spdk_thread *GetDispatchThread();
  std::vector<RequestContext*>& GetEventsToDispatch();

  static bool gEnableSyncGroup;
  void ReclaimContexts();
private:
  RequestContext* getRequestContext();
  void doExecute(uint64_t offset, uint32_t size, void* data, bool is_write, zns_raid_request_complete cb_fn, void *cb_args);
  void doWrite(RequestContext *context);
  void doRead(RequestContext *context);
  
  void initControllerThread();
  void initDispatchThread();
  void initIoThread();
  void initGc();

  bool lookupIndex(uint64_t lba, PhysicalAddr *phyAddr);

  void createSegmentIfNeeded(Segment **segment);
  void sealSegmentIfNeeded(Segment **segment);
  bool scheduleGc();

  void initializeGcTask();
  bool progressGcWriter();
  bool progressGcReader();
  void progressGcIndexUpdate();


  std::vector<Device*> mDevices;
  std::map<LogicalAddr, PhysicalAddr> *mAddressMap;
  std::vector<Segment*> mSealedSegments;
  std::vector<Segment*> mSegmentsToSeal;
  std::vector<Segment*> mOpenSegments;
  Segment* mSpareSegment;

  uint32_t mNumRequestContext = 64;
  RequestContext *mRequestContextPool;
  std::vector<RequestContext*> mAvailableRequestContext;
  std::vector<RequestContext*> mInflightRequestContext;

  std::queue<RequestContext*> mRequestQueue;
  std::mutex mRequestQueueMutex;

  std::mutex mIndexLock;

  struct GcTask mGcTask;

  uint32_t mNumOpenSegments = 1;

  IoThread mIoThread[16];
  struct spdk_thread *mDispatchThread;

  int64_t mNumInvalidBlocks = 0;
  int64_t mNumBlocks = 0;
  struct spdk_thread *mControllerThread;

  std::vector<RequestContext*> mEventsToDispatch;

  uint32_t mNumAvailableZones = 0;
  uint32_t mNumTotalZones = 0;

  uint32_t mNextAssignedSegmentId = 0;
  uint32_t mGlobalTimestamp = 0;

  PersistentMetadata *mPersistentMetadata;
};

#endif
