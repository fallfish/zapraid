#ifndef __RAID_CONTROLLER_H__
#define __RAID_CONTROLLER_H__

#include <vector>
#include <queue>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include "common.h"
#include "device.h"
#include "segment.h"
#include "spdk/thread.h"

class Segment;

class RAIDController {
public:
  ~RAIDController();
  /**
   * @brief Initialzie the RAID block device
   * 
   * @param need_env need to initialize SPDK environment. Example of false,
   *                 app is part of a SPDK bdev and the bdev already initialized the environment.
   */
  void Init(bool need_env);

  /**
   * @brief Write a block from the RAID system
   * 
   * @param offset the logical block address of the RAID block device, in bytes, aligned with 4KiB
   * @param size the number of bytes written, in bytes, aligned with 4KiB
   * @param data the data buffer
   * @param cb_fn call back function provided by the client (SPDK bdev)
   * @param cb_args arguments provided to the call back function
   */
  void Write(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);

  /**
   * @brief Read a block from the RAID system
   * 
   * @param offset the logical block address of the RAID block device, in bytes, aligned with 4KiB
   * @param size the number of bytes read, in bytes, aligned with 4KiB
   * @param data the data buffer
   * @param cb_fn call back function provided by the client (SPDK bdev)
   * @param cb_args arguments provided to the call back function
   */
  void Read(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);

  void Execute(uint64_t offset, uint32_t size, void* data, bool is_write,
      zns_raid_request_complete cb_fn, void *cb_args);

  /**
   * @brief Drain the RAID system to finish all the in-flight requests
   */
  void Drain();

  /**
   * @brief Get the Request Queue object
   * 
   * @return std::queue<RequestContext*>& 
   */
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

  /**
   * @brief Get the Io Thread object
   * 
   * @param id 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetIoThread(int id);
  /**
   * @brief Get the Dispatch Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetDispatchThread();
  /**
   * @brief Get the Ec Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetEcThread();
  /**
   * @brief Get the Index And Completion Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetIndexThread();

  /**
   * @brief Get the Completion Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetCompletionThread();

  /**
   * @brief Get the Events To Dispatch object
   * 
   * @return std::vector<RequestContext*>& 
   */
  std::vector<RequestContext*>& GetEventsToDispatch();

  /**
   * @brief Find a PhysicalAddress given a LogicalAddress of a block
   * 
   * @param lba the logial address of the queried block
   * @param phyAddr the pointer to store the physical address of the queried block
   * @return true the queried block was written before and not trimmed
   * @return false the queried block was not written before or was trimmed
   */
  bool LookupIndex(uint64_t lba, PhysicalAddr *phyAddr);

  void ReclaimContexts();
  void Flush();

  GcTask* GetGcTask();
  uint32_t GetHeaderRegionSize();
  uint32_t GetDataRegionSize();
  uint32_t GetFooterRegionSize();

  void RemoveRequestFromGcEpochIfNecessary(RequestContext *ctx);
private:
  RequestContext* getContextForUserRequest();
  void doWrite(RequestContext *context);
  void doRead(RequestContext *context);
  
  void initEcThread();
  void initDispatchThread();
  void initIoThread();
  void initIndexThread();
  void initCompletionThread();
  void initGc();

  void createSegmentIfNeeded(Segment **segment, uint32_t spId);
  void sealSegmentIfNeeded(Segment **segment);
  bool scheduleGc();

  void initializeGcTask();
  bool progressGcWriter();
  bool progressGcReader();

  std::vector<Device*> mDevices;
  std::unordered_map<uint64_t, PhysicalAddr> *mAddressMap;
  std::vector<Segment*> mSealedSegments;
  std::vector<Segment*> mSegmentsToSeal;
  std::vector<Segment*> mOpenSegments;
  Segment* mSpareSegment;

  RequestContextPool *mRequestContextPoolForUserRequests;
  std::unordered_set<RequestContext*> mInflightRequestContext;

  RequestContextPool *mRequestContextPoolForSegments;
  ReadContextPool *mReadContextPool;
  StripeWriteContextPool **mStripeWriteContextPools;

  std::queue<RequestContext*> mRequestQueue;
  std::mutex mRequestQueueMutex;

  std::mutex mIndexLock;

  struct GcTask mGcTask;

  uint32_t mNumOpenSegments = 1;

  IoThread mIoThread[16];
  struct spdk_thread *mDispatchThread;
  struct spdk_thread *mEcThread;
  struct spdk_thread *mIndexThread;
  struct spdk_thread *mCompletionThread;

  int64_t mNumInvalidBlocks = 0;
  int64_t mNumBlocks = 0;

  std::vector<RequestContext*> mEventsToDispatch;

  uint32_t mAvailableStorageSpaceInSegments = 0;
  uint32_t mStorageSpaceThresholdForGcInSegments = 0;
  uint32_t mNumTotalZones = 0;

  uint32_t mNextAppendOpenSegment = 0;
  uint32_t mNextAssignedSegmentId = 0;
  uint32_t mGlobalTimestamp = 0;

  uint32_t mHeaderRegionSize = 0;
  uint32_t mDataRegionSize = 0;
  uint32_t mFooterRegionSize = 0;

  std::unordered_set<RequestContext*> mReadsInCurrentGcEpoch;
};

#endif
