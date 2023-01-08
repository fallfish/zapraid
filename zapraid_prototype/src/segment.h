#ifndef __ZONE_GROUP_H__
#define __ZONE_GROUP_H__
#include "common.h"
#include "zone.h"
#include <set>
#include <vector>
#include <list>

class RAIDController;

// Write at the beginning of each zone in the segment; replicate to each zone
struct SegmentMetadata {
  uint64_t segmentId; // 8
  uint64_t zones[16]; // 128
  uint32_t stripeSize; // 4
  uint32_t stripeDataSize; // 4
  uint32_t stripeParitySize; // 4
  uint32_t n; // 4
  uint32_t k; // 4
  uint32_t numZones; // 4
  uint8_t  raidScheme; // 1
};

enum SegmentStatus {
  SEGMENT_NORMAL,
  SEGMENT_CONCLUDING_WRITES_IN_GROUP,
  SEGMENT_CONCLUDING_APPENDS_IN_GROUP,
  SEGMENT_WRITING_HEADER,
  SEGMENT_PREPARE_FOOTER,
  SEGMENT_WRITING_FOOTER,
  SEGMENT_SEALING,
  SEGMENT_SEALED
};

class Segment
{
public:
  Segment(RAIDController* raidController, uint32_t segmentId,
          RequestContextPool *ctxPool, ReadContextPool *rctxPool,
          StripeWriteContextPool *sctxPool);
  ~Segment();

  /**
   * @brief Add a zone to the segment.
   *        Called by controller when creating new segment.
   * 
   * @param zone 
   */
  void AddZone(Zone *zone);

  /**
   * @brief Finalize the new segment creation
   *
   */
  void FinalizeCreation();
  /**
   * @brief Summarize the header and write the headers to the zones.
   *        Called by controller after intializing all zones.
   * 
   */
  void FinalizeSegmentHeader();

  /**
   * @brief Get the Zones object
   * 
   * @return const std::vector<Zone*>& 
   */
  const std::vector<Zone*>& GetZones();

  /**
   * @brief (Attempt) to Append a block to the segment
   * 
   * @param ctx the user request that contains the data, lba, etc
   * @param offset which block in the user request to append
   * @return true successfully issued the block to the drive
   * @return false the segment is busy; cannot proceed to issue
   */
  bool Append(RequestContext *ctx, uint32_t offset);

  /**
   * @brief Read a block from the segment
   * 
   * @param ctx the user request that contains the data buffer, lba, etc
   * @param offset which block in the user request to append
   * @param phyAddr the physical block address of the requested block
   * @return true successfully issued the request to the drive
   * @return false the segment is busy (no read context remains); cannot proceed to read
   */
  bool Read(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr);

  /**
   * @brief Ensure the block is valid before performing read
   * 
   */
  bool ReadValid(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr, bool *isValid);

  /**
   * @brief Reset all the zones in the segment
   *        Called by controller only after the segment is collected by GC
   * 
   * @param ctx the controller maintained context, tracking the reset progress
   */
  void Reset(RequestContext *ctx);
  bool IsResetDone();

  /**
   * @brief Seal the segment by finishing all the zones
   *        Called by controller after the data region is full and the footer region is persisted
   */
  void Seal();

  uint64_t GetCapacity() const;
  uint32_t GetNumBlocks() const;
  uint32_t GetNumInvalidBlocks() const;
  uint32_t GetSegmentId();
  uint32_t GetZoneSize();

  /**
   * @brief Specify whether the segment can accept new blocks
   * 
   * @return true The segment is full and cannot accept new blocks; need to write the footer
   * @return false The segment is not full and can accept new blocks
   */
  bool IsFull();
  bool CanSeal();

  bool CheckOutstandingWrite();
  bool CheckOutstandingRead();

//  void ReadStripeMeta(RequestContext *ctx);
  void ReadStripe(RequestContext *ctx);

  void WriteComplete(RequestContext *ctx);
  void ReadComplete(RequestContext *ctx);

  void InvalidateBlock(uint32_t zoneId, uint32_t realOffset);
  void FinishBlock(uint32_t zoneId, uint32_t realOffset, uint64_t lba);
  void PrintStats();
  void Dump();

  void ReclaimReadContext(ReadContext *context);

  void FlushCurrentStripe();
  bool StateTransition();
  SegmentStatus GetStatus();
  void ReleaseZones();

  void GenerateParityBlock(StripeWriteContext *stripe, uint32_t zonePos);
  void ProgressFooterWriter();

  uint32_t GetPos();

  // For recovery
  void SetSegmentStatus(SegmentStatus status);
  
  void SetZonesAndWpForRecovery(std::vector<std::pair<uint64_t, uint8_t*>> zonesWp);

  void RecoverLoadAllBlocks();
  bool RecoverFooterRegionIfNeeded();
  bool RecoverNeedRewrite();
  void RecoverFromOldSegment(Segment *segment);
  void RecoverState();

  void RecoverIndexFromSealedSegment(uint8_t *buffer, std::pair<uint64_t, PhysicalAddr> *indexMap);
  void RecoverIndexFromOpenSegment(std::pair<uint64_t, PhysicalAddr> *indexMap);

  void ResetInRecovery();
  void FinishRecovery();

private:
  RequestContextPool *mRequestContextPool;

  StripeWriteContext *mCurStripe;

  ReadContextPool *mReadContextPool;
  StripeWriteContextPool *mStripeWriteContextPool;

  void degradedRead(RequestContext *ctx, PhysicalAddr phyAddr);

  void recycleStripeWriteContexts();
  void recycleReadContexts();
  void recycleContexts();
  RequestContext* GetRequestContext();
  void ReturnRequestContext(RequestContext *slot);
  bool checkStripeAvailable(StripeWriteContext *stripeContext);
  bool checkReadAvailable(ReadContext *stripeContext);
  bool findStripe();
  void encodeStripe(uint8_t **stripe, uint32_t n, uint32_t k, uint32_t unitSize);
  void decodeStripe(uint32_t offset, uint8_t **stripe, bool *alive, uint32_t n, uint32_t k, uint32_t decodeIndex);

  void fillInFooterBlock(uint8_t **data, uint32_t pos);
  
  bool* mValidBits;
  uint8_t* mCompactStripeTable;
  CodedBlockMetadata *mCodedBlockMetadata;

  std::vector<Zone*> mZones;
  std::vector<RequestContext> mResetContext;

  uint32_t mPos;

  uint32_t mHeaderRegionSize;
  uint32_t mDataRegionSize;
  uint32_t mFooterRegionSize;

  uint32_t mNumInvalidBlocks;
  uint32_t mNumBlocks;

  uint32_t mPosInStripe;
  uint32_t mCurStripeId;

  static uint8_t *gEncodeMatrix;
  static uint8_t *gGfTables;

  struct timeval mS, mE;

  SegmentMetadata mSegmentMeta;
  SegmentStatus mSegmentStatus;

  RAIDController *mRaidController;

  StripeWriteContext *mAdminStripe;

  // for recovery
  uint8_t *mDataBufferForRecovery[16];
  uint8_t *mMetadataBufferForRecovery[16];
  std::map<uint32_t, std::vector<uint64_t>> mStripesToRecover;
  std::vector<std::pair<uint64_t, uint8_t*>> mZonesWpForRecovery;
  double mLastStripeCreationTimestamp = 0;
};

#endif

