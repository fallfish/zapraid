#ifndef __ZONE_GROUP_H__
#define __ZONE_GROUP_H__
#include "common.h"
#include "zone.h"
#include <set>
#include <vector>

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
  uint8_t  reserved[4096 - 161];
};

enum SegmentStatus {
  SEGMENT_NORMAL,
  SEGMENT_PREPARE_STRIPE_META,
  SEGMENT_WRITING_STRIPE_META,
  SEGMENT_WRITING_HEADER,
  SEGMENT_PREPARE_FOOTER,
  SEGMENT_WRITING_FOOTER,
  SEGMENT_SEALING,
  SEGMENT_SEALED
};

class Segment
{
public:
  Segment(RAIDController* raidController, uint32_t segmentId);
  ~Segment();
  void AddZone(Zone *zone);
  void FinalizeSegmentHeader();

  const std::vector<Zone*>& GetZones();
  bool Append(RequestContext *ctx, uint32_t offset);
  bool Read(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr);
  bool ReadValid(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr, bool *isValid);
  void Reset(RequestContext *ctx);
  void Seal();
  uint64_t GetCapacity() const;
  uint64_t GetNumBlocks() const;
  uint64_t GetNumInvalidBlocks() const;
  uint32_t GetSegmentId();

  bool IsFull();
  bool CanSeal();

  uint32_t GetZoneSize();
  bool CheckOutstandingWrite();
  bool CheckOutstandingRead();

  void ReadStripeMeta(RequestContext *ctx);
  void ReadStripe(RequestContext *ctx);

  void WriteComplete(RequestContext *ctx);
  void ReadComplete(RequestContext *ctx);

  void UpdateSyncPoint(uint32_t zoneId, uint32_t assignedOffset, uint32_t realOffset);

  void InvalidateBlock(uint32_t zoneId, uint32_t realOffset);
  void FinishBlock(uint32_t zoneId, uint32_t realOffset, uint64_t lba);
  void PrintStats();

  bool StateTransition();
  SegmentStatus GetStatus();
  void ReleaseZones();
  void FlushStripe();
private:
  uint32_t mNumInflightStripes = 256;
  struct StripeWriteContext *mStripeWriteContextPool;
  std::vector<StripeWriteContext*> mAvailableStripeWriteContext;
  std::vector<StripeWriteContext*> mInflightStripeWriteContext;
  StripeWriteContext *mCurStripe;

  uint32_t mNumInflightReads = 256;
  struct ReadContext *mReadContextPool;

  std::vector<ReadContext*> mAvailableReadContext;
  std::vector<ReadContext*> mInflightReadContext;
  ReadContext *mCurReadContext;

  uint32_t mNumRequestContext = 2048;
  RequestContext *mRequestContextPool;
  std::vector<RequestContext*> mAvailableRequestContexts;

  void degradedRead(RequestContext *ctx, PhysicalAddr phyAddr);

  void recycleStripeWriteContexts();
  void recycleReadContexts();
  void recycleContexts();
  RequestContext* getRequestContext();
  void returnRequestContext(RequestContext *slot);
  bool checkStripeAvailable(StripeWriteContext *stripeContext, bool recycleContexts);
  bool checkReadAvailable(ReadContext *stripeContext);
  bool findStripe();
  void encodeStripe(uint8_t **stripe, uint32_t n, uint32_t k);
  void decodeStripe(uint32_t offset, uint8_t **stripe, bool *alive, uint32_t n, uint32_t k, uint32_t decodeIndex);
  void progressFooterWriter();

  void issueSyncPoint();
  bool needSyncPoint();
  bool isSyncPointDone();
  
  bool *mValidBits;
  BlockMetadata *mBlockMetadata; // if the backend support _with_md commands, then this is not used.
  std::vector<Zone*> mZones;
  std::vector<RequestContext> mResetContext;
  uint32_t mZonePos;
  uint32_t mCapacity; //  = 1024 * 1024 * 1024ull / gBlockSize;
  uint32_t mInternalCapacity;
  uint32_t mNumInvalidBlocks;
  uint32_t mNumBlocks;

  uint32_t mStripePos;
  uint32_t mCurStripeId;

  static uint8_t *gEncodeMatrix;
  static uint8_t *gGfTables;

  RAIDController *mRaidController;

  SegmentMetadata mSegmentMeta;
  SegmentStatus mSegmentStatus;

  SyncPoint mStripeMeta;

  // footer
  uint32_t mP2LTableSize;
  uint8_t *mP2LTable; // used for persisting L2P table for fast recover of index map
};

#endif

