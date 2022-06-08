#ifndef __ZONE_GROUP_H__
#define __ZONE_GROUP_H__
#include "common.h"
#include "zone.h"
#include <set>
#include <vector>

class RAIDController;

class ZoneGroup
{
public:
  ZoneGroup(RAIDController* raidController);
  ~ZoneGroup();
  void AddZone(Zone *zone);

  const std::vector<Zone*>& GetZones();
  bool Append(RequestContext *ctx, uint32_t offset);
  bool Read(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr);
  bool ReadValid(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr, bool *isValid);
  void Reset();
  void Seal();
  uint64_t GetCapacity() const;
  uint64_t GetNumBlocks() const;
  uint64_t GetNumInvalidBlocks() const;
  bool IsFull();
  void Drain();
  uint32_t GetZoneSize();

  void ReadStripeMeta(RequestContext *ctx);
  void ReadStripe(RequestContext *ctx);

  void WriteComplete(RequestContext *ctx);
  void ReadComplete(RequestContext *ctx);

  void UpdateSyncPoint(uint32_t zoneId, uint32_t assignedOffset, uint32_t realOffset);

  void InvalidateBlock(uint32_t zoneId, uint32_t realOffset);
  void FinishBlock(uint32_t zoneId, uint32_t realOffset, uint64_t lba);
  void PrintStats();

  void Assert();
  void ReleaseZones();
  void FlushStripe();
private:
  uint32_t mNumInflightStripes = 64;
  struct StripeWriteContext *mStripeWriteContextPool;
  std::vector<StripeWriteContext*> mAvailableStripeWriteContext;
  std::vector<StripeWriteContext*> mInflightStripeWriteContext;
  StripeWriteContext *mCurStripeWriteContext;

  uint32_t mNumInflightReads = 64;
  struct ReadContext *mReadContextPool;

  std::vector<ReadContext*> mAvailableReadContext;
  std::vector<ReadContext*> mInflightReadContext;
  ReadContext *mCurReadContext;

  uint32_t mNumRequestContext = 512;
  RequestContext *mRequestContextPool;
  std::vector<RequestContext*> mAvailableRequestContexts;

  void degradedRead(RequestContext *ctx, PhysicalAddr phyAddr);

  void handleStripeWriteContextCompletions();
  void handleReadContextCompletions();
  void handleRequestCompletions();
  RequestContext* getRequestContext();
  void returnRequestContext(RequestContext *slot);
  bool checkStripeAvailable(StripeWriteContext *stripeContext);
  bool checkReadAvailable(ReadContext *stripeContext);
  bool findStripe(RequestContext *ctx);
  void encodeStripe(uint8_t *stripe, uint32_t n, uint32_t k);
  void decodeStripe(uint8_t *stripe, uint32_t n, uint32_t k, uint32_t decodeIndex);

  void issueSyncPoint();
  bool needSyncPoint();
  bool isSyncPointDone();
  
  bool *mValidBits;
  BlockMetadata *mBlockMetadata; // if the backend support _with_md commands, then this is not used.
  std::vector<Zone*> mZones;
  std::vector<RequestContext> mResetContext;
  uint32_t mZonePos;
  uint32_t mCapacity; //  = 1024 * 1024 * 1024ull / gBlockSize;
  uint32_t mNumInvalidBlocks;
  uint32_t mNumBlocks;

  uint32_t mStripePos;
  uint32_t mCurStripeId;

  static uint8_t *gEncodeMatrix;
  static uint8_t *gGfTables;

  RAIDController *mRaidController;

  uint32_t mSyncGroupStatus;
  uint32_t mSyncGroupSize = 128;
  SyncPoint mSyncPoint;
};

#endif

