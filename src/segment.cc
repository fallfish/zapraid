#include "segment.h"

#include <isa-l.h>
#include "raid_controller.h"


uint8_t *Segment::gEncodeMatrix = nullptr;
uint8_t *Segment::gGfTables = nullptr;

inline uint32_t round_up(uint32_t value, uint32_t align)
{
  return (value + align - 1) / align * align;
}

inline uint32_t round_down(uint32_t value, uint32_t align)
{
  return value / align * align;
}

Segment::Segment(RAIDController *raidController, uint32_t segmentId)
{
  SystemMode mode = Configuration::GetSystemMode();
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

  mRaidController = raidController;
  mZonePos = 0;
  mStripePos = 0;

  if (gEncodeMatrix == nullptr) {

    gEncodeMatrix = new uint8_t[n * k];
    gGfTables = new uint8_t[32 * n * (n - k)];
    gf_gen_rs_matrix(gEncodeMatrix, n, k);
    ec_init_tables(k, n - k, &gEncodeMatrix[k * k], gGfTables);
    printf("gGfTables: %p\n", gGfTables);
  }

  mRequestContextPool = new RequestContext[mNumRequestContext];
  for (int i = 0; i < mNumRequestContext; ++i) {
    mRequestContextPool[i].Clear();
    mRequestContextPool[i].buffer = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    mAvailableRequestContexts.emplace_back(&mRequestContextPool[i]);
  }

  if (mode == PURE_WRITE) {
    mNumInflightStripes = 1;
  }
  mStripeWriteContextPool = new StripeWriteContext[mNumInflightStripes];
  mAvailableStripeWriteContext.clear();
  mInflightStripeWriteContext.clear();
  for (int i = 0; i < mNumInflightStripes; ++i) {
    mStripeWriteContextPool[i].data = new uint8_t*[n];
    mStripeWriteContextPool[i].metadata = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    mStripeWriteContextPool[i].ioContext.clear();
    mAvailableStripeWriteContext.emplace_back(&mStripeWriteContextPool[i]);
  }

  mReadContextPool = new ReadContext[mNumInflightReads];
  mAvailableReadContext.clear();
  mInflightReadContext.clear();
  for (int i = 0; i < mNumInflightReads; ++i) {
    mReadContextPool[i].data = new uint8_t*[n];
    mReadContextPool[i].metadata = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    mReadContextPool[i].ioContext.clear();
    mAvailableReadContext.emplace_back(&mReadContextPool[i]);
  }

  mStripeMeta.data = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
  mStripeMeta.metadata = (uint8_t*)spdk_zmalloc(
      Configuration::GetStripeSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);

  // Initialize segment metadata
  mSegmentMeta.segmentId = segmentId;
  mSegmentMeta.stripeSize = Configuration::GetStripeSize();
  mSegmentMeta.stripeDataSize = Configuration::GetStripeDataSize();
  mSegmentMeta.stripeParitySize = Configuration::GetStripeParitySize();
  mSegmentMeta.n = n;
  mSegmentMeta.k = k;
  mSegmentMeta.raidScheme = (uint32_t)Configuration::GetRaidScheme();
  mSegmentMeta.numZones = 0;

  mNumBlocks = 0;
  mNumInvalidBlocks = 0;
  mCapacity = Configuration::GetZoneCapacity();
  mValidBits = new bool[mSegmentMeta.n * mCapacity];
  memset(mValidBits, 0, mSegmentMeta.n * mCapacity * sizeof(bool));
  mBlockMetadata = new BlockMetadata[mSegmentMeta.n * mCapacity];

  // Adjust the capacity for user data + stripe metadata
  // and the L2P table information at the end of the segment
  {
    // each block needs (LBA + timestamp, 16 bytes) for L2P table recovery, and round the number to block size
    mP2LTableSize = round_up(mCapacity * 16, Configuration::GetBlockSize());
    mInternalCapacity = round_down(mCapacity - mP2LTableSize / Configuration::GetBlockSize(), Configuration::GetSyncGroupSize());
  }
  printf("P2LTable: %u, InternalCap: %u\n", mP2LTableSize, mInternalCapacity);

  mSegmentStatus = SEGMENT_NORMAL;
}

Segment::~Segment()
{
  // TODO: reclaim zones to devices
  for (int i = 0; i < mNumRequestContext; ++i) {
    spdk_free(mRequestContextPool[i].buffer);
  }

  for (int i = 0; i < mNumInflightStripes; ++i) {
    spdk_free(mStripeWriteContextPool[i].metadata);
  }
  delete mStripeWriteContextPool;
  for (int i = 0; i < mNumInflightReads; ++i) {
    spdk_free(mReadContextPool[i].metadata);
  }
  spdk_free(mStripeMeta.data);
  spdk_free(mStripeMeta.metadata);
  delete mReadContextPool;
  delete mValidBits;
  delete mBlockMetadata;
}

RequestContext *Segment::getRequestContext()
{
  RequestContext *ctx = nullptr;
  if (mAvailableRequestContexts.empty()) {
    recycleStripeWriteContexts();
    recycleReadContexts();
  }

  if (!mAvailableRequestContexts.empty()) {
    ctx = mAvailableRequestContexts.back();
    mAvailableRequestContexts.pop_back();
    ctx->Clear();
    ctx->available = false;
  } else {
    ctx = new RequestContext();
    ctx->buffer = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
    ctx->Clear();
    ctx->available = false;
    printf("RequestContext runs out!\n");
  }
  return ctx;
}

void Segment::returnRequestContext(RequestContext *slot)
{
  assert(slot->available);
  if (slot < mRequestContextPool || slot >= mRequestContextPool + mNumRequestContext) {
    spdk_free(slot->buffer);
    delete slot;
  } else {
    assert(mAvailableRequestContexts.size() <= mNumRequestContext);
    mAvailableRequestContexts.emplace_back(slot);
  }
}

void Segment::recycleStripeWriteContexts()
{
  for (auto it = mInflightStripeWriteContext.begin();
      it != mInflightStripeWriteContext.end();
      ) {
    if (checkStripeAvailable(*it, true)) {
      assert((*it)->ioContext.empty());
      mAvailableStripeWriteContext.emplace_back(*it);
      it = mInflightStripeWriteContext.erase(it);
    } else {
      ++it;
    }
  }
}

void Segment::recycleReadContexts()
{
  for (auto it = mInflightReadContext.begin();
      it != mInflightReadContext.end();
      ) {
    if (checkReadAvailable(*it)) {
      mAvailableReadContext.emplace_back(*it);
      it = mInflightReadContext.erase(it);
    } else {
      ++it;
    }
  }
}

void Segment::AddZone(Zone *zone)
{
  mSegmentMeta.zones[mZones.size()] = zone->GetSlba();
  mSegmentMeta.numZones += 1;
  mZones.emplace_back(zone);
}

const std::vector<Zone*>& Segment::GetZones()
{
  return mZones;
}

uint64_t Segment::GetCapacity() const
{
  return mCapacity;
}

uint64_t Segment::GetNumBlocks() const
{
  return mNumBlocks;
}

uint64_t Segment::GetNumInvalidBlocks() const
{
  return mNumInvalidBlocks;
}

bool Segment::IsFull()
{
  return mZonePos == mInternalCapacity;
}

bool Segment::CanSeal()
{
  return mZonePos == mInternalCapacity + mP2LTableSize / Configuration::GetBlockSize();
}

void Segment::PrintStats()
{
  printf("Zone group position: %d, capacity: %d, num invalid blocks: %d\n", mZonePos, mCapacity, mNumInvalidBlocks);
  for (auto zone : mZones) {
    zone->PrintStats();
  }
}

void Segment::recycleContexts()
{
  recycleStripeWriteContexts();
  recycleReadContexts();
}

bool Segment::checkReadAvailable(ReadContext *readContext)
{
  bool isAvailable = true;
  for (auto ctx : readContext->ioContext) {
    if (!ctx->available) {
      isAvailable = false;
    }
  }
  if (isAvailable) {
    for (auto ctx : readContext->ioContext) {
      returnRequestContext(ctx);
    }
    readContext->ioContext.clear();
  }
  return isAvailable;
}

bool Segment::checkStripeAvailable(
    StripeWriteContext *stripeContext,
    bool recycleContexts)
{
  bool isAvailable = true;

  for (auto slot : stripeContext->ioContext) {
    isAvailable = slot->available ? isAvailable : false;
  }

  if (isAvailable && recycleContexts) {
    for (auto slot : stripeContext->ioContext) {
      returnRequestContext(slot);
    }
    stripeContext->ioContext.clear();
  }

  return isAvailable;
}

SegmentStatus Segment::GetStatus()
{
  return mSegmentStatus;
}

// StateTransition must be called in the same thread
// as Append()
bool Segment::StateTransition()
{
  bool stateChanged = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    if (IsFull()) {
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
//      mSegmentStatus = SEGMENT_SEALED;
    } else {
      return stateChanged;
    }
  }

  if (mSegmentStatus == SEGMENT_PREPARE_STRIPE_META) {
    // under SyncPoint, waiting for previous appends
    recycleStripeWriteContexts();
    if (mInflightStripeWriteContext.empty()) {
      if (Configuration::GetSystemMode() == ZNS_RAID_WITH_META
          || Configuration::GetSystemMode() == ZNS_RAID_WITH_REDIRECTION) {
        issueSyncPoint();
        mSegmentStatus = SEGMENT_WRITING_STRIPE_META;
      } else {
        mSegmentStatus = SEGMENT_NORMAL;
      }
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_STRIPE_META) {
    if (Configuration::GetSystemMode() == ZNS_RAID_WITH_META
        || Configuration::GetSystemMode() == ZNS_RAID_WITH_REDIRECTION) {
      if (isSyncPointDone()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_NORMAL;
      }
    } else if (Configuration::GetSystemMode() == ZNS_RAID_NO_META) {
      recycleStripeWriteContexts();
      if (mInflightStripeWriteContext.empty()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_NORMAL;
      }
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_HEADER) {
    // wait for finalizing the header
    recycleStripeWriteContexts();
    if (mInflightStripeWriteContext.empty()) {
      stateChanged = true;
      mSegmentStatus = SEGMENT_NORMAL;
    }
  } else if (mSegmentStatus == SEGMENT_PREPARE_FOOTER) {
    // wait for finalizing the footer
    recycleStripeWriteContexts();
    if (mInflightStripeWriteContext.empty()) {
      // prepare the stripe for writing the footer
      findStripe();
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;
      for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
        RequestContext *slot = getRequestContext();
        slot->data = slot->buffer;
        slot->meta = mCurStripe->metadata + i * Configuration::GetMetadataSize();
        slot->targetBytes = 4096;
        slot->type = STRIPE_UNIT;
        slot->segment = this;
        slot->ctrl = mRaidController;
        slot->lba = ~0ull;
        slot->associatedRequest = nullptr;
        slot->available = true;

        mCurStripe->data[i] = (uint8_t*)slot->data;
        mCurStripe->ioContext.emplace_back(slot);

        stateChanged = true;
        mSegmentStatus = SEGMENT_WRITING_FOOTER;
      }
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_FOOTER) {
    if (checkStripeAvailable(mCurStripe, false)) {
      if (CanSeal()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_SEALING;
        Seal();
      } else {
        progressFooterWriter();
      }
    }
  } else if (mSegmentStatus == SEGMENT_SEALING) {
    if (checkStripeAvailable(mCurStripe, true)) {
      stateChanged = true;
      mSegmentStatus = SEGMENT_SEALED;
    }
  }
  return stateChanged;
}

void Segment::progressFooterWriter()
{
  uint32_t begin = (mZonePos - mInternalCapacity) * Configuration::GetBlockSize() / 16;
  uint32_t end = begin + Configuration::GetBlockSize() / 16;
  for (uint32_t zid = 0; zid < mSegmentMeta.numZones; ++zid) {
    BlockMetadata *blockMetadata = &mBlockMetadata[zid * mCapacity];
    uint64_t *footerBlock = reinterpret_cast<uint64_t*>(mCurStripe->data[zid]);
    uint32_t pos = 0;
    for (uint32_t offset = begin; offset < end; ++offset) {
      footerBlock[pos++] = blockMetadata[offset].d.lba;
      footerBlock[pos++] = blockMetadata[offset].d.timestamp;
    }
  }
  encodeStripe(mCurStripe->data, mSegmentMeta.n, mSegmentMeta.k);
  mCurStripe->successBytes = 0;

  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        mZonePos, i, (RAIDScheme)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
    RequestContext *slot = mCurStripe->ioContext[i];
    slot->status = WRITE_REAPING;
    slot->successBytes = 0;
    slot->offset = mZonePos;
    slot->available = false;
    slot->zoneId = zoneId;
    mZones[zoneId]->Write(mZonePos, Configuration::GetBlockSize(), (void*)slot);
  }
  mZonePos += 1;
}


bool Segment::findStripe()
{
  bool accept = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    // Under normal appends, proceed
    if (mAvailableStripeWriteContext.empty()) {
      recycleContexts();
      if (mAvailableStripeWriteContext.empty()) {
        accept = false;
      }
    }

    if (!mAvailableStripeWriteContext.empty()) {
      mCurStripe = mAvailableStripeWriteContext.back(); //  printf("Get %p\n", mCurStripe);
      mAvailableStripeWriteContext.pop_back();
      mInflightStripeWriteContext.emplace_back(mCurStripe);
      mCurStripe->successBytes = 0;
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;

      for (int j = 0; j < Configuration::GetStripeParitySize() / Configuration::GetStripeUnitSize(); ++j) {
        RequestContext *context = getRequestContext();
        context->associatedStripe = mCurStripe;
        context->targetBytes = Configuration::GetStripeParitySize();
        context->data = context->buffer;
        mCurStripe->ioContext.emplace_back(context);
        context->segment = this;
      }
      accept = true;
    }
  }
  return accept;
}

bool Segment::Append(RequestContext *ctx, uint32_t offset)
{
  if (mStripePos == 0) {
    if (!findStripe()) {
      return false;
    }
  }
  printf("Write\n");

  SystemMode mode = Configuration::GetSystemMode();

  uint64_t lba = ~0ull;
  uint8_t *blkdata = nullptr;
  uint32_t whichBlock = mStripePos / Configuration::GetBlockSize();

  if (ctx != nullptr) {
    lba = ctx->lba + offset;
    blkdata = (uint8_t*)(ctx->data) + offset;
  }

  uint32_t zoneId = Configuration::CalculateDiskId(
      mZonePos, whichBlock,
      (RAIDScheme)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
  // Issue data block
  {
    RequestContext *slot = getRequestContext();
    mCurStripe->ioContext.emplace_back(slot);
    mCurStripe->data[mStripePos / Configuration::GetBlockSize()] = blkdata;
    slot->associatedStripe = mCurStripe;
    slot->associatedRequest = ctx;
    slot->targetBytes = Configuration::GetBlockSize();
    slot->lba = lba;
    slot->zoneId = zoneId;
    slot->stripeId = mZonePos % Configuration::GetSyncGroupSize();
    slot->segment = this;
    slot->ctrl = mRaidController;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->data = blkdata;
    slot->meta = mCurStripe->metadata + whichBlock * Configuration::GetMetadataSize();
    if (mode == PURE_WRITE) {
      slot->append = false;
      slot->offset = mZonePos;
    } else {
      slot->append = true;
      slot->offset = mZonePos | (Configuration::GetSyncGroupSize() - 1);
    }
    mZones[zoneId]->Write(mZonePos, Configuration::GetStripeUnitSize(), (void*)slot);
  }

  mStripePos += Configuration::GetBlockSize();
  if (mStripePos == mSegmentMeta.stripeDataSize) {
  // issue parity block
    uint32_t n = mSegmentMeta.n;
    uint32_t k = mSegmentMeta.k;

    for (uint32_t i = k; i < n; ++i) {
      mCurStripe->data[i] = mCurStripe->ioContext[i - k]->data;
    }
    encodeStripe(mCurStripe->data, n, k);
    
    for (uint32_t i = k; i < n; ++i) {
      uint32_t zoneId = Configuration::CalculateDiskId(
          mZonePos, i, 
          (RAIDScheme)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
      RequestContext *slot = mCurStripe->ioContext[i - k];
      slot->lba = ~0ull;
      slot->ctrl = mRaidController;
      slot->segment = this;
      slot->zoneId = zoneId;
      slot->stripeId = mZonePos % Configuration::GetSyncGroupSize();
      slot->status = WRITE_REAPING;
      slot->type = STRIPE_UNIT;
      slot->meta = mCurStripe->metadata + i * Configuration::GetMetadataSize();
      if (mode == PURE_WRITE) {
        slot->append = false;
        slot->offset = mZonePos;
      } else {
        slot->append = true;
        slot->offset = mZonePos | (Configuration::GetSyncGroupSize() - 1);
      }
      mZones[zoneId]->Write(mZonePos, Configuration::GetBlockSize(), slot);

      mStripePos += Configuration::GetBlockSize();
    }
    mStripePos = 0;
    mZonePos += 1;

    if ((mode == ZNS_RAID_NO_META || mode == ZNS_RAID_WITH_META || mode == ZNS_RAID_WITH_REDIRECTION)
        && needSyncPoint()) {
      // writing the stripe metadata at the end of each group
      mSegmentStatus = SEGMENT_PREPARE_STRIPE_META;
    }

    if (mode == ZNS_RAID_NO_META && mZonePos % Configuration::GetSyncGroupSize() == 0) {
      mSegmentStatus = SEGMENT_WRITING_STRIPE_META;
    }

    if (mZonePos == mInternalCapacity) {
      // writing the P2L table at the end of the segment
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    }
  }

  return true;
}

bool Segment::Read(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr)
{
  if (mAvailableReadContext.empty()) {
    recycleContexts();
    if (mAvailableReadContext.empty()) {
      return false;
    }
  }
  printf("Read\n");

  ReadContext *readContext = mAvailableReadContext.back();
  mAvailableReadContext.pop_back();
  mInflightReadContext.emplace_back(readContext);

  RequestContext *slot = getRequestContext();
  slot->associatedRead = readContext;
  slot->available = false;
  slot->associatedRequest = ctx;
  slot->lba = ctx->lba + pos;
  slot->targetBytes = Configuration::GetBlockSize();
  slot->zoneId = phyAddr.zoneId;
  slot->stripeId = phyAddr.stripeId;
  slot->offset = phyAddr.offset;
  slot->ctrl = mRaidController;
  slot->segment = this;
  slot->status = READ_REAPING;
  slot->type = STRIPE_UNIT;
  slot->data = (uint8_t*)ctx->data + pos;
  slot->meta = readContext->metadata;

  readContext->data[readContext->ioContext.size()] = (uint8_t*)slot->data;
  readContext->ioContext.emplace_back(slot);

  slot->needDegradedRead = Configuration::GetEnableDegradedRead();
  if (slot->needDegradedRead) {
    slot->Queue();
  } else {
    uint32_t zoneId = slot->zoneId;
    uint32_t offset = slot->offset;
    mZones[zoneId]->Read(offset, Configuration::GetStripeUnitSize(), slot);
  }

  return true;
}

bool Segment::ReadValid(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr, bool *isValid)
{
  bool success = false;
  uint32_t zoneId = phyAddr.zoneId;
  uint32_t offset = phyAddr.offset;

  if (mValidBits[zoneId * mCapacity + offset] == 0) {
    *isValid = false;
    success = true;
  } else {
    *isValid = true;
    success = Read(ctx, pos, phyAddr);
  }
  return success;
}

void Segment::encodeStripe(uint8_t **stripe, uint32_t n, uint32_t k)
{
  uint8_t *input[k];
  uint8_t *output[n - k];
  for (int i = 0; i < k; ++i) {
    input[i] = stripe[i];
  }
  for (int i = 0; i < n - k; ++i) {
    output[i] = stripe[k + i];
  }
  ec_encode_data(Configuration::GetBlockSize(), k, n - k, gGfTables, input, output);
}

void Segment::decodeStripe(uint32_t offset, uint8_t **stripe, bool *alive, uint32_t n, uint32_t k, uint32_t decodeZid)
{
  uint8_t *input[k];
  uint8_t *output[1];
  uint8_t decodeGfTbl[32 * n * (n - k)];
  uint8_t recoverMatrix[k * k];
  uint8_t invRecoverMatrix[k * k];
  uint8_t decodeMatrix[n * k]; // [k:k] to [n:k] stores coefficients for the to-decode chunk part
  memset(decodeGfTbl, 0, sizeof(decodeGfTbl));
  memset(recoverMatrix, 0, sizeof(recoverMatrix));
  memset(invRecoverMatrix, 0, sizeof(invRecoverMatrix));
  memset(decodeMatrix, 0, sizeof(decodeMatrix));

  uint32_t mapping[n];
  uint32_t decodeIndex;
  for (uint32_t i = 0; i < n; ++i) {
    uint32_t zid = Configuration::CalculateDiskId(
          offset, i, (RAIDScheme)mSegmentMeta.raidScheme,
          mSegmentMeta.numZones);
    if (alive[zid]) {
      mapping[i] = zid;
    } else {
      mapping[i] = ~0u;
    }
    if (zid == decodeZid) {
      decodeIndex = i;
    }
  }

  for (uint32_t i = 0, j = 0; i < n; ++i) {
    if (mapping[i] == ~0u) continue; // alive[i] == false must implies that i is not decodeIndex
    if (j == k) break;
    memcpy(recoverMatrix + j * k,
           gEncodeMatrix + i * k,
           k * sizeof(uint8_t));
    j++;
  }
  gf_invert_matrix(recoverMatrix, invRecoverMatrix, k);

  for (uint32_t i = 0; i < k; ++i) {
    decodeMatrix[i * k + i] = 1;
  }

  if (decodeIndex < k) { // a data block need decoding
    memcpy(decodeMatrix + (0 + k) * k, invRecoverMatrix + decodeIndex * k, k * sizeof(uint8_t));
  } else { // a parity block need decoding
    for (uint32_t col = 0; col < k; ++col) {
      uint8_t s = 0;
      for (uint32_t row = 0; row < k; ++row) {
        s ^= gf_mul(invRecoverMatrix[row * k + col],
                    gEncodeMatrix[k * decodeIndex + row]);
      }
      decodeMatrix[(0 + k) * k + col] = s;
    }
  }
  ec_init_tables(k, 1, &decodeMatrix[k * k], decodeGfTbl);

  for (uint32_t i = 0, j = 0, l = 0; i < n; ++i) {
    if (i == decodeIndex) {
      output[l] = stripe[decodeZid];
      l++;
    } else if (mapping[i] != ~0u) {
      input[j] = stripe[mapping[i]];
      j++;
    }
  }
  ec_encode_data(Configuration::GetBlockSize(), k, 1, decodeGfTbl, input, output);
}

void Segment::UpdateSyncPoint(uint32_t zoneId, uint32_t stripeId, uint32_t offset)
{
  uint32_t syncGroupSize = Configuration::GetSyncGroupSize();
  uint16_t *mapping = (uint16_t*)mStripeMeta.data + zoneId * syncGroupSize;
  mapping[offset % syncGroupSize] = stripeId;
}

void Segment::issueSyncPoint()
{
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  uint8_t *stripe[n];
  for (uint32_t i = 0; i < n; ++i) {
    stripe[i] = mStripeMeta.data + i * Configuration::GetBlockSize();
  }
  encodeStripe(stripe, n, k);
  for (int i = 0; i < n; ++i) {
    uint32_t zid = Configuration::CalculateDiskId(
        mZonePos, i, (RAIDScheme)mSegmentMeta.raidScheme, mSegmentMeta.n);
    mStripeMeta.slots[i].Clear();
    mStripeMeta.slots[i].available = false;
    mStripeMeta.slots[i].targetBytes = Configuration::GetBlockSize();
    mStripeMeta.slots[i].append = false;
    mStripeMeta.slots[i].ctrl = mRaidController;
    mStripeMeta.slots[i].segment = this;
    mStripeMeta.slots[i].zoneId = zid;
    mStripeMeta.slots[i].stripeId = mZonePos % Configuration::GetSyncGroupSize();
    mStripeMeta.slots[i].offset = mZonePos;
    mStripeMeta.slots[i].type = STRIPE_UNIT;
    mStripeMeta.slots[i].status = WRITE_REAPING;
    mStripeMeta.slots[i].lba = ~0ull;
    mStripeMeta.slots[i].data = mStripeMeta.data + i * Configuration::GetBlockSize();
    mStripeMeta.slots[i].meta = mStripeMeta.metadata + i * Configuration::GetMetadataSize();
    BlockMetadata *meta = (BlockMetadata*)mStripeMeta.slots[i].meta;
    meta->d.lba = ~0ull;
    meta->d.stripeId = Configuration::GetSyncGroupSize() - 1;
    mZones[zid]->Write(mZonePos, Configuration::GetBlockSize(), &mStripeMeta.slots[i]);
  }
  mZonePos += 1;
}

bool Segment::needSyncPoint()
{
  return (mZonePos + 1) % Configuration::GetSyncGroupSize() == 0;
}

bool Segment::isSyncPointDone()
{
  bool done = true;
  for (int i = 0; i < Configuration::GetStripeSize() / Configuration::GetStripeUnitSize(); ++i) {
    if (!mStripeMeta.slots[i].available) {
       done = false;
    }
  }
  return done;
}

void Segment::Reset(RequestContext *ctx)
{
  mResetContext.clear();
  for (int i = 0; i < mZones.size(); ++i) {
    mResetContext.emplace_back(RequestContext());
    RequestContext *context = &mResetContext.back();
    context->Clear();
    context->associatedRequest = ctx;
    context->ctrl = mRaidController;
    context->type = STRIPE_UNIT;
    context->status = RESET_REAPING;
    mZones[i]->Reset(context);
  }
}

void Segment::Seal()
{
  for (int i = 0; i < mZones.size(); ++i) {
    RequestContext *context = mCurStripe->ioContext[i];
    context->Clear();
    context->available = false;
    context->ctrl = mRaidController;
    context->segment = this;
    context->type = STRIPE_UNIT;
    context->status = FINISH_REAPING;
    mZones[i]->Seal(context);
  }
}

void Segment::ReadStripeMeta(RequestContext *ctx)
{
  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

  ReadContext *readContext = ctx->associatedRead;

  uint32_t syncPointOffset = offset | (Configuration::GetSyncGroupSize() - 1);
  ctx->successBytes = 0;

  if (syncPointOffset == (mZonePos | (Configuration::GetSyncGroupSize() - 1))) {
    ctx->needDecodeMeta = false;
    ctx->targetBytes = n * Configuration::GetBlockSize();

    for (uint32_t i = 0; i < n; ++i) {
      RequestContext *reqCtx = getRequestContext();
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetBlockSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->segment = this;
      reqCtx->zoneId = 0;
      reqCtx->offset = syncPointOffset;
      reqCtx->data = reqCtx->buffer;
      reqCtx->meta = (uint8_t*)readContext->metadata + readContext->ioContext.size() * Configuration::GetMetadataSize();

      readContext->data[i] = reqCtx->data;
      readContext->ioContext.emplace_back(reqCtx);

      memcpy(reqCtx->data, mStripeMeta.data, Configuration::GetBlockSize());
      reqCtx->successBytes = reqCtx->targetBytes;
      reqCtx->Queue();
    }
  } else {
    ctx->needDecodeMeta = true;
    ctx->targetBytes = k * Configuration::GetBlockSize();
    uint32_t cnt = 0;
    for (int i = 0; i < n && cnt < k; ++i) {
      if (i == zoneId) continue;
      RequestContext *reqCtx = getRequestContext();
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetBlockSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->segment = this;
      reqCtx->zoneId = i;
      reqCtx->offset = syncPointOffset;
      reqCtx->data = reqCtx->buffer;
      reqCtx->meta = (uint8_t*)readContext->metadata + readContext->ioContext.size() * Configuration::GetMetadataSize();

      readContext->data[i] = reqCtx->data;
      readContext->ioContext.emplace_back(reqCtx);

      mZones[i]->Read(syncPointOffset, Configuration::GetBlockSize(), reqCtx);

      cnt++;
    }
  }
}

void Segment::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;

  if (mode == PURE_WRITE || mode == ZNS_RAID_WITH_META || mode == ZNS_RAID_WITH_REDIRECTION) {
    uint32_t realOffsets[n];
    ctx->targetBytes = k * Configuration::GetStripeUnitSize();
    ctx->successBytes = 0;
    if (mode == PURE_WRITE) { // PURE_WRITE
      for (uint32_t i = 0; i < n; ++i) {
        realOffsets[i] = offset;
      }
    } else if (mode == ZNS_RAID_WITH_META || mode == ZNS_RAID_WITH_REDIRECTION) {
      // decode metadata
      uint16_t offsetMap[Configuration::GetStripeSize() / 2];

      // metadata stripe
      if (ctx->needDecodeMeta) {
        bool alive[n];
        for (uint32_t i = 0; i < n; ++i) {
          alive[i] = false;
        }
        for (uint32_t i = 1; i < 1 + k; ++i) {
          uint32_t zid = readContext->ioContext[i]->zoneId;
          alive[zid] = true;
        }
        readContext->data[zoneId] = ctx->data;
        decodeStripe(offset, readContext->data, alive, n, k, zoneId);
        if (offset == 8) {
          b();
        }
        uint32_t metaOffset = offset | (Configuration::GetSyncGroupSize() - 1);
        for (uint32_t i = 0; i < n; ++i) {
          uint32_t zid = Configuration::CalculateDiskId(
              metaOffset, i, (RAIDScheme)mSegmentMeta.raidScheme, mSegmentMeta.n);
          memcpy((uint8_t*)offsetMap + i * Configuration::GetBlockSize(),
              readContext->data[zid], Configuration::GetBlockSize());
        }
      } else {
        for (uint32_t i = 0; i < n; ++i) {
          memcpy((uint8_t*)offsetMap + i * Configuration::GetBlockSize(),
              readContext->data[i], Configuration::GetBlockSize());
        }
      }
      // offset to stripe ID
      uint32_t stripeId = (offsetMap + zoneId * Configuration::GetSyncGroupSize())[offset % Configuration::GetSyncGroupSize()];
      uint32_t offsetBegin = offset & (~(Configuration::GetSyncGroupSize() - 1));

      for (uint32_t i = 0; i < n; ++i) {
        uint16_t *offsetMapCurZone = offsetMap + i * Configuration::GetSyncGroupSize();
        for (uint32_t j = 0; j < Configuration::GetSyncGroupSize() - 1; ++j) {
          if (offsetMapCurZone[j] == stripeId) {
            realOffsets[i] = offsetBegin + j;
          }
        }
      }
    }

    uint32_t cnt = 0;
    for (uint32_t i = 0; i < n && cnt < k; ++i) {
      if (i == zoneId) continue;
      RequestContext *reqCtx = nullptr;
      if (Configuration::GetSystemMode() == PURE_WRITE) {
        reqCtx = getRequestContext();
        readContext->ioContext.emplace_back(reqCtx);
      } else {
        reqCtx = readContext->ioContext[1 + cnt];
      }
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetStripeUnitSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->segment = this;
      reqCtx->zoneId = i;
      reqCtx->offset = realOffsets[i];
      reqCtx->data = reqCtx->buffer;
      reqCtx->meta = (uint8_t*)readContext->metadata + readContext->ioContext.size() * Configuration::GetMetadataSize();

      readContext->data[i] = reqCtx->data;

      mZones[i]->Read(realOffsets[i], Configuration::GetStripeUnitSize(), reqCtx);

      ++cnt;
    }
  } else if (mode == ZNS_RAID_NO_META) {
    // Note that the sync group size must be small to prevent from overwhelming the storage
    ctx->targetBytes = k * Configuration::GetStripeUnitSize() * Configuration::GetSyncGroupSize();
    ctx->successBytes = 0;
    uint32_t offsetBegin = offset & (~(Configuration::GetSyncGroupSize() - 1));
    uint32_t offsetEnd = offsetBegin + Configuration::GetSyncGroupSize();

    for (uint32_t i = 0; i < n; ++i) {
      if (i == zoneId) continue;
      for (uint32_t offset = offsetBegin; offset < offsetEnd; ++offset) {
        RequestContext *reqCtx = getRequestContext();
        reqCtx->Clear();
        reqCtx->associatedRequest = ctx;
        reqCtx->status = DEGRADED_READ_SUB;
        reqCtx->type = STRIPE_UNIT;
        reqCtx->targetBytes = Configuration::GetStripeUnitSize();
        reqCtx->ctrl = mRaidController;
        reqCtx->segment = this;
        reqCtx->zoneId = i;
        reqCtx->offset = offset;
        reqCtx->data = reqCtx->buffer;
        reqCtx->meta = (uint8_t*)readContext->metadata + readContext->ioContext.size() * Configuration::GetMetadataSize();

        readContext->ioContext.emplace_back(reqCtx);
        mZones[i]->Read(offset, Configuration::GetStripeUnitSize(), reqCtx);
      }
    }
  }

  if (mode == PURE_ZONE_APPEND) {
    fprintf(stderr, "Pure zone append does not support recovery.\n");
    exit(-1);
  }
}

void Segment::WriteComplete(RequestContext *ctx)
{
  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = &mBlockMetadata[ctx->zoneId * mCapacity + ctx->offset];
    meta->d.timestamp = ctx->timestamp;
    meta->d.lba = ctx->lba;
    meta->d.stripeId = ctx->stripeId;
  }
}

void Segment::ReadComplete(RequestContext *ctx)
{
  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t n = Configuration::GetStripeSize() / blockSize;
  uint32_t k = Configuration::GetStripeDataSize() / blockSize;
  RequestContext *parent = ctx->associatedRequest;

  ReadContext *readContext = ctx->associatedRead;
  if (ctx->status == READ_REAPING) {
    memcpy((uint8_t*)parent->data + ctx->lba - parent->lba,
            ctx->data, Configuration::GetStripeUnitSize());
  } else if (ctx->status == DEGRADED_READ_REAPING) {
    bool alive[n];
    for (uint32_t i = 0; i < n; ++i) {
      alive[i] = false;
    }

    if (Configuration::GetSystemMode() == ZNS_RAID_NO_META) {
      for (uint32_t i = 1; i < readContext->ioContext.size(); ++i) {
        RequestContext *reqCtx = readContext->ioContext[i];
        BlockMetadata *metadata = (BlockMetadata*)reqCtx->meta;
        if (metadata->d.stripeId == ctx->stripeId) {
          alive[reqCtx->zoneId] = true;
          readContext->data[reqCtx->zoneId] = reqCtx->data;
        }
      }
    } else {
      for (uint32_t i = 1; i < 1 + k; ++i) {
        uint32_t zid = readContext->ioContext[i]->zoneId;
        alive[zid] = true;
      }
    }

    readContext->data[ctx->zoneId] = ctx->data;
    decodeStripe(offset, readContext->data, alive, n, k, zoneId);
  }

  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = (BlockMetadata*)ctx->meta;
    meta->d.lba = mBlockMetadata[zoneId * mCapacity + offset].d.lba;
    meta->d.stripeId = mBlockMetadata[zoneId * mCapacity + offset].d.stripeId;
    meta->d.timestamp = mBlockMetadata[zoneId * mCapacity + offset].d.stripeId;
  }
  if (parent->type == GC && parent->meta) {
    uint32_t offsetInBlks = (ctx->lba - parent->lba) / Configuration::GetBlockSize();
    BlockMetadata *result = (BlockMetadata*)(ctx->meta);
    BlockMetadata *parentMeta = &((BlockMetadata*)parent->meta)[offsetInBlks];
    parentMeta->d.lba = result->d.lba;
    parentMeta->d.stripeId = result->d.stripeId;
    parentMeta->d.timestamp = result->d.timestamp;
  }
}

uint32_t Segment::GetZoneSize()
{
  return mCapacity;
}

void Segment::InvalidateBlock(uint32_t zoneId, uint32_t realOffset)
{
  assert(mNumInvalidBlocks < mNumBlocks);
  mNumInvalidBlocks += 1;
  mValidBits[zoneId * mCapacity + realOffset] = false;
}

void Segment::FinishBlock(uint32_t zoneId, uint32_t offset, uint64_t lba)
{
  if (lba != ~0ull) {
    mNumBlocks += 1;
    mValidBits[zoneId * mCapacity + offset] = true;
  }
}

void Segment::ReleaseZones()
{
  for (auto zone : mZones) {
    zone->Release();
  }
}

void Segment::FlushStripe()
{
  if (mStripePos == 0) return;
  SystemMode mode = Configuration::GetSystemMode();

  for ( ; mStripePos < Configuration::GetStripeDataSize();
      mStripePos += Configuration::GetBlockSize()) {
    Append(nullptr, 0);
  }
}

bool Segment::CheckOutstandingWrite()
{
  recycleStripeWriteContexts();
  return mInflightStripeWriteContext.size() != 0;
}

bool Segment::CheckOutstandingRead()
{
  recycleReadContexts();
  return mInflightReadContext.size() != 0;
}

void Segment::FinalizeSegmentHeader()
{
  StripeWriteContext *stripe = mAvailableStripeWriteContext.back();
  mAvailableStripeWriteContext.pop_back();
  mInflightStripeWriteContext.emplace_back(stripe);
  stripe->successBytes = 0;
  stripe->targetBytes = mSegmentMeta.stripeSize;

  for (uint32_t i = 0; i < mZones.size(); ++i) {
    RequestContext *slot = getRequestContext();
    stripe->ioContext.emplace_back(slot);
    slot->associatedStripe = stripe;
    slot->targetBytes = Configuration::GetBlockSize();
    slot->lba = ~0ull;
    slot->zoneId = i;
    slot->stripeId = 0;
    slot->segment = this;
    slot->ctrl = mRaidController;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->data = slot->buffer;
    slot->meta = stripe->metadata + i * Configuration::GetMetadataSize();
    slot->append = false;
    slot->offset = 0;

    stripe->data[i] = slot->data;
    memcpy(stripe->data[i], &mSegmentMeta, sizeof(mSegmentMeta));

    mZones[i]->Write(0, Configuration::GetBlockSize(), (void*)slot);
  }
  mZonePos += 1;
  mCurStripe = stripe;
  mSegmentStatus = SEGMENT_WRITING_HEADER;
}

uint32_t Segment::GetSegmentId()
{
  return mSegmentMeta.segmentId;
}
