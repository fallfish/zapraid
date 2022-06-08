#include "zone_group.h"

#include <isa-l.h>
#include "raid_controller.h"


uint8_t *ZoneGroup::gEncodeMatrix = nullptr;
uint8_t *ZoneGroup::gGfTables = nullptr;

ZoneGroup::ZoneGroup(RAIDController *raidController)
{
  SystemMode mode = Configuration::GetSystemMode();

  mRaidController = raidController;
  mZonePos = 0;
  mStripePos = 0;

  if (gEncodeMatrix == nullptr) {
    int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
    int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

    gEncodeMatrix = new uint8_t[n * k];
    gGfTables = new uint8_t[32 * n * (n - k)];
    gf_gen_rs_matrix(gEncodeMatrix, n, k);
    ec_init_tables(k, n - k, &gEncodeMatrix[k * k], gGfTables);
    printf("gGfTables: %p\n", gGfTables);
  }

  mRequestContextPool = new RequestContext[mNumRequestContext];
  for (int i = 0; i < mNumRequestContext; ++i) {
    mRequestContextPool[i].Clear();
    mAvailableRequestContexts.emplace_back(&mRequestContextPool[i]);
  }

  if (mode == PURE_WRITE) {
    mNumInflightStripes = 1;
  }
  mStripeWriteContextPool = new StripeWriteContext[mNumInflightStripes];
  mAvailableStripeWriteContext.clear();
  mInflightStripeWriteContext.clear();
  for (int i = 0; i < mNumInflightStripes; ++i) {
    mStripeWriteContextPool[i].data = (uint8_t*)spdk_zmalloc(
                                          Configuration::GetStripeSize(), 4096,
                                          NULL, SPDK_ENV_SOCKET_ID_ANY,
                                          SPDK_MALLOC_DMA);
    mStripeWriteContextPool[i].metadata = (uint8_t*)spdk_zmalloc(Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * 64,
                                          4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                                          SPDK_MALLOC_DMA);
    mStripeWriteContextPool[i].ioContext.clear();
    mAvailableStripeWriteContext.emplace_back(&mStripeWriteContextPool[i]);
  }

  mReadContextPool = new ReadContext[mNumInflightReads];
  mAvailableReadContext.clear();
  mInflightReadContext.clear();
  for (int i = 0; i < mNumInflightReads; ++i) {
    mReadContextPool[i].data = (uint8_t*)spdk_zmalloc(
                                          Configuration::GetStripeSize(), 4096,
                                          NULL, SPDK_ENV_SOCKET_ID_ANY,
                                          SPDK_MALLOC_DMA);
    mReadContextPool[i].metadata = (uint8_t*)spdk_zmalloc(Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * 64,
                                          4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                                          SPDK_MALLOC_DMA);
    mReadContextPool[i].ioContext.clear();
    mAvailableReadContext.emplace_back(&mReadContextPool[i]);
  }

  mSyncGroupStatus = 0;
  mSyncPoint.data = (uint8_t*)spdk_zmalloc(Configuration::GetStripeSize(), 4096,
                                           NULL, SPDK_ENV_SOCKET_ID_ANY,
                                           SPDK_MALLOC_DMA);
  mSyncPoint.metadata = (uint8_t*)spdk_zmalloc(Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * 64,
                                               4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                                               SPDK_MALLOC_DMA);

  mCapacity = Configuration::GetZoneCapacity();
  mValidBits = new bool[Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * mCapacity];
  memset(mValidBits, 0, Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * mCapacity * sizeof(bool));
  mBlockMetadata = new BlockMetadata[Configuration::GetStripeSize() / Configuration::GetStripeUnitSize() * mCapacity];
}

ZoneGroup::~ZoneGroup()
{
  // TODO: reclaim zones to devices
  for (int i = 0; i < mNumInflightStripes; ++i) {
    spdk_free(mStripeWriteContextPool[i].data);
    spdk_free(mStripeWriteContextPool[i].metadata);
  }
  delete mStripeWriteContextPool;
  for (int i = 0; i < mNumInflightReads; ++i) {
    spdk_free(mReadContextPool[i].data);
    spdk_free(mReadContextPool[i].metadata);
  }
  delete mReadContextPool;
  delete mValidBits;
  delete mBlockMetadata;
}

RequestContext *ZoneGroup::getRequestContext()
{
  RequestContext *ctx = nullptr;
  if (!mAvailableRequestContexts.empty()) {
    ctx = mAvailableRequestContexts.back();
    mAvailableRequestContexts.pop_back();
    ctx->Clear();
    ctx->available = false;
  } else {
    ctx = new RequestContext();
    printf("RequestContext runs out!\n");
  }
  return ctx;
}

void ZoneGroup::returnRequestContext(RequestContext *slot)
{
  assert(slot->available);
  if (slot < mRequestContextPool || slot >= mRequestContextPool + mNumRequestContext) {
    delete slot;
  } else {
    mAvailableRequestContexts.emplace_back(slot);
  }
}

void ZoneGroup::handleStripeWriteContextCompletions()
{
  for (auto it = mInflightStripeWriteContext.begin();
      it != mInflightStripeWriteContext.end();
      ) {
    if (checkStripeAvailable(*it)) {
      assert((*it)->ioContext.empty());
      mAvailableStripeWriteContext.emplace_back(*it);
      it = mInflightStripeWriteContext.erase(it);
    } else {
      ++it;
    }
  }
}

void ZoneGroup::handleReadContextCompletions()
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

void ZoneGroup::Assert()
{
  for (auto t : mAvailableReadContext) {
    assert(t >= mReadContextPool && t < mReadContextPool + mNumInflightReads);
  }
}

void ZoneGroup::Drain()
{
  while (!mInflightStripeWriteContext.empty()) {
    handleStripeWriteContextCompletions();
  }

  while (!mInflightReadContext.empty()) {
    handleReadContextCompletions();
  }
}

void ZoneGroup::AddZone(Zone *zone)
{
  mZones.emplace_back(zone);
}

const std::vector<Zone*>& ZoneGroup::GetZones()
{
  return mZones;
}

uint64_t ZoneGroup::GetCapacity() const
{
  return mCapacity;
}

uint64_t ZoneGroup::GetNumBlocks() const
{
  return mNumBlocks;
}

uint64_t ZoneGroup::GetNumInvalidBlocks() const
{
  return mNumInvalidBlocks;
}

bool ZoneGroup::IsFull()
{
  return mZonePos == mCapacity;
}

void ZoneGroup::PrintStats()
{
  printf("Zone group position: %d, capacity: %d, num invalid blocks: %d\n", mZonePos, mCapacity, mNumInvalidBlocks);
  for (auto zone : mZones) {
    zone->PrintStats();
  }
}

void ZoneGroup::handleRequestCompletions()
{
  handleStripeWriteContextCompletions();
  handleReadContextCompletions();
}

bool ZoneGroup::checkReadAvailable(ReadContext *readContext)
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

bool ZoneGroup::checkStripeAvailable(StripeWriteContext *stripeContext)
{
  bool isAvailable = true;

  for (auto slot : stripeContext->ioContext) {
    isAvailable = slot->available ? isAvailable : false;
  }

  if (isAvailable) {
    for (auto slot : stripeContext->ioContext) {
      returnRequestContext(slot);
    }
    stripeContext->ioContext.clear();
  }

  return isAvailable;
}


bool ZoneGroup::findStripe(RequestContext *ctx)
{
  if (mSyncGroupStatus == 1) {
    // under SyncPoint, waiting for previous appends
    handleStripeWriteContextCompletions();
    if (mInflightStripeWriteContext.empty()) {
      issueSyncPoint();
      mSyncGroupStatus = 2;
    }
    return false;
  } else if (mSyncGroupStatus == 2) {
    // Under SyncPoint
    if (!isSyncPointDone()) {
      return false;
    }
    mSyncGroupStatus = 0;
  }

  if (mSyncGroupStatus == 0) {
    // Under normal appends, proceed
    if (mAvailableStripeWriteContext.empty()) {
      handleRequestCompletions();
      if (mAvailableStripeWriteContext.empty()) {
        return false;
      }
    }

    mCurStripeWriteContext = mAvailableStripeWriteContext.back();
    //  printf("Get %p\n", mCurStripeWriteContext);
    mAvailableStripeWriteContext.pop_back();
    mInflightStripeWriteContext.emplace_back(mCurStripeWriteContext);
    mCurStripeWriteContext->successBytes = 0;
    mCurStripeWriteContext->targetBytes = Configuration::GetStripeSize();

    for (int j = 0; j < Configuration::GetStripeParitySize() / Configuration::GetStripeUnitSize(); ++j) {
      RequestContext *context = getRequestContext();
      context->associatedStripe = mCurStripeWriteContext;
      context->targetBytes = Configuration::GetStripeParitySize();
      mCurStripeWriteContext->ioContext.emplace_back(context);
      context->zoneGroup = this;
    }
    return true;
  }
}

void b()
{}

bool ZoneGroup::Append(RequestContext *ctx, uint32_t offset)
{
  if (mStripePos == 0) {
    if (!findStripe(ctx)) {
      return false;
    }
  }

  SystemMode mode = Configuration::GetSystemMode();

  uint32_t lba = ctx->lba + offset;
  uint8_t *blkdata = (uint8_t*)(ctx->data) + offset;
  uint32_t zoneId = mStripePos / Configuration::GetBlockSize();

  memcpy(mCurStripeWriteContext->data + mStripePos, blkdata, Configuration::GetBlockSize());

  RequestContext *slot = getRequestContext();
  mCurStripeWriteContext->ioContext.emplace_back(slot);
  slot->associatedStripe = mCurStripeWriteContext;
  slot->associatedRequest = ctx;
  slot->targetBytes = Configuration::GetBlockSize();
  slot->lba = lba;
  slot->zoneId = zoneId;
  slot->stripeId = mZonePos % mSyncGroupSize;
  slot->zoneGroup = this;
  slot->ctrl = mRaidController;
  slot->status = WRITE_REAPING;
  slot->type = STRIPE_UNIT;
  slot->data = mCurStripeWriteContext->data + zoneId * Configuration::GetBlockSize();
  slot->meta = mCurStripeWriteContext->metadata + zoneId * Configuration::GetMetadataSize();
  if (mode == PURE_WRITE) {
    slot->append = false;
    slot->offset = mZonePos;
  } else {
    slot->append = true;
    slot->offset = mZonePos | (mSyncGroupSize - 1);
  }
  mZones[zoneId]->Write(mZonePos, Configuration::GetStripeUnitSize(), (void*)slot);

  mStripePos += Configuration::GetBlockSize();
  if (mStripePos == Configuration::GetStripeDataSize()) {
    // write parity data
    uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
    uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
    encodeStripe(mCurStripeWriteContext->data, n, k);
    
    for (int i = 0; i < n - k; ++i) {
      RequestContext *slot = mCurStripeWriteContext->ioContext[i];
      slot->lba = ~0ull;
      slot->ctrl = mRaidController;
      slot->zoneGroup = this;
      slot->zoneId = k;
      slot->stripeId = mZonePos % mSyncGroupSize;
      slot->status = WRITE_REAPING;
      slot->type = STRIPE_UNIT;
      slot->data = mCurStripeWriteContext->data + (k + i) * Configuration::GetBlockSize();
      slot->data = mCurStripeWriteContext->metadata + (k + i) * 64;
      if (mode == PURE_WRITE) {
        slot->append = false;
        slot->offset = mZonePos;
      } else {
        slot->append = true;
        slot->offset = mZonePos | (mSyncGroupSize - 1);
      }
      mZones[k + i]->Write(mZonePos, Configuration::GetStripeUnitSize(), slot);
    }
    mStripePos = 0;
    mZonePos += 1;

    if ((mode == ZONE_APPEND_WITH_META || mode == ZONE_APPEND_WITH_REDIRECTION)
        && needSyncPoint()) {
      mSyncGroupStatus = 1;
    }
  }

  return true;
}

bool ZoneGroup::Read(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr)
{
  if (mAvailableReadContext.empty()) {
    handleRequestCompletions();
    if (mAvailableReadContext.empty()) {
      return false;
    }
  }

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
  slot->offset = phyAddr.offset;
  slot->ctrl = mRaidController;
  slot->zoneGroup = this;
  slot->status = READ_REAPING;
  slot->type = STRIPE_UNIT;
  slot->data = readContext->data;
  slot->meta = readContext->metadata;

  readContext->ioContext.emplace_back(slot);

  uint32_t zoneId = slot->zoneId;
  uint32_t offset = slot->offset;
  mZones[zoneId]->Read(offset, Configuration::GetStripeUnitSize(), slot);

  return true;
}

bool ZoneGroup::ReadValid(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr, bool *isValid)
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

void ZoneGroup::encodeStripe(uint8_t *stripe, uint32_t n, uint32_t k)
{
  uint8_t *input[k];
  uint8_t *output[n - k];
  for (int i = 0; i < k; ++i) {
    input[i] = stripe + Configuration::GetStripeUnitSize() * i;
  }
  for (int i = 0; i < n - k; ++i) {
    output[i] = stripe + Configuration::GetStripeUnitSize() * (k + i);
  }
  ec_encode_data(Configuration::GetStripeUnitSize(), k, n - k, gGfTables, input, output);
}

void ZoneGroup::decodeStripe(uint8_t *stripe, uint32_t n, uint32_t k, uint32_t decodeIndex)
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

  for (uint32_t i = 0, j = 0; i < n; ++i) {
    if (i == decodeIndex) continue;
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
    if (i != decodeIndex) {
      input[j] = stripe + i * Configuration::GetStripeUnitSize();
      j++;
    } else {
      output[l] = stripe + i * Configuration::GetStripeUnitSize();
      l++;
    }
  }
  ec_encode_data(Configuration::GetStripeUnitSize(), k, 1, decodeGfTbl, input, output);
}

void ZoneGroup::UpdateSyncPoint(uint32_t zoneId, uint32_t stripeId, uint32_t offset)
{
  uint16_t *mapping = (uint16_t*)(mSyncPoint.data) + zoneId * mSyncGroupSize;;
  mapping[offset % mSyncGroupSize] = stripeId;
}

void ZoneGroup::issueSyncPoint()
{
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  for (int i = 0; i < n; ++i) {
    mSyncPoint.slots[i].Clear();
    mSyncPoint.slots[i].available = false;
    mSyncPoint.slots[i].targetBytes = Configuration::GetStripeUnitSize();
    mSyncPoint.slots[i].append = false;
    mSyncPoint.slots[i].ctrl = mRaidController;
    mSyncPoint.slots[i].zoneGroup = this;
    mSyncPoint.slots[i].zoneId = i;
    mSyncPoint.slots[i].stripeId = mZonePos % mSyncGroupSize; // must be mSyncGroupSize - 1
    mSyncPoint.slots[i].offset = mZonePos;
    mSyncPoint.slots[i].type = STRIPE_UNIT;
    mSyncPoint.slots[i].status = WRITE_REAPING;
    mSyncPoint.slots[i].lba = ~0ull;
    mSyncPoint.slots[i].data = mSyncPoint.data + i * Configuration::GetStripeUnitSize();
    mSyncPoint.slots[i].meta = mSyncPoint.metadata + i * Configuration::GetMetadataSize();
    BlockMetadata *meta = (BlockMetadata*)mSyncPoint.slots[i].meta;
    meta->d.lba = ~0ull;
    meta->d.stripeId = mSyncGroupSize - 1;
  }

  encodeStripe(mSyncPoint.data, n, k);
  for (int i = 0; i < n; ++i) {
    mZones[i]->Write(mZonePos, Configuration::GetStripeUnitSize(), &mSyncPoint.slots[i]);
  }
  mZonePos += 1;
}

bool ZoneGroup::needSyncPoint()
{
  return (mZonePos + 1) % mSyncGroupSize == 0;
}

bool ZoneGroup::isSyncPointDone()
{
  bool done = true;
  for (int i = 0; i < Configuration::GetStripeSize() / Configuration::GetStripeUnitSize(); ++i) {
    if (!mSyncPoint.slots[i].available) {
       done = false;
    }
  }
  return done;
}

void ZoneGroup::Reset()
{
  mResetContext.clear();
  for (int i = 0; i < mZones.size(); ++i) {
    mResetContext.emplace_back(RequestContext());
    RequestContext *context = &mResetContext.back();
    context->Clear();
    context->ctrl = mRaidController;
    mZones[i]->Reset(context);
  }
}

void ZoneGroup::Seal()
{
  mResetContext.clear();
  for (int i = 0; i < mZones.size(); ++i) {
    mResetContext.emplace_back(RequestContext());
    RequestContext *context = &mResetContext.back();
    context->Clear();
    mZones[i]->Seal(context);
  }
}

void ZoneGroup::ReadStripeMeta(RequestContext *ctx)
{
  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

  ReadContext *readContext = ctx->associatedRead;

  uint32_t syncPointOffset = offset | (mSyncGroupSize - 1);
  ctx->targetBytes = k * Configuration::GetStripeUnitSize();
  ctx->successBytes = 0;

  if (syncPointOffset = mZonePos | (mSyncGroupSize - 1)) {
    memcpy(readContext->data, mSyncPoint.data, n * Configuration::GetStripeUnitSize());
    ctx->needDecodeMeta = false;
    ctx->successBytes = ctx->targetBytes;
    ctx->Queue();
  } else {
    ctx->needDecodeMeta = true;
    for (int i = 0; i < n; ++i) {
      if (i == zoneId) continue;
      RequestContext *reqCtx = getRequestContext();
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetBlockSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->zoneGroup = this;
      reqCtx->data = (uint8_t*)readContext->data + i * Configuration::GetStripeUnitSize();
      reqCtx->meta = (uint8_t*)readContext->metadata + i * Configuration::GetMetadataSize();
      readContext->ioContext.emplace_back(reqCtx);

      mZones[i]->Read(syncPointOffset, Configuration::GetBlockSize(), reqCtx);
    }
  }
}

void ZoneGroup::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t stripeId = ctx->stripeId;
  uint32_t offset = ctx->offset;
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;

  uint32_t realOffsets[n];
  ctx->targetBytes = k * Configuration::GetStripeUnitSize();
  ctx->successBytes = 0;

  if (mode == PURE_ZONE_APPEND) {
    fprintf(stderr, "Pure zone append does not support recovery.\n");
    exit(-1);
  }

  if (mode == ZONE_APPEND_WITH_META || mode == ZONE_APPEND_WITH_REDIRECTION) {
    if (ctx->needDecodeMeta) {
      decodeStripe(readContext->data, n, k, zoneId);
    }
    // offset to stripe ID
    uint32_t *offsetMap = reinterpret_cast<uint32_t*>(readContext->data);
    uint32_t stripeId = 0;
    uint32_t offsetBegin = offset & (~(mSyncGroupSize - 1));

    for (uint32_t i = 0; i < n; ++i) {
      uint32_t *offsetMapCurZone = offsetMap + i * mSyncGroupSize;
      for (uint32_t j = 0; j < mSyncGroupSize; ++j) {
        if (offsetMap[j] == stripeId) {
          realOffsets[i] = offsetBegin + j;
        }
      }
    }
  } else {
    for (uint32_t i = 0; i < n; ++i) {
      realOffsets[i] = offset;
    }
  }

  for (uint32_t i = 0; i < n; ++i) {
    if (i == zoneId) continue;
    RequestContext *reqCtx = getRequestContext();
    reqCtx->Clear();
    reqCtx->associatedRequest = ctx;
    reqCtx->status = DEGRADED_READ_SUB;
    reqCtx->type = STRIPE_UNIT;
    reqCtx->targetBytes = Configuration::GetStripeUnitSize();
    reqCtx->ctrl = mRaidController;
    reqCtx->zoneGroup = this;
    reqCtx->data = (uint8_t*)readContext->data + i * Configuration::GetStripeUnitSize();
    reqCtx->meta = (uint8_t*)readContext->metadata + i * Configuration::GetMetadataSize();
    readContext->ioContext.emplace_back(reqCtx);
    mZones[i]->Read(realOffsets[i], Configuration::GetStripeUnitSize(), reqCtx);
  }
}

void ZoneGroup::WriteComplete(RequestContext *ctx)
{
  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = &mBlockMetadata[ctx->zoneId * mCapacity + ctx->offset];
    meta->d.lba = ctx->lba;
    meta->d.stripeId = ctx->stripeId;
  }
}

void ZoneGroup::ReadComplete(RequestContext *ctx)
{
  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  RequestContext *parent = ctx->associatedRequest;

  ReadContext *readContext = ctx->associatedRead;
  if (ctx->status == READ_REAPING) {
    memcpy((uint8_t*)parent->data + ctx->lba - parent->lba,
            ctx->data, Configuration::GetStripeUnitSize());
  } else if (ctx->status == DEGRADED_READ_REAPING) {
    decodeStripe(readContext->data, n, k, zoneId);
    memcpy((uint8_t*)parent->data + ctx->lba - parent->lba,
            ctx->data, Configuration::GetStripeUnitSize());
  }

  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = (BlockMetadata*)ctx->meta;
    meta->d.lba = mBlockMetadata[zoneId * mCapacity + offset].d.lba;
    meta->d.stripeId = mBlockMetadata[zoneId * mCapacity + offset].d.stripeId;
//    printf("Fetch metadata: %lu %d %d %d %d\n", meta->d.lba, meta->d.stripeId, zoneId, offset, mValidBits[zoneId * mCapacity + offset]);
  }
  if (parent->meta) {
    uint32_t offsetInBlks = (ctx->lba - parent->lba) / Configuration::GetBlockSize();
    BlockMetadata *result = (BlockMetadata*)(ctx->meta);
    BlockMetadata *parentMeta = &((BlockMetadata*)parent->meta)[offsetInBlks];
    parentMeta->d.lba = result->d.lba;
    parentMeta->d.stripeId = result->d.stripeId;
  }
}

uint32_t ZoneGroup::GetZoneSize()
{
  return mCapacity;
}

void ZoneGroup::InvalidateBlock(uint32_t zoneId, uint32_t realOffset)
{
  mNumInvalidBlocks += 1;
  mValidBits[zoneId * mCapacity + realOffset] = false;
}

void ZoneGroup::FinishBlock(uint32_t zoneId, uint32_t offset, uint64_t lba)
{
  if (lba != ~0ull) {
    mNumBlocks += 1;
    mValidBits[zoneId * mCapacity + offset] = true;
  }
}

void ZoneGroup::ReleaseZones()
{
  for (auto zone : mZones) {
    zone->Release();
  }
}

void ZoneGroup::FlushStripe()
{
  if (mStripePos == 0) return;
  SystemMode mode = Configuration::GetSystemMode();

  for ( ; mStripePos < Configuration::GetStripeDataSize(); mStripePos += Configuration::GetBlockSize()) {
    uint32_t zoneId = mStripePos / Configuration::GetBlockSize();
    RequestContext *slot = getRequestContext();
    mCurStripeWriteContext->ioContext.emplace_back(slot);
    slot->associatedStripe = mCurStripeWriteContext;
    slot->associatedRequest = nullptr;
    slot->targetBytes = Configuration::GetBlockSize();
    slot->lba = ~0ull;
    slot->zoneId = zoneId;
    slot->stripeId = mZonePos % mSyncGroupSize;
    slot->zoneGroup = this;
    slot->ctrl = mRaidController;
    // if SyncGroup is not enable, use conventional write
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->data = mCurStripeWriteContext->data + zoneId * Configuration::GetBlockSize();
    slot->meta = mCurStripeWriteContext->metadata + zoneId * 64;
    if (mode == PURE_WRITE) {
      slot->append = false;
      slot->offset = mZonePos;
    } else {
      slot->append = true;
      slot->offset = mZonePos | (mSyncGroupSize - 1);
    }

    mZones[zoneId]->Write(mZonePos, Configuration::GetStripeUnitSize(), (void*)slot);
  }

  if (mStripePos == Configuration::GetStripeDataSize()) {
    // write parity data
    uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
    uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
    encodeStripe(mCurStripeWriteContext->data, n, k);
    
    for (int i = 0; i < n - k; ++i) {
      RequestContext *slot = mCurStripeWriteContext->ioContext[i];
      slot->lba = ~0ull;
      slot->ctrl = mRaidController;
      slot->zoneGroup = this;
      slot->zoneId = k;
      slot->stripeId = mZonePos % mSyncGroupSize;
      slot->status = WRITE_REAPING;
      slot->type = STRIPE_UNIT;
      slot->data = mCurStripeWriteContext->data + (k + i) * Configuration::GetBlockSize();
      slot->data = mCurStripeWriteContext->metadata + (k + i) * Configuration::GetMetadataSize();
      if (mode == PURE_WRITE) {
        slot->append = false;
        slot->offset = mZonePos;
      } else {
        slot->append = true;
        slot->offset = mZonePos | (mSyncGroupSize - 1);
      }
      mZones[k + i]->Write(mZonePos, Configuration::GetStripeUnitSize(), slot);
    }
    mStripePos = 0;
    mZonePos += 1;

    if ((mode == ZONE_APPEND_WITH_META || mode == ZONE_APPEND_WITH_REDIRECTION)
        && needSyncPoint()) {
      mSyncGroupStatus = 1;
    }
  }
}
