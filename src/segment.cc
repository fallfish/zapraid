#include "segment.h"

#include <sys/time.h>
#include <isa-l.h>
#include "raid_controller.h"

uint8_t *Segment::gEncodeMatrix = nullptr;
uint8_t *Segment::gGfTables = nullptr;

Segment::Segment(RAIDController *raidController,
                 uint32_t segmentId,
                 RequestContextPool *ctxPool,
                 ReadContextPool *rctxPool,
                 StripeWriteContextPool *sctxPool)
{
  SystemMode mode = Configuration::GetSystemMode();
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

  if (gEncodeMatrix == nullptr) {
    gEncodeMatrix = new uint8_t[n * k];
    gGfTables = new uint8_t[32 * n * (n - k)];
    gf_gen_rs_matrix(gEncodeMatrix, n, k);
    ec_init_tables(k, n - k, &gEncodeMatrix[k * k], gGfTables);
    printf("gGfTables: %p\n", gGfTables);
  }

  mRaidController = raidController;
  mPos = 0;
  mPosInStripe = 0;

  mRequestContextPool = ctxPool;
  mReadContextPool = rctxPool;
  mStripeWriteContextPool = sctxPool;

  mAdminStripe = new StripeWriteContext();
  mAdminStripe->data = new uint8_t *[Configuration::GetStripeSize() /
                                     Configuration::GetBlockSize()];
  mAdminStripe->metadata = new uint8_t *[Configuration::GetStripeSize() /
                                         Configuration::GetBlockSize()];

  mCurrentGroupMeta.data = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
  mCurrentGroupMeta.metadata = (uint8_t*)spdk_zmalloc(
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
  mSegmentMeta.raidScheme = (uint32_t)Configuration::GetRaidLevel();
  mSegmentMeta.numZones = 0;

  mNumBlocks = 0;
  mNumInvalidBlocks = 0;

  mHeaderRegionSize = raidController->GetHeaderRegionSize();
  mDataRegionSize = raidController->GetDataRegionSize();
  mFooterRegionSize = raidController->GetFooterRegionSize();

  mValidBits = new bool[mSegmentMeta.n * mDataRegionSize];
  memset(mValidBits, 0, mSegmentMeta.n * mDataRegionSize);

  if (Configuration::GetSyncGroupSize() <= 256) { // less than 8 bits
    mCompactStripeTable = new uint8_t[mSegmentMeta.n * mDataRegionSize];
    memset(mCompactStripeTable, 0, mSegmentMeta.n * mDataRegionSize);
  } else { // 16 bits
    mCompactStripeTable = new uint8_t[2 * mSegmentMeta.n * mDataRegionSize];
    memset(mCompactStripeTable, 0, 2 * mSegmentMeta.n * mDataRegionSize);
  }

  mProtectedBlockMetadata = new ProtectedBlockMetadata[mSegmentMeta.n * mDataRegionSize];
  memset(mProtectedBlockMetadata, 0, mSegmentMeta.n * mDataRegionSize);

  mSegmentStatus = SEGMENT_NORMAL;
}

Segment::~Segment()
{
  // TODO: reclaim zones to devices
  delete []mValidBits;
  delete []mCompactStripeTable;
  if (!Configuration::GetDeviceSupportMetadata()) {
    delete []mProtectedBlockMetadata;
  }
  // delete mValidBits;
}

void Segment::recycleStripeWriteContexts()
{
  mStripeWriteContextPool->Recycle();
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

uint32_t Segment::GetNumBlocks() const
{
  return mNumBlocks;
}

uint32_t Segment::GetNumInvalidBlocks() const
{
  return mNumInvalidBlocks;
}

bool Segment::IsFull()
{
  if (Configuration::GetSystemMode() == ZAPRAID) {
    return mPos == mHeaderRegionSize + mDataRegionSize - 1;
  } else {
    return mPos == mHeaderRegionSize + mDataRegionSize;
  }
}

bool Segment::CanSeal()
{
  return mPos == mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;
}

void Segment::PrintStats()
{
  printf("Zone group position: %d, capacity: %d, num invalid blocks: %d\n", mPos, mDataRegionSize, mNumInvalidBlocks);
  for (auto zone : mZones) {
    zone->PrintStats();
  }
}

void Segment::recycleContexts()
{
  recycleStripeWriteContexts();
}

bool Segment::checkStripeAvailable(StripeWriteContext *stripe)
{
  bool isAvailable = true;

  for (auto slot : stripe->ioContext) {
    isAvailable = slot && slot->available ? isAvailable : false;
  }

  return isAvailable;
}

SegmentStatus Segment::GetStatus()
{
  return mSegmentStatus;
}

void progressFooterWriter2(void *arg1, void *arg2) {
 Segment *segment = reinterpret_cast<Segment*>(arg1);
 segment->ProgressFooterWriter();
}

void progressFooterWriter(void *args) {
  progressFooterWriter2(args, nullptr);
}

// StateTransition must be called in the same thread
// as Append()
bool Segment::StateTransition()
{
  bool stateChanged = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    if (mPos == mHeaderRegionSize + mDataRegionSize) {
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    }
  }

  if (mSegmentStatus == SEGMENT_PREPARE_STRIPE_META) {
    // under SyncPoint, waiting for previous appends

    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
      if (Configuration::GetSystemMode() == ZAPRAID) {
        IssueGroupMeta();
        mSegmentStatus = SEGMENT_WRITING_STRIPE_META;
      } else {
        mSegmentStatus = SEGMENT_NORMAL;
      }
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_STRIPE_META) {
    if (Configuration::GetSystemMode() == ZAPRAID) {
      if (hasGroupMetaDone()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_NORMAL;
      }
    } else if (Configuration::GetSystemMode() == GROUP_LAYOUT) {
      mStripeWriteContextPool->Recycle();
      if (mStripeWriteContextPool->NoInflightStripes()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_NORMAL;
      }
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_HEADER) {
    // wait for finalizing the header
    if (checkStripeAvailable(mCurStripe) && mStripeWriteContextPool->NoInflightStripes()) {
      for (auto slot : mCurStripe->ioContext) {
        mRequestContextPool->ReturnRequestContext(slot);
      }
      mCurStripe->ioContext.clear();
      stateChanged = true;
      mSegmentStatus = SEGMENT_NORMAL;
    }
  } else if (mSegmentStatus == SEGMENT_PREPARE_FOOTER) {
    // wait for persisting the stripes
    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
      // prepare the stripe for writing the footer
      mCurStripe = mAdminStripe;
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;
      for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
        RequestContext *slot = mRequestContextPool->GetRequestContext(true);
        slot->data = slot->dataBuffer;
        slot->meta = slot->metadataBuffer;
        slot->targetBytes = 4096;
        slot->type = STRIPE_UNIT;
        slot->segment = this;
        slot->ctrl = mRaidController;
        slot->lba = ~0ull;
        slot->associatedRequest = nullptr;
        slot->available = true;
        slot->append = false;

        mCurStripe->data[i] = (uint8_t*)slot->data;
        mCurStripe->ioContext.emplace_back(slot);

        mSegmentStatus = SEGMENT_WRITING_FOOTER;
      }
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_FOOTER) {
    if (checkStripeAvailable(mCurStripe)) {
      if (CanSeal()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_SEALING;
        Seal();
      } else {
        for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
          uint32_t zoneId = Configuration::CalculateDiskId(
              mPos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
          RequestContext *slot = mCurStripe->ioContext[i];
          slot->available = false;
        }
        if (!Configuration::GetEventFrameworkEnabled()) {
          thread_send_msg(mRaidController->GetEcThread(), progressFooterWriter, this);
        } else {
          event_call(Configuration::GetEcThreadCoreId(), progressFooterWriter2, this, nullptr);
        }
      }
    }
  } else if (mSegmentStatus == SEGMENT_SEALING) {
    if (checkStripeAvailable(mCurStripe)) {
      for (auto slot : mCurStripe->ioContext) {
        mRequestContextPool->ReturnRequestContext(slot);
      }
      mCurStripe->ioContext.clear();
      stateChanged = true;
      delete []mAdminStripe->data;
      delete []mAdminStripe->metadata;
      delete mAdminStripe;
      spdk_free(mCurrentGroupMeta.data);
      spdk_free(mCurrentGroupMeta.metadata);
      if (Configuration::GetDeviceSupportMetadata()) {
        delete []mProtectedBlockMetadata;
      }
      mSegmentStatus = SEGMENT_SEALED;
    }
  }

  return stateChanged;
}

void finalizeSegmentHeader2(void *arg1, void *arg2)
{
  Segment *segment = reinterpret_cast<Segment*>(arg1);
  segment->FinalizeSegmentHeader();
}

void finalizeSegmentHeader(void *args)
{
  finalizeSegmentHeader2(args, nullptr);
}

void Segment::FinalizeCreation()
{
  mSegmentStatus = SEGMENT_WRITING_HEADER;
  StripeWriteContext *stripe = mAdminStripe;
  mCurStripe = stripe;
  stripe->ioContext.resize(mSegmentMeta.n);
  stripe->successBytes = 0;
  stripe->targetBytes = mSegmentMeta.stripeSize;

  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = mRequestContextPool->GetRequestContext(true);
    stripe->ioContext[i] = slot;
    slot->available = false;
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mRaidController->GetEcThread(), finalizeSegmentHeader, this);
  } else {
    event_call(Configuration::GetEcThreadCoreId(), finalizeSegmentHeader2, this, nullptr);
  }
}

void Segment::FinalizeSegmentHeader()
{
  if (!Configuration::GetEnableHeaderFooter()) {
    mSegmentStatus = SEGMENT_NORMAL;
    return;
  }

  StripeWriteContext *stripe = mAdminStripe;

  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = stripe->ioContext[i];
    slot->associatedStripe = stripe;
    slot->targetBytes = Configuration::GetBlockSize();
    slot->lba = ~0ull;
    slot->zoneId = i;
    slot->stripeId = 0;
    slot->segment = this;
    slot->ctrl = mRaidController;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->data = slot->dataBuffer;
    slot->meta = slot->metadataBuffer;
    slot->append = false;
    slot->offset = 0;

    stripe->data[i] = slot->data;
    memcpy(stripe->data[i], &mSegmentMeta, sizeof(mSegmentMeta));

    mZones[i]->Write(0, Configuration::GetBlockSize(), (void*)slot);
  }
  mPos += 1;
}

void Segment::ProgressFooterWriter()
{
  // Currently it is the i-th stripe of the footer
  // 8 bytes for LBA and 8 bytes for timestamp
  // Each footer block contains blockSize / (8 + 8) entries
  // Thus the offset in the metadata array is "i * blockSize / 16"
  uint32_t begin = (mPos - mHeaderRegionSize - mDataRegionSize) * Configuration::GetBlockSize() / 16;
  uint32_t end = std::min(mDataRegionSize, begin + Configuration::GetBlockSize() / 16);
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    uint32_t base = i * mDataRegionSize;
    uint64_t *footerBlock = reinterpret_cast<uint64_t*>(mCurStripe->ioContext[i]->dataBuffer);
    uint32_t pos = 0;
    for (uint32_t offset = begin; offset < end; ++offset) {
      footerBlock[pos++] = mProtectedBlockMetadata[base + offset].lba;
      footerBlock[pos++] = mProtectedBlockMetadata[base + offset].timestamp;
    }
  }
  mCurStripe->successBytes = 0;
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = mCurStripe->ioContext[i];
    slot->status = WRITE_REAPING;
    slot->successBytes = 0;
    slot->offset = mPos;
    slot->zoneId = i;
    slot->data = slot->dataBuffer;
    slot->meta = slot->metadataBuffer;
    mZones[i]->Write(mPos, Configuration::GetBlockSize(), (void*)slot);
  }
  mPos += 1;
}

bool Segment::findStripe()
{
  bool accept = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    // Under normal appends, proceed

    StripeWriteContext *stripe = mStripeWriteContextPool->GetContext();
    if (stripe == nullptr) {
      accept = false;
    } else {
      mCurStripe = stripe;
      mCurStripe->successBytes = 0;
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;
      // prepare contexts for parity blocks
      mCurStripe->ioContext.resize(mSegmentMeta.n);
      for (int j = 0; j < Configuration::GetStripeParitySize() / Configuration::GetStripeUnitSize(); ++j) {
        RequestContext *context = mRequestContextPool->GetRequestContext(true);
        context->associatedStripe = mCurStripe;
        context->targetBytes = Configuration::GetBlockSize();
        context->data = context->dataBuffer;
        context->meta = context->metadataBuffer;
        context->segment = this;
        mCurStripe->ioContext[mSegmentMeta.k + j] = context;
      }

      accept = true;
    }
  }
  return accept;
}

struct GenerateParityBlockArgs {
  Segment *segment;
  StripeWriteContext *stripe;
  uint32_t zonePos;
};

void generateParityBlock2(void *arg1, void *arg2)
{
  struct GenerateParityBlockArgs *gen_args = reinterpret_cast<struct GenerateParityBlockArgs*>(arg1);
  Segment *segment = reinterpret_cast<Segment*>(gen_args->segment);
  StripeWriteContext *stripe = reinterpret_cast<StripeWriteContext*>(gen_args->stripe);
  uint32_t zonePos = gen_args->zonePos;
  free(gen_args);
  segment->GenerateParityBlock(stripe, zonePos);
}

void generateParityBlock(void *args)
{
  generateParityBlock2(args, nullptr);
}


bool Segment::Append(RequestContext *ctx, uint32_t offset)
{
  if (mPosInStripe == 0) {
    if (!findStripe()) {
      return false;
    }
  }

  SystemMode mode = Configuration::GetSystemMode();
  uint32_t blockSize = Configuration::GetBlockSize();

  uint64_t lba = ~0ull;
  uint8_t *blkdata = nullptr;
  uint32_t whichBlock = mPosInStripe / blockSize;

  lba = ctx->lba + offset * blockSize;
  blkdata = (uint8_t*)(ctx->data) + offset * blockSize;

  uint32_t zoneId = Configuration::CalculateDiskId(
      mPos, whichBlock,
      (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
  // Issue data block
  {
    RequestContext *slot = mRequestContextPool->GetRequestContext(true);
    mCurStripe->ioContext[whichBlock] = slot;
    slot->lba = lba;
    slot->timestamp = ctx->timestamp;

    slot->data = slot->dataBuffer;
    memcpy(slot->data, blkdata, blockSize);
    slot->meta = slot->metadataBuffer;

    slot->associatedStripe = mCurStripe;
    slot->associatedRequest = ctx;

    slot->zoneId = zoneId;
    slot->stripeId = (mPos - mHeaderRegionSize) % Configuration::GetSyncGroupSize();
    slot->segment = this;
    slot->ctrl = mRaidController;

    slot->type = STRIPE_UNIT;
    slot->status = WRITE_REAPING;
    slot->targetBytes = blockSize;

    // Initialize block (flash page) metadata
    BlockMetadata *blkMeta = (BlockMetadata *)slot->meta;
    blkMeta->fields.protectedField.lba = slot->lba;
    blkMeta->fields.protectedField.timestamp = slot->timestamp;
    blkMeta->fields.nonProtectedField.stripeId = slot->stripeId;

    if (mode == ZONEWRITE_ONLY
        || (mode == GROUP_LAYOUT && mPos % Configuration::GetSyncGroupSize() == 0)) {
      slot->append = false;
    } else {
      slot->append = true;
    }

     mZones[zoneId]->Write(mPos, blockSize, (void*)slot);
  }

  mPosInStripe += blockSize;
  if (mPosInStripe == mSegmentMeta.stripeDataSize) {
  // issue parity block
    GenerateParityBlockArgs *args = (GenerateParityBlockArgs*)calloc(1, sizeof(GenerateParityBlockArgs));
    args->segment = this;
    args->stripe = mCurStripe;
    args->zonePos = mPos;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(mRaidController->GetEcThread(), generateParityBlock, args);
    } else {
      event_call(Configuration::GetEcThreadCoreId(), generateParityBlock2, args, nullptr);
    }

    mPosInStripe = 0;
    mPos += 1;

    if ((mode == GROUP_LAYOUT || mode == ZAPRAID)
        && NeedGroupMeta()) {
      // writing the stripe metadata at the end of each group
      mSegmentStatus = SEGMENT_PREPARE_STRIPE_META;
    }

    if (mode == GROUP_LAYOUT && mPos % Configuration::GetSyncGroupSize() == 1) {
      // The next stripe is the begin of the next group
      mSegmentStatus = SEGMENT_WRITING_STRIPE_META;
    }

    if (mPos == mHeaderRegionSize + mDataRegionSize) {
      // writing the P2L table at the end of the segment
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    }
  }

  return true;
}

void Segment::GenerateParityBlock(StripeWriteContext *stripe, uint32_t zonePos)
{
  static double accummulated = 0;
  static int count = 0;
  struct timeval s, e;
  // gettimeofday(&s, NULL);
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  SystemMode mode = Configuration::GetSystemMode();

  uint8_t* stripeData[n];
  uint8_t* stripeProtectedMetadata[n];
  if (Configuration::GetRaidLevel() == RAID0) {
    // NO PARITY
  } else if (Configuration::GetRaidLevel() == RAID1) {
    for (uint32_t i = k; i < n; ++i) {
      memcpy(stripe->ioContext[i]->data, stripe->ioContext[0]->data, Configuration::GetBlockSize());
    }
  } else if (Configuration::GetRaidLevel() == RAID01) {
    for (uint32_t i = k; i < n; ++i) {
      memcpy(stripe->ioContext[i]->data, stripe->ioContext[i - k]->data, Configuration::GetBlockSize());
    }
  } else if (Configuration::GetRaidLevel() == RAID4
      || Configuration::GetRaidLevel() == RAID5
      || Configuration::GetRaidLevel() == RAID6) {
    for (uint32_t i = 0; i < n; ++i) {
      stripeData[i] = stripe->ioContext[i]->data;
      stripeProtectedMetadata[i] = reinterpret_cast<uint8_t*>(
          &(((BlockMetadata*)stripe->ioContext[i]->meta)->fields.protectedField));
    }
    encodeStripe(stripeData, n, k, Configuration::GetBlockSize());
    encodeStripe(stripeProtectedMetadata, n, k, 16); // 16 bytes for the protected fields
  }

  for (uint32_t i = k; i < n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        zonePos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
    RequestContext *slot = stripe->ioContext[i];
    slot->lba = ~0ull;
    slot->ctrl = mRaidController;
    slot->segment = this;
    slot->zoneId = zoneId;
    slot->stripeId = (zonePos - mHeaderRegionSize) % Configuration::GetSyncGroupSize();
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    if (mode == ZONEWRITE_ONLY
        || (mode == GROUP_LAYOUT && zonePos % Configuration::GetSyncGroupSize() == 0)) {
      slot->append = false;
    } else {
      slot->append = true;
    }

    BlockMetadata *blkMeta = (BlockMetadata *)slot->meta;
    blkMeta->fields.nonProtectedField.stripeId = slot->stripeId;

    mZones[zoneId]->Write(zonePos, Configuration::GetBlockSize(), slot);
  }
}

bool Segment::Read(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr)
{
  ReadContext *readContext = mReadContextPool->GetContext();
  if (readContext == nullptr) {
    return false;
  }

  uint32_t blockSize = Configuration::GetBlockSize();
  RequestContext *slot = mRequestContextPool->GetRequestContext(true);
  slot->associatedRead = readContext;
  slot->available = false;
  slot->associatedRequest = ctx;
  slot->lba = ctx->lba + pos * blockSize;
  slot->targetBytes = blockSize;
  slot->zoneId = phyAddr.zoneId;
  slot->offset = phyAddr.offset;
  slot->ctrl = mRaidController;
  slot->segment = this;
  slot->status = READ_REAPING;
  slot->type = STRIPE_UNIT;
  slot->data = (uint8_t*)ctx->data + pos * blockSize;
  slot->meta = (uint8_t*)(slot->metadataBuffer);

  readContext->data[readContext->ioContext.size()] = (uint8_t*)slot->data;
  readContext->ioContext.emplace_back(slot);

  slot->needDegradedRead = Configuration::GetEnableDegradedRead();
  if (slot->needDegradedRead) {
    slot->Queue();
  } else {
    uint32_t zoneId = slot->zoneId;
    uint32_t offset = slot->offset;
    mZones[zoneId]->Read(offset, blockSize, slot);
  }

  return true;
}

bool Segment::ReadValid(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr, bool *isValid)
{
  bool success = false;
  uint32_t zoneId = phyAddr.zoneId;
  uint32_t offset = phyAddr.offset;
  if (offset < mHeaderRegionSize || offset >= mHeaderRegionSize + mDataRegionSize) {
    *isValid = false;
    success = true;
  } else if (mValidBits[zoneId * mDataRegionSize + offset - mHeaderRegionSize] == 0) {
    *isValid = false;
    success = true;
  } else {
    *isValid = true;
    success = Read(ctx, pos, phyAddr);
  }
  return success;
}

void Segment::encodeStripe(uint8_t **stripe, uint32_t n, uint32_t k, uint32_t unitSize)
{
  uint8_t *input[k];
  uint8_t *output[n - k];
  for (int i = 0; i < k; ++i) {
    input[i] = stripe[i];
  }
  for (int i = 0; i < n - k; ++i) {
    output[i] = stripe[k + i];
  }
  if (Configuration::GetRaidLevel() == RAID1) {
    memcpy(output[0], input[0], unitSize);
  } else {
    ec_encode_data(unitSize, k, n - k, gGfTables, input, output);
  }
}

void Segment::decodeStripe(uint32_t offset, uint8_t **stripe, bool *alive, uint32_t n, uint32_t k, uint32_t decodeZid)
{
  if (Configuration::GetRaidLevel() == RAID1) {
    memcpy(stripe[decodeZid], stripe[1 - decodeZid], Configuration::GetBlockSize());
    return;
  }

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
          offset, i, (RAIDLevel)mSegmentMeta.raidScheme,
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

void Segment::UpdateGroupMeta(uint32_t zoneId, uint32_t stripeId, uint32_t offset)
{
  uint32_t syncGroupSize = Configuration::GetSyncGroupSize();
  uint16_t *mapping = (uint16_t*)mCurrentGroupMeta.data + zoneId * syncGroupSize;
  mapping[offset % syncGroupSize] = stripeId;
}

void Segment::IssueGroupMeta()
{
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  uint8_t *stripe[n];

  uint32_t groupSize = Configuration::GetSyncGroupSize();
  bool isSingleBlock = (sizeof(uint16_t) * groupSize * n <= 4096);
  uint16_t *array = reinterpret_cast<uint16_t*>(mCurrentGroupMeta.data);
  for (uint32_t i = 0; i < n; ++i) {
    mCompactStripeTable[i * mDataRegionSize + mPos - 1] = groupSize - 1;
    for (uint32_t j = 0; j < groupSize; ++j) {
      uint32_t index1 = i * mDataRegionSize + mPos - groupSize + j;
      uint32_t index2 = i * groupSize + j;
      if (groupSize <= 256) {
        array[index2] = mCompactStripeTable[index1];
      } else {
        array[index2] = ((uint16_t*)mCompactStripeTable)[index1];
      }
    }
  }

  if (!isSingleBlock) {
    for (uint32_t i = 0; i < n; ++i) {
      stripe[i] = mCurrentGroupMeta.data + i * Configuration::GetBlockSize();
    }
    encodeStripe(stripe, n, k, Configuration::GetBlockSize());
  } else {
    for (uint32_t i = 1; i < n; ++i) {
      memcpy(mCurrentGroupMeta.data + i * Configuration::GetBlockSize(),
             mCurrentGroupMeta.data + 0 * Configuration::GetBlockSize(),
             Configuration::GetBlockSize());
    }
  }

  for (int i = 0; i < n; ++i) {
    uint32_t zid = Configuration::CalculateDiskId(
        mPos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.n);
    mCurrentGroupMeta.slots[i].Clear();
    mCurrentGroupMeta.slots[i].available = false;
    mCurrentGroupMeta.slots[i].targetBytes = Configuration::GetBlockSize();
    mCurrentGroupMeta.slots[i].append = false;
    mCurrentGroupMeta.slots[i].ctrl = mRaidController;
    mCurrentGroupMeta.slots[i].segment = this;
    mCurrentGroupMeta.slots[i].zoneId = zid;
    mCurrentGroupMeta.slots[i].stripeId = Configuration::GetSyncGroupSize() - 1;
    mCurrentGroupMeta.slots[i].offset = mPos;
    mCurrentGroupMeta.slots[i].type = STRIPE_UNIT;
    mCurrentGroupMeta.slots[i].status = WRITE_REAPING;
    mCurrentGroupMeta.slots[i].lba = ~0ull;
    mCurrentGroupMeta.slots[i].data = mCurrentGroupMeta.data;
    mCurrentGroupMeta.slots[i].meta = mCurrentGroupMeta.metadata + i * Configuration::GetMetadataSize();
    {
      BlockMetadata *meta = (BlockMetadata *)mCurrentGroupMeta.slots[i].meta;
      meta->fields.protectedField.lba = ~0ull;
      meta->fields.protectedField.timestamp = 0;
      meta->fields.nonProtectedField.stripeId = Configuration::GetSyncGroupSize() - 1;
    }
    mZones[zid]->Write(mPos, Configuration::GetBlockSize(), &mCurrentGroupMeta.slots[i]);
  }
  mPos += 1;
}

bool Segment::NeedGroupMeta()
{
  // We need to issue Zone Write in the next stripe
  // Due to the header region size, [1, GroupSize] is a group
  // and the stripe with ID of GroupSize is the Zone Write
  return mPos % Configuration::GetSyncGroupSize() == 0;
}

bool Segment::hasGroupMetaDone()
{
  bool done = true;
  for (int i = 0; i < mSegmentMeta.n; ++i) {
    if (!mCurrentGroupMeta.slots[i].available) {
       done = false;
    }
  }
  return done;
}

void Segment::Reset(RequestContext *ctx)
{
  mResetContext.resize(mSegmentMeta.n);
  for (int i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *context = &mResetContext[i];
    context->Clear();
    context->associatedRequest = ctx;
    context->ctrl = mRaidController;
    context->segment = this;
    context->type = STRIPE_UNIT;
    context->status = RESET_REAPING;
    context->available = false;
    mZones[i]->Reset(context);
  }
}

bool Segment::IsResetDone()
{
  bool done = true;
  for (auto context : mResetContext) {
    if (!context.available) {
      done = false;
      break;
    }
  }
  return done;
}

void Segment::Seal()
{
  if (mCurStripe->ioContext.empty()) {
    mCurStripe->ioContext.resize(mSegmentMeta.n);
    for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
      mCurStripe->ioContext[i] = mRequestContextPool->GetRequestContext(true);
    }
  }
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
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

void b()
{}

void Segment::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;
  uint32_t realOffsets[n];

  if (mode == GROUP_LAYOUT || mode == ZAPRAID) {
    uint32_t groupId = (offset - mHeaderRegionSize) / Configuration::GetSyncGroupSize();
    uint32_t searchBegin = groupId * Configuration::GetSyncGroupSize();
    uint32_t searchEnd = searchBegin + Configuration::GetSyncGroupSize();

    // Find out the stripeId of the requested block
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;

    uint32_t stripeId;
    if (Configuration::GetSyncGroupSize() <= 256) {
      stripeId = mCompactStripeTable[index];
    } else {
      stripeId = ((uint16_t*)mCompactStripeTable)[index];
    }

    ctx->stripeId = stripeId;

    // Search the stripe ID table
    for (uint32_t i = 0; i < n; ++i) {
      realOffsets[i] = ~0u;
      if (i == zoneId) {
        realOffsets[i] = offset;
        continue;
      }
      for (uint32_t j = searchBegin; j < searchEnd; ++j) {
        uint32_t index = i * mDataRegionSize + j;
        uint32_t stripeIdOfCurBlock = 0;
        if (Configuration::GetSyncGroupSize() <= 256) {
          stripeIdOfCurBlock = mCompactStripeTable[index];
        } else {
          stripeIdOfCurBlock = ((uint16_t*)mCompactStripeTable)[index];
        }
        if (stripeIdOfCurBlock == stripeId) {
          if (realOffsets[i] != ~0u) {
            printf("Duplicate stripe ID!\n");
          }
          realOffsets[i] = mHeaderRegionSize + j;
        }
      }
      if (realOffsets[i] == ~0u) printf("Not find the stripe ID!\n");
    }
  } else if (mode == ZONEWRITE_ONLY) {
    for (uint32_t i = 0; i < n; ++i) {
      realOffsets[i] = offset;
    }
  }

  ctx->successBytes = 0;
  ctx->targetBytes = k * Configuration::GetStripeUnitSize();
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < n && cnt < k; ++i) {
    if (i == zoneId) continue;

    RequestContext *reqCtx = nullptr;
    reqCtx = mRequestContextPool->GetRequestContext(true);
    readContext->ioContext.emplace_back(reqCtx);

    reqCtx->Clear();
    reqCtx->associatedRequest = ctx;
    reqCtx->status = DEGRADED_READ_SUB;
    reqCtx->type = STRIPE_UNIT;
    reqCtx->targetBytes = Configuration::GetStripeUnitSize();
    reqCtx->ctrl = mRaidController;
    reqCtx->segment = this;
    reqCtx->zoneId = i;
    reqCtx->offset = realOffsets[i];
    reqCtx->data = reqCtx->dataBuffer;
    reqCtx->meta = reqCtx->metadataBuffer;

    readContext->data[i] = reqCtx->data;

    mZones[i]->Read(realOffsets[i], Configuration::GetStripeUnitSize(), reqCtx);

    ++cnt;
  }
}

void Segment::WriteComplete(RequestContext *ctx)
{
  if (ctx->offset < mHeaderRegionSize ||
      ctx->offset >= mHeaderRegionSize + mDataRegionSize) {
    return;
  }

  uint32_t index = ctx->zoneId * mDataRegionSize + ctx->offset - mHeaderRegionSize;
  // Note that here the (lba, timestamp, stripeId) is not the one written to flash page
  // Thus the lba and timestamp is "INVALID" for parity block, and the footer
  // (which uses this field to fill) is valid
  ProtectedBlockMetadata &pbm = mProtectedBlockMetadata[index];
  pbm.lba = ctx->lba;
  pbm.timestamp = ctx->timestamp;
  if (Configuration::GetSyncGroupSize() <= 256) {
    mCompactStripeTable[index] = ctx->stripeId;
  } else {
    ((uint16_t*)mCompactStripeTable)[index] = ctx->stripeId;
  }
  FinishBlock(ctx->zoneId, ctx->offset, ctx->lba);
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

    for (uint32_t i = 1; i < 1 + k; ++i) {
      uint32_t zid = readContext->ioContext[i]->zoneId;
      alive[zid] = true;
    }

    readContext->data[ctx->zoneId] = ctx->data;
    if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
      decodeStripe(offset, readContext->data, alive, n, k, zoneId);
    } else {
      uint32_t groupSize = Configuration::GetSyncGroupSize();
      uint32_t offsetInStripes = mHeaderRegionSize + 
          (offset - mHeaderRegionSize) / groupSize * groupSize + 
          ctx->stripeId;
      decodeStripe(offsetInStripes, readContext->data, alive, n, k, zoneId);
    }
  }

  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = (BlockMetadata*)ctx->meta;
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;
    meta->fields.protectedField.lba = mProtectedBlockMetadata[index].lba;
    meta->fields.protectedField.timestamp = mProtectedBlockMetadata[index].timestamp;
      if (Configuration::GetSyncGroupSize() <= 256) {
      meta->fields.nonProtectedField.stripeId = mCompactStripeTable[index];
    } else {
      meta->fields.nonProtectedField.stripeId = ((uint16_t*)mCompactStripeTable)[index];
    }
  }

  if (parent->type == GC && parent->meta) {
    uint32_t offsetInBlks = (ctx->lba - parent->lba) / Configuration::GetBlockSize();
    BlockMetadata *result = (BlockMetadata*)(ctx->meta);
    BlockMetadata *parentMeta = &((BlockMetadata*)parent->meta)[offsetInBlks];
    parentMeta->fields.protectedField.lba = result->fields.protectedField.lba;
    parentMeta->fields.protectedField.timestamp = result->fields.protectedField.timestamp;
    parentMeta->fields.nonProtectedField.stripeId = result->fields.nonProtectedField.stripeId;
  }
}

void Segment::InvalidateBlock(uint32_t zoneId, uint32_t realOffset)
{
  assert(mNumInvalidBlocks < mNumBlocks);
  mNumInvalidBlocks += 1;
  mValidBits[zoneId * mDataRegionSize + realOffset - mHeaderRegionSize] = 0;
}

void Segment::FinishBlock(uint32_t zoneId, uint32_t offset, uint64_t lba)
{
  if (lba != ~0ull) {
    mNumBlocks += 1;
    mValidBits[zoneId * mDataRegionSize + offset - mHeaderRegionSize] = 1;
  } else {
    mValidBits[zoneId * mDataRegionSize + offset - mHeaderRegionSize] = 0;
  }
}

void Segment::ReleaseZones()
{
  for (auto zone : mZones) {
    zone->Release();
  }
}

bool Segment::CheckOutstandingWrite()
{
  recycleStripeWriteContexts();
  return !mStripeWriteContextPool->NoInflightStripes();
}

uint32_t Segment::GetSegmentId()
{
  return mSegmentMeta.segmentId;
}

uint64_t Segment::GetCapacity() const
{
  return mDataRegionSize;
}

void Segment::ReclaimReadContext(ReadContext *readContext)
{
  mReadContextPool->ReturnContext(readContext);
}

// For recovery
void Segment::SetSegmentStatus(SegmentStatus status)
{
  mSegmentStatus = status;
}

void Segment::RecoverLoadAllBlocks()
{
  mDataBufferForRecovery = (uint8_t*)spdk_zmalloc(
      mDataRegionSize * mZonesWpForRecovery.size() * Configuration::GetBlockSize(),
      Configuration::GetBlockSize(),
      NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

  mMetadataBufferForRecovery = (uint8_t*)spdk_zmalloc(
      mDataRegionSize * mZonesWpForRecovery.size() * Configuration::GetMetadataSize(),
      Configuration::GetBlockSize(),
      NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    printf("Read blocks from zone %u\n", i);
    Zone *zone = mZones[i];
    uint32_t wp = mZonesWpForRecovery[i].first;
    uint32_t zslba = zone->GetSlba();
    for (uint32_t offset = 0; offset < wp - zslba - mHeaderRegionSize; offset += 512) {
      bool done = false;
      if (spdk_nvme_ns_cmd_read_with_md(zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            mDataBufferForRecovery + i * mDataRegionSize * Configuration::GetBlockSize() + offset * Configuration::GetBlockSize(),
            mMetadataBufferForRecovery + i * mDataRegionSize * Configuration::GetMetadataSize() + offset * Configuration::GetMetadataSize(),
            zslba + mHeaderRegionSize + offset,
            std::min(512u, wp - zslba - mHeaderRegionSize - offset),
            complete, &done, 0, 0, 0) < 0) {
        fprintf(stderr, "Read error in loading all blocks.\n");
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }

    for (uint32_t j = 0; j < wp - zslba - mHeaderRegionSize; ++j) {
      uint32_t index = i * mDataRegionSize + j;
      BlockMetadata *blockMeta = &((BlockMetadata*)mMetadataBufferForRecovery)[index];
      uint64_t lba = blockMeta->fields.protectedField.lba;
      uint64_t writeTimestamp = blockMeta->fields.protectedField.timestamp;
      uint16_t stripeId = blockMeta->fields.nonProtectedField.stripeId;
      uint32_t stripeOffset = ~0u;

      if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
        stripeOffset = mHeaderRegionSize + j;
      } else {
        uint32_t groupSize = Configuration::GetSyncGroupSize();
        stripeOffset = mHeaderRegionSize + j / groupSize * groupSize + stripeId;
      }

      uint32_t whichBlock = 0;
      for ( ; whichBlock < mZones.size(); ++whichBlock) {
        uint32_t driveId = Configuration::CalculateDiskId(
            stripeOffset, whichBlock, Configuration::GetRaidLevel(), mZones.size());
        if (driveId == i) {
          break;
        }
      }

      if (whichBlock == mZones.size() - 1) {
        // parity; another case is about stripe metadata, which stores ~0ull already
        mProtectedBlockMetadata[index].lba = ~0ull;
        mProtectedBlockMetadata[index].timestamp = 0;
      } else {
        mProtectedBlockMetadata[index].lba = blockMeta->fields.protectedField.lba;
        mProtectedBlockMetadata[index].timestamp = blockMeta->fields.protectedField.timestamp;
      }
      if (Configuration::GetSyncGroupSize() <= 256) {
        mCompactStripeTable[index] = stripeId;
      } else {
        ((uint16_t*)mCompactStripeTable)[index] = stripeId;
      }
    }
  }
}

bool Segment::RecoverFooterRegionIfNeeded()
{
  bool needed = false;
  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint64_t wp = mZonesWpForRecovery[i].first - zslba;

    if (wp >= mHeaderRegionSize + mDataRegionSize) {
      needed = true;
    }
  }

  if (!needed) {
    return false;
  }

  uint8_t *footerBlock = (uint8_t*)spdk_zmalloc(Configuration::GetBlockSize(), Configuration::GetBlockSize(),
                                      NULL, SPDK_ENV_SOCKET_ID_ANY,
                                      SPDK_MALLOC_DMA);
  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint64_t wp = mZonesWpForRecovery[i].first - zslba;

    for (uint32_t pos = wp; pos < mHeaderRegionSize + mDataRegionSize + mFooterRegionSize; ++pos) {
      uint32_t begin = (pos - 1 - mHeaderRegionSize - mDataRegionSize) * Configuration::GetBlockSize() / 16;
      uint32_t end = std::min(mDataRegionSize, begin + Configuration::GetBlockSize() / 16);

      uint32_t base = i * mDataRegionSize;
      uint32_t posInBlk = 0;
      bool done = false;
      for (uint32_t offset = begin; offset < end; ++offset) {
        footerBlock[posInBlk++] = mProtectedBlockMetadata[base + offset].lba;
        footerBlock[posInBlk++] = mProtectedBlockMetadata[base + offset].timestamp;
      }
      if (spdk_nvme_ns_cmd_write(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          footerBlock, pos, 1, complete, &done, 0) != 0) {
        fprintf(stderr, "Write error in recovering footer region.\n");
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
    bool done = false;
    if (spdk_nvme_zns_finish_zone(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          zslba, 0, complete, &done) != 0) {
      fprintf(stderr, "Seal error in recovering footer region.\n");
    }
    while (!done) {
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
  }

  spdk_free(footerBlock);
  return true;
}

void Segment::RecoverIndexFromSealedSegment(
    uint8_t *buffer, std::pair<uint64_t, PhysicalAddr> *indexMap)
{
  for (uint32_t zid = 0; zid < mZones.size(); ++zid) {
    Zone *zone = mZones[zid];
    for (uint32_t offset = 0; offset < mFooterRegionSize; offset += 512) {
      bool done = false;
      spdk_nvme_ns_cmd_read(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          buffer + offset * Configuration::GetBlockSize(),
          zone->GetSlba() + mHeaderRegionSize + mDataRegionSize + offset,
          std::min(512u, mFooterRegionSize - offset), complete, &done, 0);
      while (!done) {
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }

    uint64_t *footerBlock = reinterpret_cast<uint64_t*>(buffer);
    uint32_t pos = 0;
    for (uint32_t offset = 0; offset < mDataRegionSize; ++offset) {
      uint64_t lba = footerBlock[pos++];
      uint64_t writeTimestamp = footerBlock[pos++];
      PhysicalAddr pba;
      pba.segment = this;
      pba.zoneId = zid;
      pba.offset = mHeaderRegionSize + offset;
      
      if (lba == ~0ull) {
        continue;
      }

      if (indexMap[lba / Configuration::GetBlockSize()].second.segment == nullptr
          || writeTimestamp > indexMap[lba / Configuration::GetBlockSize()].first) {
        indexMap[lba / Configuration::GetBlockSize()] = std::make_pair(writeTimestamp, pba);
      }
    }
  }

  uint32_t zid = 0;
  uint32_t groupSize = Configuration::GetSyncGroupSize();
  uint32_t counter = 0;
  for (uint32_t offset = 0; offset < mDataRegionSize; offset += groupSize) {
    Zone *zone = mZones[zid];
    zid = (zid + 1) % mZones.size();
    // update compact stripe table
    uint32_t stripeMetaOffset = offset + groupSize - 1;
    spdk_nvme_ns_cmd_read(zone->GetDevice()->GetNamespace(),
        zone->GetDevice()->GetIoQueue(0),
        buffer + offset / groupSize * Configuration::GetBlockSize(),
        zone->GetSlba() + mHeaderRegionSize + stripeMetaOffset,
        1, completeOneEvent, &counter, 0);
    counter += 1;

    if (counter == 256 * mZones.size()) {
      while (counter != 0) {
        for (uint32_t i = 0; i < mZones.size(); ++i) {
          spdk_nvme_qpair_process_completions(mZones[i]->GetDevice()->GetIoQueue(0), 0);
        }
      }
    }
  }

  while (counter != 0) {
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      spdk_nvme_qpair_process_completions(mZones[i]->GetDevice()->GetIoQueue(0), 0);
    }
  }

  for (uint32_t offset = 0; offset < mDataRegionSize; offset += Configuration::GetSyncGroupSize()) {
    uint32_t groupId = offset / groupSize;
    uint16_t *array = reinterpret_cast<uint16_t*>(buffer + offset / groupSize * Configuration::GetBlockSize());
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      for (uint32_t j = 0; j < Configuration::GetSyncGroupSize(); ++j) {
        uint32_t index1 = i * mDataRegionSize + offset + j;
        uint32_t index2 = i * groupSize + j;
        if (groupSize <= 256) {
          mCompactStripeTable[index1] = array[index2];
        } else {
          ((uint16_t*)mCompactStripeTable)[index1] = array[index2];
        }
      }
    }
  }

  mPos = mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;

  delete []mAdminStripe->data;
  delete []mAdminStripe->metadata;
  delete mAdminStripe;
  spdk_free(mCurrentGroupMeta.data);
  spdk_free(mCurrentGroupMeta.metadata);
  if (Configuration::GetDeviceSupportMetadata()) {
    delete []mProtectedBlockMetadata;
  }
}

void Segment::RecoverIndexFromOpenSegment(
    std::pair<uint64_t, PhysicalAddr> *indexMap)
{
  // The block must have been brought into the memory and
  // mProtectedBlockMetadata is revived accordingly
  for (uint32_t zid = 0; zid < mZones.size(); ++zid) {
    for (uint32_t offset = 0; offset < mPos; ++offset) {
      uint32_t index = zid * mDataRegionSize + offset;
      uint64_t lba = mProtectedBlockMetadata[index].lba;
      uint64_t writeTimestamp = mProtectedBlockMetadata[index].timestamp;

      PhysicalAddr pba;
      pba.segment = this;
      pba.zoneId = zid;
      pba.offset = mHeaderRegionSize + offset;
      
      if (lba == ~0ull) {
        continue;
      }
      if (indexMap[lba / Configuration::GetBlockSize()].second.segment == nullptr
          || writeTimestamp > indexMap[lba / Configuration::GetBlockSize()].first) {
        indexMap[lba / Configuration::GetBlockSize()] = std::make_pair(writeTimestamp, pba);
      }
    }
  }
}

bool Segment::RecoverNeedRewrite()
{
  // stripe ID to blocks
  uint32_t checkpoint = ~0u;
  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint32_t wp = mZonesWpForRecovery[i].first - zslba;

    uint32_t begin = (wp - mHeaderRegionSize) / 
      Configuration::GetSyncGroupSize() *
      Configuration::GetSyncGroupSize();
    uint32_t end = begin + Configuration::GetSyncGroupSize();
    if (checkpoint == ~0) {
      checkpoint = begin;
    } else {
      if (checkpoint != begin) {
        printf("Error in the checkpoint during need rewrite.\n");
      }
    }

    for (uint32_t offset = begin; offset < end; ++offset) {
      uint32_t index = i * mDataRegionSize + offset;
      uint32_t stripeId = ~0u;
      if (Configuration::GetSyncGroupSize() <= 256) {
        stripeId = mCompactStripeTable[index];
      } else {
        stripeId = ((uint16_t*)mCompactStripeTable)[index];
      }
      if (mStripesToRecover.find(stripeId) == mStripesToRecover.end()) {
        mStripesToRecover[stripeId].resize(mZones.size());
        for (uint32_t j = 0; j < mZones.size(); ++j) {
          mStripesToRecover[stripeId][j] = ~0u;
        }
      }
      mStripesToRecover[stripeId][i] = offset;
    }
  }

  bool needRewrite = false;
  for (auto stripe : mStripesToRecover) {
    uint32_t stripeId = stripe.first;
    const auto &blocks = stripe.second;

    bool missingDataBlock = false;
    bool havingParityBlock = false;

    for (uint32_t i = 0; i < mZones.size(); ++i) {
      uint32_t driveId = Configuration::CalculateDiskId(
          mHeaderRegionSize + checkpoint + stripeId, i,
          Configuration::GetRaidLevel(), mZones.size());
      if (i != mZones.size() - 1 && blocks[driveId] == ~0u) {
        missingDataBlock = true; 
      }
      if (i == mZones.size() - 1 && blocks[driveId] != ~0u) {
        havingParityBlock = true;
      }
    }

    if (missingDataBlock && havingParityBlock) {
      needRewrite = true;
    }
  }
  return needRewrite;
}

void Segment::RecoverInflightStripes()
{
  // Then, rewrite all valid blocks
  uint32_t checkpoint = ~0u;
  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint32_t wp = mZonesWpForRecovery[i].first - zslba;
    uint32_t lastCheckpoint = (wp - mHeaderRegionSize) / 
        Configuration::GetSyncGroupSize() *
        Configuration::GetSyncGroupSize();

    if (checkpoint == ~0) {
      checkpoint = lastCheckpoint;
    } else if (checkpoint != lastCheckpoint) {
      printf("Critical error: checkpoint not match.\n");
    }
  }

  uint8_t *dataStripe[mZones.size()];
  uint8_t *metadataStripe[mZones.size()];
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    dataStripe[i] = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(),
        Configuration::GetBlockSize(),
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    metadataStripe[i] = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(),
        Configuration::GetBlockSize(),
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  }

  // For blocks in the last group
  uint32_t maxStripeId = 0;
  for (auto stripe : mStripesToRecover) {
    uint32_t stripeId = stripe.first;
    const auto &blocks = stripe.second;
    uint32_t counter = 0;

    if (stripeId > maxStripeId) {
      maxStripeId = stripeId;
    }

    for (uint32_t i = 0; i < mZones.size(); ++i) {
      uint32_t driveId = Configuration::CalculateDiskId(
          mHeaderRegionSize + checkpoint + stripeId, i,
          Configuration::GetRaidLevel(), mZones.size());

      BlockMetadata *blockMeta = reinterpret_cast<BlockMetadata*>(metadataStripe[i]);
      if (blocks[driveId] != ~0u) {
        memcpy(dataStripe[i],
            mDataBufferForRecovery + blocks[driveId] * Configuration::GetBlockSize(),
            Configuration::GetBlockSize());
        memcpy(metadataStripe[i],
            mMetadataBufferForRecovery + blocks[driveId] * Configuration::GetMetadataSize(),
            Configuration::GetMetadataSize());
      } else {
        if (stripeId == Configuration::GetSyncGroupSize() - 1) {
          // a stripe metadata; and we assume replication here
          for (uint32_t t = 0; t < mZones.size(); ++t) {
            if (blocks[t] != ~0u) {
              memcpy(dataStripe[i], mDataBufferForRecovery + blocks[t] * Configuration::GetBlockSize(),
                  sizeof(Configuration::GetBlockSize()));
              break;
            }
          }
          blockMeta->fields.protectedField.lba = ~0ull;
          blockMeta->fields.protectedField.timestamp = 0;
        } else {
          if (i != mZones.size() - 1) {
            memset(dataStripe[i], 0, sizeof(Configuration::GetBlockSize()));
            blockMeta->fields.protectedField.lba = ~0ull;
            blockMeta->fields.protectedField.timestamp = 0;
          } else {
            encodeStripe(dataStripe, mSegmentMeta.n, mSegmentMeta.k, Configuration::GetBlockSize());
            encodeStripe(metadataStripe, mSegmentMeta.n, mSegmentMeta.k, 16); // 16 bytes for the protected fields
          }
        }
        blockMeta->fields.nonProtectedField.stripeId = stripeId;

        Zone *zone = mZones[driveId];
        if (spdk_nvme_ns_cmd_write_with_md(
              zone->GetDevice()->GetNamespace(),
              zone->GetDevice()->GetIoQueue(0),
              dataStripe[i], metadataStripe[i],
              mZonesWpForRecovery[driveId].first,
              1, completeOneEvent, &counter, 0, 0, 0) != 0) {
          fprintf(stderr, "Write error in recovering inflight stripes.\n");
        }
        counter += 1;

        printf("Repair: block %u with stripe %u to the drive %u with pos %lu\n",
            i, stripeId, driveId, mZonesWpForRecovery[driveId].first);
        uint32_t index = driveId * mDataRegionSize + mZonesWpForRecovery[driveId].first;
        mZonesWpForRecovery[driveId].first += 1;
        if (Configuration::GetSyncGroupSize() <= 256) {
          mCompactStripeTable[index] = stripeId;
        } else {
          ((uint16_t*)mCompactStripeTable)[index] = stripeId;
        }
      }
    }

    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
  }

  mPos = mHeaderRegionSize + checkpoint + maxStripeId + 1;

  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else if (mPos == Configuration::GetSyncGroupSize() - 1) {
    mSegmentStatus = SEGMENT_PREPARE_STRIPE_META;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
  }

  for (uint32_t i = 0; i < mZones.size(); ++i) {
    spdk_free(dataStripe[i]);
    spdk_free(metadataStripe[i]);
  }
}

void Segment::RecoverFromOldSegment(Segment *oldSegment)
{
  // First, write the header region
  uint32_t counter = mZones.size();
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    Zone *zone = mZones[i];
    memcpy(mAdminStripe->data + i * Configuration::GetBlockSize(), &mSegmentMeta, sizeof(mSegmentMeta));

    if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            mAdminStripe->data + i * Configuration::GetBlockSize(),
            mAdminStripe->metadata + i * Configuration::GetMetadataSize(),
            0, 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
      fprintf(stderr, "Write error in writing the header for new segment.\n");
    }
  }

  while (counter != 0) {
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      Zone *zone = mZones[i];
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
  }

  // Then, rewrite all valid blocks
  uint32_t checkpoint = ~0u;
  for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint32_t wp = mZonesWpForRecovery[i].first - zslba;
    uint32_t lastCheckpoint = (wp - mHeaderRegionSize) / 
        Configuration::GetSyncGroupSize() *
        Configuration::GetSyncGroupSize();

    if (checkpoint == ~0) {
      checkpoint = lastCheckpoint;
    } else if (checkpoint != lastCheckpoint) {
      printf("Critical error: checkpoint not match.\n");
    }
  }

  // Blocks (both data and parity) before the checkpoint can be kept as is.
  for (uint32_t i = 0; i < checkpoint; ++i) {
    Zone *zone = mZones[i];
    uint32_t counter = mZones.size();
    for (uint32_t j = 0; j < mZones.size(); ++j) {
      uint32_t index = j * mDataRegionSize + i;
      if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            oldSegment->mDataBufferForRecovery + index * Configuration::GetBlockSize(),
            oldSegment->mMetadataBufferForRecovery + index * Configuration::GetMetadataSize(),
            zone->GetSlba() + mHeaderRegionSize + i, 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in writing the previous groups for new segment.\n");
      }
      mProtectedBlockMetadata[index].lba = oldSegment->mProtectedBlockMetadata[index].lba;
      mProtectedBlockMetadata[index].timestamp = oldSegment->mProtectedBlockMetadata[index].timestamp;
      if (Configuration::GetSyncGroupSize() <= 256) {
        mCompactStripeTable[index] = oldSegment->mCompactStripeTable[index];
      } else {
        ((uint16_t*)mCompactStripeTable)[index] = ((uint16_t*)(oldSegment->mCompactStripeTable))[index];
      }
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
  }

  uint8_t *dataStripe[mZones.size()];
  uint8_t *metadataStripe[mZones.size()];
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    dataStripe[i] = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(),
        Configuration::GetBlockSize(),
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    metadataStripe[i] = (uint8_t*)spdk_zmalloc(
        Configuration::GetBlockSize(),
        Configuration::GetBlockSize(),
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  }

  // For blocks in the last group
  uint32_t maxStripeId = 0;
  for (auto stripe : oldSegment->mStripesToRecover) {
    uint32_t stripeId = stripe.first;
    const auto &blocks = stripe.second;
    uint32_t counter = mZones.size();

    if (stripeId > maxStripeId) {
      stripeId = maxStripeId;
    }
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      uint32_t driveId = Configuration::CalculateDiskId(
          mHeaderRegionSize + checkpoint + stripeId, i,
          Configuration::GetRaidLevel(), mZones.size());
      uint32_t index = driveId * mDataRegionSize + checkpoint + stripeId;

      BlockMetadata *blockMeta = reinterpret_cast<BlockMetadata*>(metadataStripe[i]);
      if (i != mZones.size() - 1) {
        if (blocks[driveId] != ~0u) {
          memcpy(dataStripe[i],
                 oldSegment->mDataBufferForRecovery + blocks[driveId] * Configuration::GetBlockSize(),
                 Configuration::GetBlockSize());
          memcpy(metadataStripe[i],
                 oldSegment->mMetadataBufferForRecovery + blocks[driveId] * Configuration::GetMetadataSize(),
                 Configuration::GetMetadataSize());
        } else {
          blockMeta->fields.protectedField.lba = ~0ull;
          blockMeta->fields.protectedField.timestamp = 0;
        }
        mProtectedBlockMetadata[index].lba = blockMeta->fields.protectedField.lba;
        mProtectedBlockMetadata[index].timestamp = blockMeta->fields.protectedField.timestamp;
      } else {
        // parity block
        if (blocks[driveId] != ~0u) {
          // If the parity block is not missing, all the data blocks must be not missing
          // because we are already at rewriting
          memcpy(dataStripe[i],
                 oldSegment->mDataBufferForRecovery + blocks[driveId] * Configuration::GetBlockSize(),
                 Configuration::GetBlockSize());
          memcpy(metadataStripe[i],
                 oldSegment->mMetadataBufferForRecovery + blocks[driveId] * Configuration::GetMetadataSize(),
                 Configuration::GetMetadataSize());
        } else {
          // generate new parity
          encodeStripe(dataStripe, mSegmentMeta.n, mSegmentMeta.k, Configuration::GetBlockSize());
          encodeStripe(metadataStripe, mSegmentMeta.n, mSegmentMeta.k, 16); // 16 bytes for the protected fields
        }
        mProtectedBlockMetadata[index].lba = ~0ull;
        mProtectedBlockMetadata[index].timestamp = 0;
      }
      blockMeta->fields.nonProtectedField.stripeId = stripeId;
      if (Configuration::GetSyncGroupSize() <= 256) {
        mCompactStripeTable[index] = stripeId;
      } else {
        ((uint16_t*)mCompactStripeTable)[index] = stripeId;
      }

      Zone *zone = mZones[driveId];
      if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            dataStripe[i], metadataStripe[i],
            zone->GetSlba() + mHeaderRegionSize + checkpoint + stripeId,
            1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in rewriting the last group in the new segment.\n");
      }
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
  }

  mPos = maxStripeId + 1;
  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else if (mPos == Configuration::GetSyncGroupSize() - 1) {
    mSegmentStatus = SEGMENT_PREPARE_STRIPE_META;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
  }

  for (uint32_t i = 0; i < mZones.size(); ++i) {
    spdk_free(dataStripe[i]);
    spdk_free(metadataStripe[i]);
  }
}

void Segment::SetZonesAndWpForRecovery(std::vector<std::pair<uint64_t, uint8_t*>> zonesWp)
{
  mZonesWpForRecovery = zonesWp;
}

void Segment::ResetInRecovery()
{
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    Zone *zone = mZones[i];
    bool done = false;
    if (spdk_nvme_zns_reset_zone(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          zone->GetSlba(), 0, complete, &done) != 0) {
      fprintf(stderr, "Reset error in recovering.\n");
    }
    while (!done) {
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
  }
}

void Segment::FinishRecovery()
{
  // 
  uint32_t numBlocks = 0;
  if (mSegmentStatus == SEGMENT_SEALED) {
    numBlocks = (mDataRegionSize - mDataRegionSize / Configuration::GetSyncGroupSize()) * mSegmentMeta.k;
  } else {
    numBlocks = (mPos - mHeaderRegionSize - 
        (mPos - mHeaderRegionSize) / Configuration::GetSyncGroupSize())
      * mSegmentMeta.k;
  }

  mNumInvalidBlocks = numBlocks - mNumBlocks;
  mNumBlocks = numBlocks;
  return;
}


uint32_t Segment::GetPos() {
  return mPos;
}

void Segment::Dump() {
  printf("Segment ID: %lu, n: %u, k: %u, numZones: %u, raidScheme: %u, position: %u, stripe offset: %u\n",
      mSegmentMeta.segmentId,
      mSegmentMeta.n,
      mSegmentMeta.k,
      mSegmentMeta.numZones,
      mSegmentMeta.raidScheme,
      mPos, mPosInStripe);
  printf("\tZones: ");
  for (uint32_t i = 0; i < mSegmentMeta.numZones; ++i) {
    printf("%lu ", mSegmentMeta.zones[i]);
  }
  printf("\n\tnumBlocks: %u, numInvalidBlocks: %u\n", mNumBlocks, mNumInvalidBlocks);
}
