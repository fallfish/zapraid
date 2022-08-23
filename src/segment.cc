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
  struct timeval s, e;
  gettimeofday(&s, NULL);
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

  mCurrentNamedGroupMetadata.data = (uint8_t*)spdk_zmalloc(
        Configuration::GetStripeSize(), 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);
  mCurrentNamedGroupMetadata.metadata = (uint8_t*)spdk_zmalloc(
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
  mCompactStripeTable = new uint8_t[mSegmentMeta.n * mDataRegionSize];
  memset(mCompactStripeTable, 0, mSegmentMeta.n * mDataRegionSize);
  mProtectedBlockMetadata = new ProtectedBlockMetadata[mSegmentMeta.n * mDataRegionSize];
  memset(mProtectedBlockMetadata, 0, mSegmentMeta.n * mDataRegionSize);

  mSegmentStatus = SEGMENT_NORMAL;
}

Segment::~Segment()
{
  // TODO: reclaim zones to devices
  delete mValidBits;
  delete mCompactStripeTable;
  delete mProtectedBlockMetadata;
  spdk_free(mCurrentNamedGroupMetadata.data);
  spdk_free(mCurrentNamedGroupMetadata.metadata);
  // delete mValidBits;
}

void Segment::recycleStripeWriteContexts()
{
  mStripeWriteContextPool->Recycle();
}

void Segment::recycleReadContexts()
{
  mReadContextPool->Recycle();
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
  return mPos == mHeaderRegionSize + mDataRegionSize;
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
  recycleReadContexts();
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
    if (IsFull()) {
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    }
  }

  if (mSegmentStatus == SEGMENT_PREPARE_ZAPRAID) {
    // under SyncPoint, waiting for previous appends
    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
      if (Configuration::GetSystemMode() == ZAPRAID) {
        issueNamedMetadata();
        mSegmentStatus = SEGMENT_WRITING_ZAPRAID;
      } else {
        mSegmentStatus = SEGMENT_NORMAL;
      }
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_ZAPRAID) {
    if (Configuration::GetSystemMode() == ZAPRAID) {
      if (hasNamedMetadataDone()) {
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
    if (checkStripeAvailable(mCurStripe)) {
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

  for (uint32_t i = 0; i < mZones.size(); ++i) {
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
  for (uint32_t zid = 0; zid < mSegmentMeta.numZones; ++zid) {
    uint32_t base = zid * mDataRegionSize;
    uint64_t *footerBlock = reinterpret_cast<uint64_t*>(mCurStripe->data[zid]);
    uint32_t pos = 0;
    for (uint32_t offset = begin; offset < end; ++offset) {
      footerBlock[pos++] = mProtectedBlockMetadata[base + offset].lba;
      footerBlock[pos++] = mProtectedBlockMetadata[base + offset].timestamp;
    }
  }
  encodeStripe(mCurStripe->data, mSegmentMeta.n, mSegmentMeta.k, Configuration::GetBlockSize());
  mCurStripe->successBytes = 0;
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        mPos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
    RequestContext *slot = mCurStripe->ioContext[i];
    slot->status = WRITE_REAPING;
    slot->successBytes = 0;
    slot->offset = mPos;
    slot->zoneId = zoneId;
    mZones[zoneId]->Write(mPos, Configuration::GetBlockSize(), (void*)slot);
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
    if (ctx->type == USER) {
      slot->data = slot->dataBuffer;
      memcpy(slot->data, blkdata, blockSize);
    } else {
      slot->data = blkdata;
    }
    slot->meta = slot->metadataBuffer;

    slot->associatedStripe = mCurStripe;
    slot->associatedRequest = ctx;
    slot->targetBytes = blockSize;
    slot->lba = lba;
    slot->zoneId = zoneId;
    slot->stripeId = mPos % Configuration::GetSyncGroupSize();
    slot->segment = this;
    slot->ctrl = mRaidController;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;

    // Initialize block (flash page) metadata
    BlockMetadata *blkMeta = (BlockMetadata *)slot->meta;
    blkMeta->fields.protectedField.lba = slot->lba;
    blkMeta->fields.protectedField.timestamp = 0;
    blkMeta->fields.nonProtectedField.stripeId = slot->stripeId;

    if (mode == ZONE_WRITE) {
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
        && needNamedMetadata()) {
      // writing the stripe metadata at the end of each group
      mSegmentStatus = SEGMENT_PREPARE_ZAPRAID;
    }

    if (mode == GROUP_LAYOUT && (mPos + 1) % Configuration::GetSyncGroupSize() == 0) {
      // The next stripe is the begin of the next group
      mSegmentStatus = SEGMENT_WRITING_ZAPRAID;
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
  for (uint32_t i = 0; i < n; ++i) {
    stripeData[i] = stripe->ioContext[i]->data;
    stripeProtectedMetadata[i] = reinterpret_cast<uint8_t*>(
        &(((BlockMetadata*)stripe->ioContext[i]->meta)->fields.protectedField));
  }
  encodeStripe(stripeData, n, k, Configuration::GetBlockSize());
  encodeStripe(stripeProtectedMetadata, n, k, 16); // 16 bytes for the protected fields

  for (uint32_t i = k; i < n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        zonePos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
    RequestContext *slot = stripe->ioContext[i];
    slot->lba = ~0ull;
    slot->ctrl = mRaidController;
    slot->segment = this;
    slot->zoneId = zoneId;
    slot->stripeId = zonePos % Configuration::GetSyncGroupSize();
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    if (mode == ZONE_WRITE) {
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
  slot->stripeId = phyAddr.stripeId;
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

void Segment::UpdateNamedMetadata(uint32_t zoneId, uint32_t stripeId, uint32_t offset)
{
  uint32_t syncGroupSize = Configuration::GetSyncGroupSize();
  uint16_t *mapping = (uint16_t*)mCurrentNamedGroupMetadata.data + zoneId * syncGroupSize;
  mapping[offset % syncGroupSize] = stripeId;
}

void Segment::issueNamedMetadata()
{
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  uint8_t *stripe[n];
  for (uint32_t i = 0; i < n; ++i) {
    stripe[i] = mCurrentNamedGroupMetadata.data + i * Configuration::GetBlockSize();
  }

  bool isSingleBlock = (sizeof(uint16_t) * Configuration::GetSyncGroupSize() * n <= 4096);
  if (!isSingleBlock) {
    encodeStripe(stripe, n, k, Configuration::GetBlockSize());
  }

  for (int i = 0; i < n; ++i) {
    uint32_t zid = Configuration::CalculateDiskId(
        mPos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.n);
    mCurrentNamedGroupMetadata.slots[i].Clear();
    mCurrentNamedGroupMetadata.slots[i].available = false;
    mCurrentNamedGroupMetadata.slots[i].targetBytes = Configuration::GetBlockSize();
    mCurrentNamedGroupMetadata.slots[i].append = false;
    mCurrentNamedGroupMetadata.slots[i].ctrl = mRaidController;
    mCurrentNamedGroupMetadata.slots[i].segment = this;
    mCurrentNamedGroupMetadata.slots[i].zoneId = zid;
    mCurrentNamedGroupMetadata.slots[i].stripeId = 0;
    mCurrentNamedGroupMetadata.slots[i].offset = mPos;
    mCurrentNamedGroupMetadata.slots[i].type = STRIPE_UNIT;
    mCurrentNamedGroupMetadata.slots[i].status = WRITE_REAPING;
    mCurrentNamedGroupMetadata.slots[i].lba = ~0ull;
    mCurrentNamedGroupMetadata.slots[i].data = mCurrentNamedGroupMetadata.data;
    mCurrentNamedGroupMetadata.slots[i].meta = mCurrentNamedGroupMetadata.metadata + i * Configuration::GetMetadataSize();
    {
      BlockMetadata *meta = (BlockMetadata *)mCurrentNamedGroupMetadata.slots[i].meta;
      meta->fields.protectedField.lba = ~0ull;
      meta->fields.protectedField.timestamp = 0;
      meta->fields.nonProtectedField.stripeId = 0;
    }
    mZones[zid]->Write(mPos, Configuration::GetBlockSize(), &mCurrentNamedGroupMetadata.slots[i]);
  }
  mPos += 1;
}

bool Segment::needNamedMetadata()
{
  // We need to issue Zone Write in the next stripe
  // Due to the header region size, [1, GroupSize] is a group
  // and the stripe with ID of GroupSize is the Zone Write
  return mPos % Configuration::GetSyncGroupSize() == 0;
}

bool Segment::hasNamedMetadataDone()
{
  bool done = true;
  for (int i = 0; i < Configuration::GetStripeSize() / Configuration::GetStripeUnitSize(); ++i) {
    if (!mCurrentNamedGroupMetadata.slots[i].available) {
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
    mCurStripe->ioContext.resize(mZones.size());
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      mCurStripe->ioContext[i] = mRequestContextPool->GetRequestContext(true);
    }
  }
  for (uint32_t i = 0; i < mZones.size(); ++i) {
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
  bool isSingleBlock = (sizeof(uint16_t) * Configuration::GetSyncGroupSize() * n <= 4096);

  ReadContext *readContext = ctx->associatedRead;

  // Note that data region begins from offset 1, we need to decrement the offset by 1
  uint32_t offsetOfLastStripeInRequestedGroup = 
      ((offset - 1) | (Configuration::GetSyncGroupSize() - 1)) + 1;
  uint32_t offsetOfLastStripeInCurrentGroup =
      (mPos | (Configuration::GetSyncGroupSize() - 1)) + 1;
  ctx->successBytes = 0;

  if (offsetOfLastStripeInCurrentGroup == offsetOfLastStripeInRequestedGroup) {
    ctx->needDecodeMeta = false;
    ctx->targetBytes = n * Configuration::GetBlockSize();

    for (uint32_t i = 0; i < n; ++i) {
      RequestContext *reqCtx = mRequestContextPool->GetRequestContext(true);
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetBlockSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->segment = this;
      reqCtx->zoneId = 0;
      reqCtx->offset = offsetOfLastStripeInRequestedGroup;
      reqCtx->data = reqCtx->dataBuffer;
      reqCtx->meta = reqCtx->metadataBuffer;

      readContext->data[i] = reqCtx->data;
      readContext->ioContext.emplace_back(reqCtx);

      memcpy(reqCtx->data, mCurrentNamedGroupMetadata.data, Configuration::GetBlockSize());
      reqCtx->successBytes = reqCtx->targetBytes;
      reqCtx->Queue();
    }
  } else {
    ctx->needDecodeMeta = true;
    if (isSingleBlock) {
      ctx->targetBytes = Configuration::GetBlockSize();
    } else {
      ctx->targetBytes = k * Configuration::GetBlockSize();
    }
    uint32_t cnt = 0;
    for (int i = 0; i < n && cnt < k; ++i) {
      if (i == zoneId) continue;
      RequestContext *reqCtx = mRequestContextPool->GetRequestContext(true);
      reqCtx->Clear();
      reqCtx->associatedRequest = ctx;
      reqCtx->status = DEGRADED_READ_SUB;
      reqCtx->type = STRIPE_UNIT;
      reqCtx->targetBytes = Configuration::GetBlockSize();
      reqCtx->ctrl = mRaidController;
      reqCtx->segment = this;
      reqCtx->zoneId = i;
      reqCtx->offset = offsetOfLastStripeInRequestedGroup;
      reqCtx->data = reqCtx->dataBuffer;
      reqCtx->meta = reqCtx->metadataBuffer;

      readContext->data[i] = reqCtx->data;
      readContext->ioContext.emplace_back(reqCtx);

      if ((!isSingleBlock) || (isSingleBlock && cnt == 0)) {
        mZones[i]->Read(reqCtx->offset, Configuration::GetBlockSize(), reqCtx);
      }

      cnt++;
    }
  }
}

void Segment::ReadStripeMemorySufficient(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;

  uint32_t groupId = (offset - mHeaderRegionSize) / Configuration::GetSyncGroupSize();
  uint32_t searchBegin = groupId * Configuration::GetSyncGroupSize();
  uint32_t searchEnd = searchBegin + Configuration::GetSyncGroupSize();

  // Find out the stripeId of the requested block
  uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;
  uint8_t  stripeId = mCompactStripeTable[index];

  uint32_t realOffsets[n];
  // Search the stripe Id table
  for (uint32_t i = 0; i < n; ++i) {
    realOffsets[i] = ~0u;
    if (i == zoneId) {
      realOffsets[i] = offset;
      continue;
    }
    for (uint32_t j = searchBegin; j < searchEnd; ++j) {
      uint32_t index = i * mHeaderRegionSize + j;
      if (mCompactStripeTable[index] == stripeId) {
        if (realOffsets[i] != ~0u) printf("Duplicate stripe ID!\n");
        realOffsets[i] = mCompactStripeTable[index];
      }
    }
    if (realOffsets[i] == ~0u) printf("Not find the stripe ID!\n");
  }

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

void Segment::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;

  if (mode == ZONE_WRITE || mode == ZAPRAID) {
    uint32_t realOffsets[n];
    ctx->targetBytes = k * Configuration::GetStripeUnitSize();
    ctx->successBytes = 0;
    if (mode == ZONE_WRITE) { // PURE_WRITE
      for (uint32_t i = 0; i < n; ++i) {
        realOffsets[i] = offset;
      }
    } else if (mode == ZAPRAID) {
      // decode metadata
      uint16_t offsetMap[Configuration::GetStripeSize() / 2];

      bool isSingleBlock = (sizeof(uint16_t) * Configuration::GetSyncGroupSize() * n <= 4096);
      if (isSingleBlock) {
        if (ctx->needDecodeMeta && zoneId == 0) {
          memcpy((uint8_t *)offsetMap, readContext->data[1], Configuration::GetBlockSize());
        } else {
          memcpy((uint8_t *)offsetMap, readContext->data[0], Configuration::GetBlockSize());
        }
      } else {
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
          uint32_t metaOffset = offset | (Configuration::GetSyncGroupSize() - 1);
          for (uint32_t i = 0; i < n; ++i) {
            uint32_t zid = Configuration::CalculateDiskId(
                metaOffset, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.n);
            memcpy((uint8_t*)offsetMap + i * Configuration::GetBlockSize(),
                readContext->data[zid], Configuration::GetBlockSize());
          }
        } else {
          for (uint32_t i = 0; i < n; ++i) {
            memcpy((uint8_t*)offsetMap + i * Configuration::GetBlockSize(),
                readContext->data[i], Configuration::GetBlockSize());
          }
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
      if (Configuration::GetSystemMode() == ZONE_WRITE) {
        reqCtx = mRequestContextPool->GetRequestContext(true);
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
      reqCtx->data = reqCtx->dataBuffer;
      reqCtx->meta = reqCtx->metadataBuffer;

      readContext->data[i] = reqCtx->data;

      mZones[i]->Read(realOffsets[i], Configuration::GetStripeUnitSize(), reqCtx);

      ++cnt;
    }
  } else if (mode == GROUP_LAYOUT) {
    // Note that the sync group size must be small to prevent from overwhelming the storage
    ctx->targetBytes = k * Configuration::GetStripeUnitSize() * Configuration::GetSyncGroupSize();
    ctx->successBytes = 0;
    uint32_t offsetBegin = offset & (~(Configuration::GetSyncGroupSize() - 1));
    uint32_t offsetEnd = offsetBegin + Configuration::GetSyncGroupSize();

    for (uint32_t i = 0; i < n; ++i) {
      if (i == zoneId) continue;
      for (uint32_t offset = offsetBegin; offset < offsetEnd; ++offset) {
        RequestContext *reqCtx = mRequestContextPool->GetRequestContext(true);
        reqCtx->Clear();
        reqCtx->associatedRequest = ctx;
        reqCtx->status = DEGRADED_READ_SUB;
        reqCtx->type = STRIPE_UNIT;
        reqCtx->targetBytes = Configuration::GetStripeUnitSize();
        reqCtx->ctrl = mRaidController;
        reqCtx->segment = this;
        reqCtx->zoneId = i;
        reqCtx->offset = offset;
        reqCtx->data = reqCtx->dataBuffer;
        reqCtx->meta = reqCtx->metadataBuffer;

        readContext->ioContext.emplace_back(reqCtx);
        mZones[i]->Read(offset, Configuration::GetStripeUnitSize(), reqCtx);
      }
    }
  }

  if (mode == ZONE_APPEND) {
    fprintf(stderr, "Pure zone append does not support recovery.\n");
    exit(-1);
  }
}

void Segment::WriteComplete(RequestContext *ctx)
{
  if (ctx->offset < mHeaderRegionSize ||
      ctx->offset >= mHeaderRegionSize + mDataRegionSize) {
    return;
  }

  uint32_t index = ctx->zoneId * mDataRegionSize + ctx->offset - mHeaderRegionSize;
  if (!Configuration::GetDeviceSupportMetadata()) {
    // Note that here the (lba, timestamp, stripeId) is not the one written to flash page
    // Thus the lba and timestamp is "INVALID" for parity block, and the footer
    // (which uses this field to fill) is valid
    ProtectedBlockMetadata &pbm = mProtectedBlockMetadata[index];
    pbm.lba = ctx->lba;
    pbm.timestamp = ctx->timestamp;
  }
  mCompactStripeTable[index] = ctx->stripeId;
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

    if (Configuration::GetSystemMode() == GROUP_LAYOUT) {
      for (uint32_t i = 1; i < readContext->ioContext.size(); ++i) {
        RequestContext *reqCtx = readContext->ioContext[i];
        BlockMetadata *metadata = (BlockMetadata*)reqCtx->meta;
        if (metadata->fields.nonProtectedField.stripeId == ctx->stripeId) {
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
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;
    meta->fields.protectedField.lba = mProtectedBlockMetadata[index].lba;
    meta->fields.protectedField.timestamp = mProtectedBlockMetadata[index].timestamp;
    meta->fields.nonProtectedField.stripeId = mCompactStripeTable[index];
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

bool Segment::CheckOutstandingRead()
{
  recycleReadContexts();
  return mReadContextPool->inflightContexts.size() == 0;
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
