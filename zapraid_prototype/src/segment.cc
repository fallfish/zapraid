#include "segment.h"

#include <sys/time.h>
#include "raid_controller.h"

Segment::Segment(RAIDController *raidController,
                 uint32_t segmentId,
                 RequestContextPool *ctxPool,
                 ReadContextPool *rctxPool,
                 StripeWriteContextPool *sctxPool)
{
  SystemMode mode = Configuration::GetSystemMode();
  int n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  int k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();

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

  if (Configuration::GetStripeGroupSize() <= 256) { // less than 8 bits
    mCompactStripeTable = new uint8_t[mSegmentMeta.n * mDataRegionSize];
    memset(mCompactStripeTable, 0, mSegmentMeta.n * mDataRegionSize);
  } else if (Configuration::GetStripeGroupSize() <= 65536) { // 16 bits
    mCompactStripeTable = new uint8_t[2 * mSegmentMeta.n * mDataRegionSize];
    memset(mCompactStripeTable, 0, 2 * mSegmentMeta.n * mDataRegionSize);
  } else {
    mCompactStripeTable = new uint8_t[3 * mSegmentMeta.n * mDataRegionSize];
    memset(mCompactStripeTable, 0, 3 * mSegmentMeta.n * mDataRegionSize);
  }

  mCodedBlockMetadata = new CodedBlockMetadata[mSegmentMeta.n * mDataRegionSize];
  memset(mCodedBlockMetadata, 0, mSegmentMeta.n * mDataRegionSize);

  mSegmentStatus = SEGMENT_NORMAL;

  mLastStripeCreationTimestamp = GetTimestampInUs();
}

Segment::~Segment()
{
  // TODO: reclaim zones to devices
  delete []mValidBits;
  delete []mCompactStripeTable;
  if (!Configuration::GetDeviceSupportMetadata()) {
    delete []mCodedBlockMetadata;
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
  double timestamp = GetTimestampInUs();
  bool stateChanged = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    if (mPos == mHeaderRegionSize + mDataRegionSize) {
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    } else {
      if (!Configuration::InjectCrash() && timestamp - mLastStripeCreationTimestamp >= 0.001) {
        // fill and flush the last stripe if no further write request for 200us interval
        FlushCurrentStripe();
      }
    }
  }

  if (mSegmentStatus == SEGMENT_CONCLUDING_APPENDS_IN_GROUP
      || mSegmentStatus == SEGMENT_CONCLUDING_WRITES_IN_GROUP) {
    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
      mSegmentStatus = SEGMENT_NORMAL;
      stateChanged = true;
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
        slot->associatedStripe = mCurStripe;
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
      if (Configuration::GetDeviceSupportMetadata()) {
        delete []mCodedBlockMetadata;
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
  // 4096 / 20 = 204 LBAs
  // 8 bytes for LBA, 8 bytes write timestamp, and 4 bytes stripe ID
  // Each footer block contains blockSize / (8 + 8) entries
  // Thus the offset in the metadata array is "i * blockSize / 16"
  uint32_t begin = (mPos - mHeaderRegionSize - mDataRegionSize) * (Configuration::GetBlockSize() / (8 + 8 + 4));
  uint32_t end = std::min(mDataRegionSize, begin + Configuration::GetBlockSize() / (8 + 8 + 4));
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    uint32_t base = i * mDataRegionSize;
    uint32_t pos = 0;
    for (uint32_t offset = begin; offset < end; ++offset) {
      uint8_t *footerForCurrentBlock = mCurStripe->ioContext[i]->dataBuffer + (offset - begin) * 20;
      *(uint64_t*)(footerForCurrentBlock + 0) = mCodedBlockMetadata[base + offset].lba;
      *(uint64_t*)(footerForCurrentBlock + 8) = mCodedBlockMetadata[base + offset].timestamp;

      uint32_t stripeId;
      if (Configuration::GetStripeGroupSize() <= 256) {
        stripeId = mCompactStripeTable[base + offset];
      } else if (Configuration::GetStripeGroupSize() <= 65536) {
        stripeId = ((uint16_t*)mCompactStripeTable)[base + offset];
      } else {
        uint8_t *tmp = mCompactStripeTable + (base + offset) * 3;
        stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
      }
      *(uint32_t*)(footerForCurrentBlock + 16) = stripeId;
    }
    for (uint32_t offset = begin; offset < end; ++offset) {
      uint8_t *footerForCurrentBlock = mCurStripe->ioContext[i]->dataBuffer + (offset - begin) * 20;
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

void Segment::FlushCurrentStripe()
{
  while (mPosInStripe != 0) {
    static uint64_t counter = 0;
    Append(nullptr, 0);
  } 
}

bool Segment::Append(RequestContext *ctx, uint32_t offset)
{
  if (mPosInStripe == 0) {
    if (!findStripe()) {
      return false;
    }
    mLastStripeCreationTimestamp = GetTimestampInUs();
  }

  SystemMode mode = Configuration::GetSystemMode();
  uint32_t blockSize = Configuration::GetBlockSize();

  uint64_t lba = ~0ull;
  uint8_t *blkdata = nullptr;
  uint32_t whichBlock = mPosInStripe / blockSize;

  if (ctx) {
    lba = ctx->lba + offset * blockSize;
    blkdata = (uint8_t*)(ctx->data) + offset * blockSize;
  }

  uint32_t zoneId = Configuration::CalculateDiskId(
      mPos, whichBlock,
      (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
  // Issue data block
  {
    RequestContext *slot = mRequestContextPool->GetRequestContext(true);
    mCurStripe->ioContext[whichBlock] = slot;
    slot->lba = lba;
    slot->timestamp = ctx ? ctx->timestamp : ~0ull;

    slot->data = slot->dataBuffer;
    if (blkdata) {
      memcpy(slot->data, blkdata, blockSize);
    } else {
      memset(slot->data, 0, blockSize);
    }
    slot->meta = slot->metadataBuffer;

    slot->associatedStripe = mCurStripe;
    slot->associatedRequest = ctx;

    slot->zoneId = zoneId;
    slot->stripeId = (mPos - mHeaderRegionSize) % Configuration::GetStripeGroupSize();
    slot->segment = this;
    slot->ctrl = mRaidController;

    slot->type = STRIPE_UNIT;
    slot->status = WRITE_REAPING;
    slot->targetBytes = blockSize;

    // Initialize block (flash page) metadata
    BlockMetadata *blkMeta = (BlockMetadata *)slot->meta;
    blkMeta->fields.coded.lba = slot->lba;
    blkMeta->fields.coded.timestamp = slot->timestamp;
    blkMeta->fields.replicated.stripeId = slot->stripeId;

    if (mode == ZONEWRITE_ONLY ||
        (mode == ZAPRAID && mPos % Configuration::GetStripeGroupSize() == 0)) {
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

    if (mode == ZAPRAID) {
      if (mPos % Configuration::GetStripeGroupSize() == 0) {
        // writing the stripe metadata at the end of each group
        mSegmentStatus = SEGMENT_CONCLUDING_APPENDS_IN_GROUP;
      } else if (mPos % Configuration::GetStripeGroupSize() == 1) {
        mSegmentStatus = SEGMENT_CONCLUDING_WRITES_IN_GROUP;
      }
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
          &(((BlockMetadata*)stripe->ioContext[i]->meta)->fields.coded));
    }
    EncodeStripe(stripeData, n, k, Configuration::GetBlockSize());
    EncodeStripe(stripeProtectedMetadata, n, k, 16); // 16 bytes for the protected fields
//    uint32_t zoneId = 0;
//    for (uint32_t i = 0; i < n; ++i) {
//      uint32_t diskId = Configuration::CalculateDiskId(
//          zonePos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
//      if (diskId == 0) { zoneId = i; }
////      printf("%u %u %u %lu %lu %u\n", i, diskId, ((BlockMetadata*)stripe->ioContext[i]->meta)->fields.coded.lba, ((BlockMetadata*)stripe->ioContext[i]->meta)->fields.coded.timestamp, ((BlockMetadata*)stripe->ioContext[i]->meta)->fields.replicated.stripeId);
//    }
//    printf("%u %lu %lu %u\n", zoneId, ((BlockMetadata*)stripe->ioContext[zoneId]->meta)->fields.coded.lba, ((BlockMetadata*)stripe->ioContext[zoneId]->meta)->fields.coded.timestamp, ((BlockMetadata*)stripe->ioContext[zoneId]->meta)->fields.replicated.stripeId);
  }

  for (uint32_t i = k; i < n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        zonePos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones);
    RequestContext *slot = stripe->ioContext[i];
    slot->lba = ~0ull;
    slot->ctrl = mRaidController;
    slot->segment = this;
    slot->zoneId = zoneId;
    slot->stripeId = (zonePos - mHeaderRegionSize) % Configuration::GetStripeGroupSize();
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    if (mode == ZONEWRITE_ONLY ||
        (mode == ZAPRAID && zonePos % Configuration::GetStripeGroupSize() == 0)) {
      slot->append = false;
    } else {
      slot->append = true;
    }

    BlockMetadata *blkMeta = (BlockMetadata *)slot->meta;
    blkMeta->fields.replicated.stripeId = slot->stripeId;

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

void Segment::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();

  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = Configuration::GetStripeSize() / Configuration::GetStripeUnitSize();
  uint32_t k = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  ReadContext *readContext = ctx->associatedRead;
  uint32_t realOffsets[n];

  if (mode == ZAPRAID) {
    uint32_t groupId = (offset - mHeaderRegionSize) / Configuration::GetStripeGroupSize();
    uint32_t searchBegin = groupId * Configuration::GetStripeGroupSize();
    uint32_t searchEnd = std::min(searchBegin + Configuration::GetStripeGroupSize(), mPos - mHeaderRegionSize);

    // Find out the stripeId of the requested block
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;

    uint32_t stripeId;
    if (Configuration::GetStripeGroupSize() <= 256) {
      stripeId = mCompactStripeTable[index];
    } else if (Configuration::GetStripeGroupSize() <= 65536) {
      stripeId = ((uint16_t*)mCompactStripeTable)[index];
    } else {
      uint8_t *tmp = mCompactStripeTable + index * 3;
      stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
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
        if (Configuration::GetStripeGroupSize() <= 256) {
          stripeIdOfCurBlock = mCompactStripeTable[index];
        } else if (Configuration::GetStripeGroupSize() <= 65536) {
          stripeIdOfCurBlock = ((uint16_t*)mCompactStripeTable)[index];
        } else {
          uint8_t *tmp = mCompactStripeTable + index * 3;
          stripeIdOfCurBlock = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
        }

        if (stripeIdOfCurBlock == stripeId) {
          if (realOffsets[i] != ~0u) {
            printf("Duplicate stripe ID %p %u %u %u!\n", this, index, stripeIdOfCurBlock, stripeId);
//            uint8_t *tmp = mCompactStripeTable + index * 3;
//            printf("%p %p %p %lu %lu\n", this, tmp, mCompactStripeTable, index, *(tmp + 2), *(uint16_t*)tmp);
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
  CodedBlockMetadata &pbm = mCodedBlockMetadata[index];
  pbm.lba = ctx->lba;
  pbm.timestamp = ctx->timestamp;
  if (Configuration::GetStripeGroupSize() <= 256) {
    mCompactStripeTable[index] = ctx->stripeId;
  } else if (Configuration::GetStripeGroupSize() <= 65536) {
    ((uint16_t*)mCompactStripeTable)[index] = ctx->stripeId;
  } else {
    uint8_t *tmp = mCompactStripeTable + index * 3;
    *(tmp + 2) = ctx->stripeId >> 16;
    *(uint16_t*)tmp = ctx->stripeId & 0xffff;
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
      DecodeStripe(offset, readContext->data, alive, n, k, zoneId, blockSize);
    } else {
      uint32_t groupSize = Configuration::GetStripeGroupSize();
      uint32_t offsetOfStripe = mHeaderRegionSize + 
          (offset - mHeaderRegionSize) / groupSize * groupSize + 
          ctx->stripeId;
      DecodeStripe(offsetOfStripe, readContext->data, alive, n, k, zoneId, blockSize);
    }
  }

  if (!Configuration::GetDeviceSupportMetadata()) {
    BlockMetadata *meta = (BlockMetadata*)ctx->meta;
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;
    meta->fields.coded.lba = mCodedBlockMetadata[index].lba;
    meta->fields.coded.timestamp = mCodedBlockMetadata[index].timestamp;
    if (Configuration::GetStripeGroupSize() <= 256) {
      meta->fields.replicated.stripeId = mCompactStripeTable[index];
    } else if (Configuration::GetStripeGroupSize() <= 65536) {
      meta->fields.replicated.stripeId = ((uint16_t*)mCompactStripeTable)[index];
    } else {
      uint8_t *tmp = mCompactStripeTable + index * 3;
      meta->fields.replicated.stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
    }
  }

  if (parent->type == GC && parent->meta) {
    uint32_t offsetInBlks = (ctx->lba - parent->lba) / Configuration::GetBlockSize();
    BlockMetadata *result = (BlockMetadata*)(ctx->meta);
    BlockMetadata *parentMeta = &((BlockMetadata*)parent->meta)[offsetInBlks];
    parentMeta->fields.coded.lba = result->fields.coded.lba;
    parentMeta->fields.coded.timestamp = result->fields.coded.timestamp;
    parentMeta->fields.replicated.stripeId = result->fields.replicated.stripeId;
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
  uint32_t numZones = mZonesWpForRecovery.size();
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();

  for (uint32_t i = 0; i < numZones; ++i) {
    mDataBufferForRecovery[i] = (uint8_t*)spdk_zmalloc(
        mDataRegionSize * blockSize, 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    mMetadataBufferForRecovery[i] = (uint8_t*)spdk_zmalloc(
        mDataRegionSize * metadataSize, 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  }

  assert(mDataBufferForRecovery != nullptr);
  assert(mMetadataBufferForRecovery != nullptr);

  printf("Read blocks from zones\n");
  uint32_t offsets[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    offsets[i] = mHeaderRegionSize;
  }

  while (true) {
    uint32_t counter = 0;
    for (uint32_t i = 0; i < numZones; ++i) {
      uint64_t wp = mZonesWpForRecovery[i].first;
      uint64_t zslba = mZones[i]->GetSlba();
      uint32_t wpInZone = wp - zslba;
      uint32_t offsetInDataRegion = offsets[i] - 1;
      uint32_t numBlocks2Fetch = std::min(wpInZone - offsets[i], 32u);

      if (numBlocks2Fetch > 0) {
        // update compact stripe table
        int error = 0;
        if ((error = spdk_nvme_ns_cmd_read_with_md(
            mZones[i]->GetDevice()->GetNamespace(),
            mZones[i]->GetDevice()->GetIoQueue(0),
            mDataBufferForRecovery[i] + offsetInDataRegion * blockSize,
            mMetadataBufferForRecovery[i] + offsetInDataRegion * metadataSize,
            zslba + offsets[i], numBlocks2Fetch,
            completeOneEvent, &counter, 0, 0, 0)) < 0) {
          printf("Error in reading %d %s.\n", error, strerror(error));
        }
        offsets[i] += numBlocks2Fetch;
        counter += 1;
      }
    }

    if (counter == 0) {
      break;
    }

    while (counter != 0) {
      for (uint32_t i = 0; i < numZones; ++i) {
        spdk_nvme_qpair_process_completions(mZones[i]->GetDevice()->GetIoQueue(0), 0);
      }
    }
  }
  printf("Finish read all blocks.\n");

  for (uint32_t i = 0; i < numZones; ++i) {
    uint64_t wp = mZonesWpForRecovery[i].first;
    uint64_t zslba = mZones[i]->GetSlba();
    for (uint32_t j = 0; j < wp - zslba - mHeaderRegionSize; ++j) {
      uint32_t index = i * mDataRegionSize + j;
      BlockMetadata *blockMeta = &(((BlockMetadata*)mMetadataBufferForRecovery[i])[j]);
      uint64_t lba = blockMeta->fields.coded.lba;
      uint64_t writeTimestamp = blockMeta->fields.coded.timestamp;
      uint16_t stripeId = blockMeta->fields.replicated.stripeId;
      uint32_t stripeOffset = ~0u;

      if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
        stripeOffset = mHeaderRegionSize + j;
      } else {
        uint32_t groupSize = Configuration::GetStripeGroupSize();
        stripeOffset = mHeaderRegionSize + j / groupSize * groupSize + stripeId;
      }

      uint32_t whichBlock = 0;
      if (i == Configuration::CalculateDiskId(
            stripeOffset, mZones.size() - 1,
            Configuration::GetRaidLevel(), mZones.size())
         ) {
        // parity
        mCodedBlockMetadata[index].lba = ~0ull;
        mCodedBlockMetadata[index].timestamp = 0;
      } else {
        mCodedBlockMetadata[index].lba = blockMeta->fields.coded.lba;
        mCodedBlockMetadata[index].timestamp = blockMeta->fields.coded.timestamp;
      }

      if (Configuration::GetSystemMode() == ZAPRAID) {
        if (Configuration::GetStripeGroupSize() <= 256) {
          mCompactStripeTable[index] = stripeId;
        } else if (Configuration::GetStripeGroupSize() <= 65536) {
          ((uint16_t*)mCompactStripeTable)[index] = stripeId;
        } else {
          uint8_t *tmp = mCompactStripeTable + index * 3;
          *(tmp + 2) = stripeId >> 16;
          *(uint16_t*)tmp = stripeId & 0xffff;
        }
      }
    }
  }
}

bool Segment::RecoverFooterRegionIfNeeded()
{
  bool needed = false;
  uint32_t numZones = mZonesWpForRecovery.size();
  uint32_t blockSize = Configuration::GetBlockSize();
  for (uint32_t i = 0; i < numZones; ++i) {
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
  uint32_t offsets[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    offsets[i] = mZonesWpForRecovery[i].first - mZones[i]->GetSlba();
  }

  for (uint32_t i = 0; i < numZones; ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();

    for (uint32_t pos = offsets[i]; pos < mHeaderRegionSize + mDataRegionSize + mFooterRegionSize; ++pos) {
      uint32_t begin = (pos - 1 - mHeaderRegionSize - mDataRegionSize) * (blockSize / 20);
      uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

      uint32_t base = i * mDataRegionSize;
      uint32_t posInBlk = 0;
      bool done = false;
      for (uint32_t offset = begin; offset < end; ++offset) {
        *(uint64_t*)(footerBlock + (offset - begin) * 20 + 0) = mCodedBlockMetadata[base + offset].lba;
        *(uint64_t*)(footerBlock + (offset - begin) * 20 + 8) = mCodedBlockMetadata[base + offset].timestamp;

        uint16_t stripeId;
        if (Configuration::GetStripeGroupSize() <= 256) {
          stripeId = mCompactStripeTable[base + offset];
        } else if (Configuration::GetStripeGroupSize() <= 65536) {
          stripeId = ((uint16_t*)mCompactStripeTable)[base + offset];
        } else {
          uint8_t *tmp = mCompactStripeTable + (base + offset) * 3;
          stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
        }
        *(uint32_t*)(footerBlock + (offset - begin) * 20 +16) = stripeId;
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
  uint32_t numZones = mZones.size();
  for (uint32_t zid = 0; zid < numZones; ++zid) {
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

    uint32_t blockSize = Configuration::GetBlockSize();
    for (uint32_t offsetInFooterRegion = 0; offsetInFooterRegion < mFooterRegionSize; ++offsetInFooterRegion) {
      uint32_t begin = offsetInFooterRegion * (blockSize / 20);
      uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

      for (uint32_t offsetInDataRegion = begin; offsetInDataRegion < end; ++offsetInDataRegion) {
        uint8_t *metadataForCurrentBlock = buffer + offsetInFooterRegion * blockSize + (offsetInDataRegion - begin) * 20;
        uint64_t lba = *(uint64_t*)(metadataForCurrentBlock + 0);
        uint64_t writeTimestamp = *(uint64_t*)(metadataForCurrentBlock + 8);
        uint32_t stripeId = *(uint32_t*)(metadataForCurrentBlock + 16);

        if (Configuration::GetSystemMode() == ZAPRAID) {
          // update compact stripe table first
          uint32_t index = zid * mDataRegionSize + offsetInDataRegion;
          if (Configuration::GetStripeGroupSize() <= 256) {
            mCompactStripeTable[index] = stripeId;
          } else if (Configuration::GetStripeGroupSize() <= 65536) {
            ((uint16_t*)mCompactStripeTable)[index] = stripeId;
          } else {
            uint8_t *tmp = mCompactStripeTable + index * 3;
            *(tmp + 2) = stripeId >> 16;
            *(uint16_t*)tmp = stripeId & 0xffff;
          }
        }

        PhysicalAddr pba;
        pba.segment = this;
        pba.zoneId = zid;
        pba.offset = mHeaderRegionSize + offsetInDataRegion;

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

  mPos = mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;

  delete []mAdminStripe->data;
  delete []mAdminStripe->metadata;
  delete mAdminStripe;
  if (Configuration::GetDeviceSupportMetadata()) {
    delete []mCodedBlockMetadata;
  }
}

void Segment::RecoverIndexFromOpenSegment(
    std::pair<uint64_t, PhysicalAddr> *indexMap)
{
  // The block must have been brought into the memory and
  // mCodedBlockMetadata is revived accordingly
  for (uint32_t zid = 0; zid < mZones.size(); ++zid) {
    for (uint32_t offset = 0; offset < mPos; ++offset) {
      uint32_t index = zid * mDataRegionSize + offset;
      uint64_t lba = mCodedBlockMetadata[index].lba;
      uint64_t writeTimestamp = mCodedBlockMetadata[index].timestamp;

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
  bool needRewrite = false;
  if (Configuration::GetSystemMode() == ZAPRAID) {
    uint32_t checkpoint = ~0u;
    for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
      Zone *zone = mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = mZonesWpForRecovery[i].first - zslba;

      uint32_t begin = (wp - mHeaderRegionSize) / 
        Configuration::GetStripeGroupSize() *
        Configuration::GetStripeGroupSize();
      uint32_t end = begin + Configuration::GetStripeGroupSize();
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
        if (Configuration::GetStripeGroupSize() <= 256) {
          stripeId = mCompactStripeTable[index];
        } else if (Configuration::GetStripeGroupSize() <= 65536) {
          stripeId = ((uint16_t*)mCompactStripeTable)[index];
        } else {
          uint8_t *tmp = mCompactStripeTable + index * 3;
          stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
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

    for (auto stripe : mStripesToRecover) {
      uint32_t stripeId = stripe.first;
      const auto &blocks = stripe.second;

      for (uint32_t i = 0; i < mZones.size(); ++i) {
        if (blocks[i] == ~0u) {
          needRewrite = true;
        }
      }
    }
  } else {
    uint32_t agreedWp = 0;
    for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
      Zone *zone = mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = mZonesWpForRecovery[i].first - zslba;
      if (agreedWp == 0) {
        agreedWp = wp;
      }

      if (agreedWp != wp) {
        needRewrite = true;
      }
    }
  }

  return needRewrite;
}

void Segment::RecoverState()
{
  mPos = mZonesWpForRecovery[0].first - mZones[0]->GetSlba();

  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
  }
}

void Segment::RecoverFromOldSegment(Segment *oldSegment)
{
  // First, write the header region
  uint32_t counter = mZones.size();
  RequestContext *contexts[mZones.size()];
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    Zone *zone = mZones[i];
    contexts[i] = mRequestContextPool->GetRequestContext(false);
    memcpy(contexts[i]->dataBuffer, &mSegmentMeta, sizeof(mSegmentMeta));

    if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            contexts[i]->dataBuffer, contexts[i]->metadataBuffer,
            zone->GetSlba(), 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
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
  if (Configuration::GetSystemMode() == ZAPRAID) {
    for (uint32_t i = 0; i < oldSegment->mZonesWpForRecovery.size(); ++i) {
      Zone *zone = oldSegment->mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = oldSegment->mZonesWpForRecovery[i].first - zslba;
      uint32_t lastCheckpoint = (wp - mHeaderRegionSize) /
          Configuration::GetStripeGroupSize() *
          Configuration::GetStripeGroupSize();

      if (checkpoint == ~0) {
        checkpoint = lastCheckpoint;
      } else if (checkpoint != lastCheckpoint) {
        printf("Critical error: checkpoint not match.\n");
      }
    }
  } else if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
    for (uint32_t i = 0; i < oldSegment->mZonesWpForRecovery.size(); ++i) {
      Zone *zone = oldSegment->mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = oldSegment->mZonesWpForRecovery[i].first - zslba;
      checkpoint = std::min(wp, checkpoint);
    }
  }
  // Blocks (both data and parity) before the checkpoint can be kept as is.
  for (uint32_t i = 0; i < checkpoint; ++i) {
    struct timeval s, e;
    gettimeofday(&s, NULL);
    uint32_t counter = mZones.size();
    for (uint32_t j = 0; j < mZones.size(); ++j) {
      Zone *zone = mZones[j];
      memcpy(contexts[j]->dataBuffer, oldSegment->mDataBufferForRecovery[j] + i * Configuration::GetBlockSize(), 4096);
      memcpy(contexts[j]->metadataBuffer, oldSegment->mMetadataBufferForRecovery[j] + i * Configuration::GetMetadataSize(), 4096);
      if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            contexts[j]->dataBuffer, contexts[j]->metadataBuffer,
            zone->GetSlba() + mHeaderRegionSize + i, 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in writing the previous groups for new segment.\n");
      }
      uint32_t index = j * mDataRegionSize + i;
      mCodedBlockMetadata[index].lba = oldSegment->mCodedBlockMetadata[index].lba;
      mCodedBlockMetadata[index].timestamp = oldSegment->mCodedBlockMetadata[index].timestamp;
      if (Configuration::GetStripeGroupSize() <= 256) {
        mCompactStripeTable[index] = oldSegment->mCompactStripeTable[index];
      } else if (Configuration::GetStripeGroupSize() <= 65536) {
        ((uint16_t*)mCompactStripeTable)[index] = oldSegment->mCompactStripeTable[index];
      } else {
        uint8_t *tmp = mCompactStripeTable + index * 3;
        uint8_t *oldStripeId = oldSegment->mCompactStripeTable + index * 3;
        *(tmp + 2) = *(oldStripeId + 2);
        *(uint16_t*)tmp = *(uint16_t*)(oldStripeId);
      }
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
    gettimeofday(&e, NULL);
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  }

  // For blocks in the last group
  uint32_t maxStripeId = 0;
  uint32_t numZones = mZones.size();
  for (auto stripe : oldSegment->mStripesToRecover) {
    // the iteration order must be from the smallest to the largest;
    // so filter out invalid stripe and re-assign the stripe IDs will not
    // affect the ordering
    const auto &blocks = stripe.second;
    uint32_t counter = mZones.size();

    bool valid = true;
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      if (blocks[i] == ~0u) {
        valid = false;
      }
    }

    if (!valid) {
      continue;
    }

    for (uint32_t i = 0; i < numZones; ++i) {
      uint32_t index = i * mDataRegionSize + checkpoint + maxStripeId;
      BlockMetadata *blockMeta = reinterpret_cast<BlockMetadata*>(
          oldSegment->mMetadataBufferForRecovery[i] + blocks[i] * Configuration::GetMetadataSize());

      if (i == Configuration::CalculateDiskId(
            mHeaderRegionSize + index, mZones.size() - 1,
            Configuration::GetRaidLevel(), mZones.size())
         ) {
        // parity
        mCodedBlockMetadata[index].lba = ~0ull;
        mCodedBlockMetadata[index].timestamp = 0;
      } else {
        mCodedBlockMetadata[index].lba = blockMeta->fields.coded.lba;
        mCodedBlockMetadata[index].timestamp = blockMeta->fields.coded.timestamp;
      }

      blockMeta->fields.replicated.stripeId = maxStripeId;
      if (Configuration::GetStripeGroupSize() <= 256) {
        mCompactStripeTable[index] = maxStripeId;
      } else if (Configuration::GetStripeGroupSize() <= 65536) {
        ((uint16_t*)mCompactStripeTable)[index] = maxStripeId;
      } else {
        uint8_t *tmp = mCompactStripeTable + index * 3;
        *(tmp + 2) = maxStripeId >> 16;
        *(uint16_t*)tmp = maxStripeId & 0xffff;
      }

      if (spdk_nvme_ns_cmd_write_with_md(
            mZones[i]->GetDevice()->GetNamespace(),
            mZones[i]->GetDevice()->GetIoQueue(0),
            oldSegment->mDataBufferForRecovery[i] + blocks[i] * Configuration::GetBlockSize(),
            oldSegment->mMetadataBufferForRecovery[i] + blocks[i] * Configuration::GetMetadataSize(),
            mZones[i]->GetSlba() + mHeaderRegionSize + checkpoint + maxStripeId,
            1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in rewriting the last group in the new segment.\n");
      }
      // printf("%u %u %s\n", mCodedBlockMetadata[index].lba, mCompactStripeTable[index], oldSegment->mDataBufferForRecovery[j] + i * Configuration::GetBlockSize());
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
    maxStripeId += 1;
  }

  mPos = mHeaderRegionSize + checkpoint + maxStripeId;
  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
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
    while (!done)
    {
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
    zone->GetDevice()->ReturnZone(zone);
  }
}

void Segment::FinishRecovery()
{
  // 
  uint32_t numBlocks = 0;
  if (mSegmentStatus == SEGMENT_SEALED) {
    numBlocks = mDataRegionSize * mSegmentMeta.k;
  } else {
    numBlocks = (mPos - mHeaderRegionSize) * mSegmentMeta.k;
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
