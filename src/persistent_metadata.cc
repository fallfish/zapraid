#include "persistent_metadata.h"

PersistentMetadata::PersistentMetadata()
{
  mEntryBlockSizeInNumEntries = Configuration::GetStripeDataSize() / sizeof(IndexUpdateEntry);
  entryBlocks[0].Clear();
  entryBlocks[1].Clear();
  entryBlocks[0].data = (uint8_t*)spdk_zmalloc(
      Configuration::GetStripeDataSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);
  entryBlocks[0].meta = (uint8_t*)spdk_zmalloc(
      Configuration::GetStripeDataSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);
  entryBlocks[1].data = (uint8_t*)spdk_zmalloc(
      Configuration::GetStripeDataSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);
  entryBlocks[1].meta = (uint8_t*)spdk_zmalloc(
      Configuration::GetStripeDataSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);

  mCurEntryBlock = &entryBlocks[0];
  mOldEntryBlock = &entryBlocks[1];
  mOffset = 0;
  mWaitPersisting = false;
  mResetContext.Clear();
}

void PersistentMetadata::SetMetadataSegments(Segment *segment1, Segment *segment2)
{
  mCurSegment = segment1;
  mOldSegment = segment2;
}

void PersistentMetadata::AddIndexUpdateEntry(IndexUpdateEntry *entry)
{
  memcpy((uint8_t*)mCurEntryBlock->data + mOffset * sizeof(*entry), entry, sizeof(*entry));
  mOffset += 1;

  PersistBlock();
}

bool PersistentMetadata::IsCurrentIndexEntryBlockFull()
{
  return mOffset >= mEntryBlockSizeInNumEntries;
}

void PersistentMetadata::PersistBlock()
{
  if (IsCurrentIndexEntryBlockFull()) {
    if (mOldEntryBlock->available) {
      RequestContext *tmp = mCurEntryBlock;
      mCurEntryBlock = mOldEntryBlock;
      mOldEntryBlock = tmp;
      mOffset = 0;

      mOldEntryBlock->pbaArray.resize(
          Configuration::GetStripeDataSize() /
          Configuration::GetBlockSize());
      mOldEntryBlock->type = INDEX;
      mOldEntryBlock->status = WRITE_REAPING;
      mOldEntryBlock->successBytes = 0;
      mOldEntryBlock->targetBytes = Configuration::GetStripeDataSize();
      mOldEntryBlock->available = false;
      mOldEntryBlock->lba = 0;
      mWaitPersisting = true;
    }
  }

  if (mWaitPersisting) {
    // First check whether the current segment is resetting

    // If the segment has finished reset, write the whole index map at the head

    // Second check whether the current segment has been written the whole index map in the head

    bool success = true;
    if (mResetContext.available == false) {
      printf("Reset old segment succeed!\n");
      success = false;
    }
    if (mCurSegment->IsFull() && mResetContext.available == true) {
      {
        Segment *tmp = mOldSegment;
        mOldSegment = mCurSegment;
        mCurSegment = tmp;
      }
      mCurSegment->FinalizeSegmentHeader();
      printf("Finalize old segment!\n");
      success = false;
    }

    for (uint32_t pos = 0; pos < Configuration::GetStripeDataSize();
        pos += Configuration::GetBlockSize()) {
      success = mCurSegment->Append(mOldEntryBlock, pos);
      if (!success) {
        break;
      }
    }

    if (success) {
      mWaitPersisting = false;
    }

    if (mCurSegment->IsFull()) {
      printf("Current metadata segment is full!\n");
      mResetContext.available = false;
      mOldSegment->Reset(&mResetContext);
    }
  }
}
