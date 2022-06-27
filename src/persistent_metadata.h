#ifndef __PERSISTENT_METADATA_H_
#define __PERSISTENT_METADATA_H_
#include "common.h"
#include "segment.h"
struct IndexUpdateEntry {
  // 8 + 8 + 4 = 20
  uint64_t lba;
  uint32_t segmentId;
  uint32_t zoneId;
  uint32_t offset;
};

class PersistentMetadata {
public:
  PersistentMetadata();
  
  void SetMetadataSegments(Segment *segment1, Segment *segment2);
  void AddIndexUpdateEntry(IndexUpdateEntry *entry);
  bool IsCurrentIndexEntryBlockFull();
  void PersistBlock();

private:
  RequestContext entryBlocks[2];
  RequestContext mResetContext;

  RequestContext *mCurEntryBlock;
  RequestContext *mOldEntryBlock;

  Segment *mCurSegment;
  Segment *mOldSegment;

  uint32_t mOffset;
  uint32_t mEntryBlockSizeInNumEntries;
  bool     mWaitPersisting;
};
#endif
