#ifndef __COMMON_H__
#define __COMMON_H__
#include <cstdint>
#include <cstdio>
#include <cassert>
#include <mutex>
#include <vector>
#include <map>
#include "spdk/nvme.h"
#include "configuration.h"

class Segment;
class RAIDController;
struct RequestContext;
struct GcTask;

typedef void (*zns_raid_request_complete)(void *cb_arg);

typedef uint64_t LogicalAddr;
struct PhysicalAddr {
  Segment* segment;
  uint32_t zoneId;
  uint32_t stripeId;
  uint32_t offset;
  void PrintOffset();

  bool operator==(const PhysicalAddr o) const {
    return (segment == o.segment) &&
           (zoneId == o.zoneId) &&
           (offset == o.offset);
  }
};

struct Metadata {
public:
  uint64_t lba;
  uint64_t assignedPba;
};

enum ContextStatus {
  WRITE_REAPING,
  WRITE_COMPLETE,
  READ_REAPING,
  READ_COMPLETE,
  DEGRADED_READ_REAPING,
  DEGRADED_READ_META,
  DEGRADED_READ_SUB,
  RESET_REAPING,
  RESET_COMPLETE,
  FINISH_REAPING,
  FINISH_COMPLETE
};

enum ContextType
{
  USER,
  GC,
  INDEX,
  STRIPE_UNIT,
};

struct StripeWriteContext {
  uint8_t **data;
  uint8_t *metadata;
  uint8_t *dataPool;
  std::vector<RequestContext*> ioContext;
  uint32_t targetBytes;
  uint32_t successBytes;
};

struct ReadContext {
  uint8_t **data;
  uint8_t *dataPool;
  uint8_t *metadata;
  std::vector<RequestContext*> ioContext;
};

union BlockMetadata {
  struct {
    uint64_t lba;
    uint64_t timestamp;
    uint8_t  stripeId;
  } d;
  uint8_t reserved[64];
};

struct RequestContext
{
  // Each Context is pre-allocated with a buffer
  uint8_t *buffer;
  ContextType type;
  ContextStatus status;

  // A user request use the following field:
  // Info: lba, size, req_type, data
  // pbaArray, successBytes, and targetBytes
  uint64_t lba;
  uint32_t size;
  uint8_t  req_type;
  uint8_t *data;
  uint8_t *meta;
  uint32_t successBytes;
  uint32_t targetBytes;
  uint32_t curOffset;
  zns_raid_request_complete cb_fn;
  void *cb_args;

  bool available;

  // Used inside a Segment write/read
  RAIDController *ctrl;
  Segment *segment;
  uint32_t zoneId;
  uint32_t stripeId;
  uint32_t offset;
  bool append;

  double stime;
  double ctime;
  uint64_t timestamp;

  // context during request process
  RequestContext* associatedRequest;
  StripeWriteContext *associatedStripe;
  ReadContext *associatedRead;

  bool needDegradedRead;
  bool needDecodeMeta;

  struct {
    struct spdk_nvme_ns *ns;
    struct spdk_nvme_qpair *qpair;
    void *data;
    void *metadata;
    uint64_t offset;
    uint32_t size;
    spdk_nvme_cmd_cb cb;
    void *ctx;
    uint32_t flags;
  } ioContext;

  GcTask *gcTask;
  std::vector<PhysicalAddr> pbaArray;

  void Clear();
  void Queue();
  PhysicalAddr GetPba();
  double GetElapsedTime();
  void PrintStats();
  void CopyFrom(const RequestContext &o);
};

// Data is an array: index is offset, value is the stripe Id in the corresponding group
struct SyncPoint {
  uint8_t *data;
  uint8_t *metadata;
  RequestContext slots[16];
};

enum GcTaskStage {
  IDLE,
  INIT,
  GC_RUNNING,
  COMPLETE
};

struct GcTask {
  GcTaskStage stage;

  Segment *inputSegment;
  Segment *outputSegment;
  uint32_t maxZoneId;
  uint32_t maxOffset;

  uint8_t* dataBuffer;
  uint8_t* metaBuffer;
  uint32_t writerPos;
  uint32_t readerPos;
  RequestContext contextPool[8];
  PhysicalAddr blockAddr[8];

  uint32_t numWriteSubmitted;
  uint32_t numWriteFinish;

  uint32_t curZoneId;
  uint32_t nextOffset;

  std::map<uint64_t, std::pair<PhysicalAddr, PhysicalAddr>> mappings;
};

struct IoThread {
  struct spdk_nvme_poll_group *group;
  struct spdk_thread *thread;
  uint32_t threadId;
  RAIDController *controller;
};

double timestamp();
static void b() {}

#endif
