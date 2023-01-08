#ifndef __COMMON_H__
#define __COMMON_H__
#include <cstdint>
#include <cstdio>
#include <cassert>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_set>
#include <list>
#include <spdk/nvme.h>
#include <spdk/thread.h>
#include <spdk/event.h>
#include "configuration.h"

class Segment;
class RAIDController;
struct RequestContext;
struct GcTask;

inline uint64_t round_up(uint64_t value, uint64_t align)
{
  return (value + align - 1) / align * align;
}

inline uint64_t round_down(uint64_t value, uint64_t align)
{
  return value / align * align;
}


typedef void (*zns_raid_request_complete)(void *cb_arg);

void completeOneEvent(void *arg, const struct spdk_nvme_cpl *completion);
void complete(void *arg, const struct spdk_nvme_cpl *completion);
void thread_send_msg(spdk_thread *thread, spdk_msg_fn fn, void *args);
void event_call(uint32_t core_id, spdk_event_fn fn, void *arg1, void *arg2);

struct Request {
  RAIDController *controller;
  uint64_t offset;
  uint32_t size;
  void *data;
  char type;
  zns_raid_request_complete cb_fn;
  void *cb_args;
};

struct PhysicalAddr {
  Segment* segment;
  uint32_t zoneId;
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
  WRITE_INDEX_UPDATING,
  WRITE_INDEX_UPDATED,
  WRITE_COMPLETE,
  READ_PREPARE,
  READ_INDEX_QUERYING,
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
  uint8_t **metadata;
  uint8_t *dataBuffer;
  uint8_t *metadataBuffer;
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

struct CodedBlockMetadata {
  uint64_t lba;
  uint64_t timestamp;
};

struct ReplicatedBlockMetadata {
  uint32_t stripeId;
};

union BlockMetadata {
  struct {
    struct CodedBlockMetadata coded;
    struct ReplicatedBlockMetadata replicated;
  } fields;
  uint8_t reserved[64];
};

struct RequestContext
{
  // The buffers are pre-allocated
  uint8_t *dataBuffer;
  uint8_t *metadataBuffer;
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

  struct timeval timeA;

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
struct GroupMeta {
  uint8_t *data;
  uint8_t *metadata;
  RequestContext slots[16];
};

enum GcTaskStage {
  IDLE,
  INIT,
  REWRITING,
  REWRITE_COMPLETE,
  INDEX_UPDATING,
  INDEX_UPDATING_BATCH,
  INDEX_UPDATE_COMPLETE,
  RESETTING_INPUT_SEGMENT,
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
  RequestContext *contextPool;

  uint32_t numWriteSubmitted;
  uint32_t numWriteFinish;
  uint32_t numReads;

  uint32_t curZoneId;
  uint32_t nextOffset;

  uint32_t numBuffers;

  std::map<uint64_t, std::pair<PhysicalAddr, PhysicalAddr>> mappings;
};

struct IoThread {
  struct spdk_nvme_poll_group *group;
  struct spdk_thread *thread;
  uint32_t threadId;
  RAIDController *controller;
};

struct RequestContextPool {
  RequestContext *contexts;
  std::vector<RequestContext*> availableContexts;
  uint32_t capacity;

  RequestContextPool(uint32_t cap);
  RequestContext *GetRequestContext(bool force);
  void ReturnRequestContext(RequestContext *slot);
};

struct ReadContextPool {
  ReadContext *contexts;
  std::vector<ReadContext*> availableContexts;
  RequestContextPool *requestPool;
  uint32_t capacity;

  ReadContextPool(uint32_t cap, RequestContextPool *rp);
  ReadContext* GetContext();
  void Recycle();
  void ReturnContext(ReadContext *readContext);
private:
  bool checkReadAvailable(ReadContext *readContext);
};
  
struct StripeWriteContextPool {
  uint32_t capacity;
  struct StripeWriteContext *contexts;
  std::vector<StripeWriteContext *> availableContexts;
  std::unordered_set<StripeWriteContext *> inflightContexts;
  struct RequestContextPool *rPool;

  StripeWriteContextPool(uint32_t cap, struct RequestContextPool *rp);
  StripeWriteContext* GetContext();
  void Recycle();
  bool NoInflightStripes();

private:
  bool checkStripeAvailable(StripeWriteContext *stripe);
};

double GetTimestampInUs();
double timestamp();
double gettimediff(struct timeval s, struct timeval e);

void InitErasureCoding();
void DecodeStripe(uint32_t offset, uint8_t **stripe,
    bool *alive, uint32_t n, uint32_t k,
    uint32_t decodeZid, uint32_t unitSize);
void EncodeStripe(uint8_t **stripe, uint32_t n, uint32_t k, uint32_t unitSize);

#endif
