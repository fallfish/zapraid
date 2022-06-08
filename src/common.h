#ifndef __COMMON_H__
#define __COMMON_H__
#include <cstdint>
#include <cstdio>
#include <cassert>
#include <mutex>
#include <vector>
#include <map>
#include "spdk/nvme.h"

class ZoneGroup;
class RAIDController;
struct RequestContext;
struct GcTask;

enum SystemMode {
  PURE_WRITE, PURE_ZONE_APPEND,
  ZONE_APPEND_NO_META, ZONE_APPEND_WITH_META,
  ZONE_APPEND_WITH_REDIRECTION
};

class Configuration {
public:
  static Configuration& GetInstance() {
    static Configuration instance;
    return instance;
  }

  static int GetStripeSize() {
    return GetInstance().gStripeSize;
  }

  static int GetStripeDataSize() {
    return GetInstance().gStripeDataSize;
  }

  static int GetStripeParitySize() {
    return GetInstance().gStripeParitySize;
  }

  static int GetStripeUnitSize() {
    return GetInstance().gStripeUnitSize;
  }

  static int GetBlockSize() {
    return GetInstance().gBlockSize;
  }

  static int GetMetadataSize() {
    return GetInstance().gMetadataSize;
  }

  static int GetNumIoThreads() {
    return GetInstance().gNumIoThreads;
  }

  static bool GetDeviceSupportMetadata() {
    return GetInstance().gDeviceSupportMetadata;
  }

  static int GetZoneCapacity() {
    return GetInstance().gZoneCapacity;
  }

  static void SetZoneCapacity(int cap) {
    GetInstance().gZoneCapacity = cap;
  }

  static SystemMode GetSystemMode() {
    return GetInstance().gSystemMode;
  }

  static void SetSystemMode(SystemMode mode) {
    GetInstance().gSystemMode = mode;
  }

private:
  int gStripeSize = 4096 * 3;
  int gStripeDataSize = 4096 * 2;
  int gStripeParitySize = 4096 * 1;
  int gStripeUnitSize = 4096 * 1;
  int gStripeBlockSize = 4096;
  int gBlockSize = 4096;
  int gMetadataSize = 64;
  int gNumIoThreads = 3;
  bool gDeviceSupportMetadata = false;
  int gZoneCapacity = 0;

  SystemMode gSystemMode = PURE_WRITE;
  // 0: Pure write; 1: Pure zone append; 2: Zone append without metadata; 3: Zone append with metadata; 4: Zone append with redirection

};

typedef void (*zns_raid_request_complete)(void *cb_arg);

typedef uint64_t LogicalAddr;
struct PhysicalAddr {
  ZoneGroup* zoneGroup;
  uint32_t zoneId;
  uint32_t stripeId;
  uint32_t offset;
  void PrintOffset();

  bool operator==(const PhysicalAddr o) const {
    return (zoneGroup == o.zoneGroup) &&
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
};

enum ContextType
{
  USER,
  GC,
  STRIPE_UNIT,
};

struct StripeWriteContext {
  uint8_t *data;
  uint8_t *metadata;
  std::vector<RequestContext*> ioContext;
  uint32_t targetBytes;
  uint32_t successBytes;
};

struct ReadContext {
  uint8_t *data;
  uint8_t *metadata;
  std::vector<RequestContext*> ioContext;
};

union BlockMetadata {
  struct {
    uint64_t lba;
    uint8_t stripeId;
  } d;
  uint8_t reserved[64];
};

struct RequestContext
{
  ContextType type;
  ContextStatus status;

  // A user request use the following field:
  // Info: lba, size, req_type, data
  // pbaArray, successBytes, and targetBytes
  uint64_t lba;
  uint32_t size;
  uint8_t  req_type;
  void *data;
  void *meta;
  std::vector<PhysicalAddr> pbaArray;
  uint32_t successBytes;
  uint32_t targetBytes;
  uint32_t curOffset;
  zns_raid_request_complete cb_fn;
  void *cb_args;

  bool available;

  // Used inside a ZoneGroup write/read
  RAIDController *ctrl;
  ZoneGroup *zoneGroup;
  uint32_t zoneId;
  uint32_t stripeId;
  uint32_t offset;
  bool append;

  double stime;
  double ctime;

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
  RequestContext slots[3];
};

enum GcTaskStage {
  IDLE,
  INIT,
  GC_RUNNING,
  COMPLETE
};

struct GcTask {
  GcTaskStage stage;

  ZoneGroup *inputZoneGroup;
  ZoneGroup *outputZoneGroup;
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

#endif
