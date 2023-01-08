enum SystemMode {
  ZONEWRITE_ONLY, ZONEAPPEND_ONLY, ZAPRAID
};

enum RAIDLevel {
  RAID0, RAID1, RAID4, RAID5, RAID6,
  // For a four-drive setup, RAID01 first stripes the blocks into two drives,
  // and then replicate the two blocks into four drives during parity generation
  RAID01,
};

const static int raid6_4drive_mapping[4][4] = {
  {0, 1, 2, 3},
  {0, 2, 3, 1},
  {2, 3, 0, 1},
  {3, 0, 1, 2}
};

const static int raid6_5drive_mapping[5][5] = {
  {0, 1, 2, 3, 4},
  {0, 1, 3, 4, 2},
  {0, 3, 4, 1, 2},
  {3, 4, 0, 1, 2},
  {4, 0, 1, 2, 3}
};

const static int raid6_6drive_mapping[6][6] = {
  {0, 1, 2, 3, 4, 5},
  {0, 1, 2, 4, 5, 3},
  {0, 1, 4, 5, 2, 3},
  {0, 4, 5, 1, 2, 3},
  {4, 5, 0, 1, 2, 3},
  {5, 0, 1, 2, 3, 4}
};

class Configuration {
public:
  static Configuration& GetInstance() {
    static Configuration instance;
    return instance;
  }

  static void PrintConfigurations() {
    Configuration &instance = GetInstance();
    const char *systemModeStrs[] = {"ZoneWrite-Only", "ZoneAppend-Only", "ZapRAID"};
    printf("ZapRAID Configuration:\n");
    printf("-- Raid mode: %d %d %d --\n",
        instance.gStripeDataSize,
        instance.gStripeParitySize,
        instance.gRaidScheme);
    printf("-- System mode: %s --\n", systemModeStrs[(int)instance.gSystemMode]);
    printf("-- GC Enable: %d --\n", instance.gEnableGc);
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

  static SystemMode GetSystemMode() {
    return GetInstance().gSystemMode;
  }

  static void SetSystemMode(SystemMode mode) {
    GetInstance().gSystemMode = mode;
  }

  static int GetStripePersistencyMode() {
    return GetInstance().gStripePersistencyMode;
  }

  static void SetStripeDataSize(int stripeDataSize) {
    GetInstance().gStripeDataSize = stripeDataSize;
    GetInstance().gStripeSize = GetInstance().gStripeDataSize + GetInstance().gStripeParitySize;
  }

  static void SetStripeParitySize(int stripeParitySize) {
    GetInstance().gStripeParitySize = stripeParitySize;
    GetInstance().gStripeSize = GetInstance().gStripeDataSize + GetInstance().gStripeParitySize;
  }

  static void SetEnableGc(bool enable) {
    GetInstance().gEnableGc = enable;
  }

  static bool GetEnableGc() {
    return GetInstance().gEnableGc;
  }

  static void SetStripeGroupSize(uint32_t groupSize) {
    GetInstance().gStripeGroupSize = groupSize;
  }

  static int GetStripeGroupSize() {
    return GetInstance().gStripeGroupSize;
  }

  static void SetEnableDegradedRead(bool enable) {
    GetInstance().gEnableDegradedRead = enable;
  }

  static bool GetEnableDegradedRead() {
    return GetInstance().gEnableDegradedRead;
  }

  static void SetRaidLevel(RAIDLevel level) {
    GetInstance().gRaidScheme = level;
  }

  static RAIDLevel GetRaidLevel() {
    return GetInstance().gRaidScheme;
  }

  static void SetNumOpenSegments(uint32_t num_open_segments) {
    GetInstance().gNumOpenSegments = num_open_segments;
  }

  static int GetNumOpenSegments() {
    return GetInstance().gNumOpenSegments;
  }

  static void SetEnableHeaderFooter(bool enable_header_footer) {
    GetInstance().gEnableHeaderFooter = enable_header_footer;
  }

  static bool GetEnableHeaderFooter() {
    return GetInstance().gEnableHeaderFooter;
  }

  static bool GetBypassDevice() {
    return false;
  }

  static void SetRebootMode(uint32_t rebootMode) {
    GetInstance().gRebootMode = rebootMode;
  }

  static uint32_t GetRebootMode() {
    return GetInstance().gRebootMode;
  }

  static uint32_t GetReceiverThreadCoreId() {
    return GetInstance().gReceiverThreadCoreId;
  }

  static uint32_t GetEcThreadCoreId() {
    return GetInstance().gEcThreadCoreId;
  }

  static uint32_t GetIndexThreadCoreId() {
    return GetInstance().gIndexThreadCoreId;
  }

  static uint32_t GetDispatchThreadCoreId() {
    return GetInstance().gDispatchThreadCoreId;
  }

  static uint32_t GetCompletionThreadCoreId() {
    return GetInstance().gCompletionThreadCoreId;
  }

  static uint32_t GetIoThreadCoreId(uint32_t thread_id) {
    return GetInstance().gIoThreadCoreIdBase + thread_id;
  }

  static void SetStorageSpaceInBytes(uint64_t storageSpaceInBytes) {
    GetInstance().gStorageSpaceInBytes = storageSpaceInBytes;
  }

  static uint64_t GetStorageSpaceInBytes() {
    return GetInstance().gStorageSpaceInBytes;
  }

  static void SetEnableEventFramework(bool enable) {
    GetInstance().gEnableEventFramework = enable;
  }

  static bool GetEventFrameworkEnabled() {
    return GetInstance().gEnableEventFramework;
  }

  static void SetEnableRedirection(bool enable) {
    GetInstance().gEnableRedirection = enable;
  }

  static bool GetRedirectionEnable() {
    return GetInstance().gEnableRedirection;
  }


  static uint32_t CalculateDiskId(uint32_t stripeId, uint32_t whichBlock, RAIDLevel raidScheme, uint32_t numDisks) {
    // calculate which disk current block (data/parity) should go
    uint32_t driveId = ~0u;
    uint32_t idOfGroup = stripeId % numDisks;

    if (raidScheme == RAID0
        || raidScheme == RAID1
        || raidScheme == RAID01
        || raidScheme == RAID4) {
      driveId = whichBlock;
    } else if (raidScheme == RAID5) {
      // Example: 0 1 P
      //          0 P 1
      //          P 0 1
      if (whichBlock == numDisks - 1) { // parity block
        driveId = whichBlock - idOfGroup;
      } else if (whichBlock + idOfGroup >= numDisks - 1) {
        driveId = whichBlock + 1;
      } else {
        driveId = whichBlock;
      }
    } else if (raidScheme == RAID6 && numDisks == 4) {
      driveId = raid6_4drive_mapping[stripeId % numDisks][whichBlock];
    } else if (raidScheme == RAID6 && numDisks == 5) {
      // A1 A2 A3 P1 P2
      // B1 B2 P1 P2 B3
      // C1 P1 P2 C2 C3
      // P1 P2 D1 D2 D3
      // P2 E1 E2 E3 P1
      // ...
      driveId = raid6_5drive_mapping[stripeId % numDisks][whichBlock];
    }
    else if (raidScheme == RAID6 && numDisks == 6)
    {
      driveId = raid6_6drive_mapping[stripeId % numDisks][whichBlock];
    }
    else
    {
      fprintf(stderr, "RAID scheme not supported!\n");
    }
    return driveId;
  }

private:
  uint32_t gRebootMode = 0; // 0: new, 1: restart, 2: rebuild.

  int gStripeSize = 4096 * 4;
  int gStripeDataSize = 4096 * 3;
  int gStripeParitySize = 4096 * 1;
  int gStripeUnitSize = 4096 * 1;
  int gStripeBlockSize = 4096;
  int gBlockSize = 4096;
  int gMetadataSize = 64;
  int gNumIoThreads = 1;
  bool gDeviceSupportMetadata = true;
  int gZoneCapacity = 0;
  int gStripePersistencyMode = 0;
  bool gEnableGc = true;
  int gStripeGroupSize = 256;
  bool gEnableDegradedRead = false;
  uint32_t gNumOpenSegments = 1;
  RAIDLevel gRaidScheme = RAID5;
  bool gEnableHeaderFooter = true;
  bool gEnableRedirection = false;

  uint64_t gStorageSpaceInBytes = 1024 * 1024 * 1024 * 1024ull; // 1TiB

  SystemMode gSystemMode = ZAPRAID;

  uint32_t gReceiverThreadCoreId = 3;
  uint32_t gDispatchThreadCoreId = 4;
  // Not used for now; functions collocated with dispatch thread.
  uint32_t gCompletionThreadCoreId = 5;
  uint32_t gIndexThreadCoreId = 6;
  uint32_t gEcThreadCoreId = 7;
  uint32_t gIoThreadCoreIdBase = 8;

  // SPDK target adopts the reactors framework, and we should not manually initiate any SPDK thread
  bool gEnableEventFramework = false;
};

