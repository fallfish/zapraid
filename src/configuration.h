enum SystemMode {
  NAMED_WRITE, NAMELESS_WRITE,
  NAMED_GROUP, NAMED_META, REDIRECTION
};

enum RAIDLevel {
  RAID0, RAID1, RAID4, RAID5, RAID6
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
    const char *systemModeStrs[] = {"Pure write", "Pure zone append", "ZnsRaid no meta", "ZnsRaid with meta", "ZnsRaid with redirection"};
    printf("ZNS RAID Configuration:\n");
    printf("-- Raid mode: %d %d %d --\n",
        instance.gStripeDataSize,
        instance.gStripeParitySize,
        instance.gRaidScheme);
    printf("-- System mode: %s --\n", systemModeStrs[(int)instance.gSystemMode]);
    printf("-- Stripe Persistency Mode: %d --\n", instance.gStripePersistencyMode);
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

  static void SetSyncGroupSize(uint32_t groupSize) {
    GetInstance().gSyncGroupSize = groupSize;
  }

  static int GetSyncGroupSize() {
    return GetInstance().gSyncGroupSize;
  }

  static void SetEnableDegradedRead(bool enable) {
    GetInstance().gEnableDegradedRead = enable;
  }

  static bool GetEnableDegradedRead() {
    return GetInstance().gEnableDegradedRead;
  }

  static void SetRaidLevel(RAIDLevel level) {
    printf("Set raid level: %d\n", (int)level);
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

  static bool GetEventFrameworkEnabled() {
    return true;
  }

  static bool GetIsBrandNew() {
    return GetInstance().gIsBrandNew;
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

  static uint32_t CalculateDiskId(uint32_t stripeId, uint32_t whichBlock, RAIDLevel raidScheme, uint32_t numDisks) {
    Configuration& conf = GetInstance();
    // calculate which disk current block (data/parity) should go
    uint32_t driveId = ~0u;
    uint32_t idOfGroup = stripeId % numDisks;

    if (raidScheme == RAID0 || raidScheme == RAID1 || raidScheme == RAID4) {
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
  bool gIsBrandNew = true;

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
  bool gEnableGc = false;
  int gSyncGroupSize = 512;
  bool gEnableDegradedRead = false;
  uint32_t gNumOpenSegments = 1;
  RAIDLevel gRaidScheme = RAID5;
  bool gEnableHeaderFooter = true;

  SystemMode gSystemMode = NAMED_WRITE;
  // 0: Pure write; 1: Pure zone append; 2: Zone append without metadata; 3: Zone append with metadata; 4: Zone append with redirection

  uint32_t gIndexThreadCoreId = 4;
  uint32_t gDispatchThreadCoreId = 5;
  uint32_t gCompletionThreadCoreId = 6;
  uint32_t gIoThreadCoreIdBase = 7;
  uint32_t gEcThreadCoreId = 8;
};

