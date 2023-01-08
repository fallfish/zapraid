#include "raid_controller.h"

#include <sys/time.h>
#include <algorithm>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/rpc.h>
#include <spdk/event.h>
#include <spdk/init.h>
#include <spdk/string.h>
#include <isa-l.h>
#include <rte_mempool.h>
#include <rte_errno.h>
#include <thread>

#include <algorithm>

#include "zone.h"
#include "messages_and_functions.h"

static void busyWait(bool *ready)
{
  while (!*ready) {
    if (spdk_get_thread() == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(0));
    }
  }
}

static std::vector<Device*> g_devices;

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {

  for (int nsid = 1; nsid <= 1; nsid++) {
    Device* device = new Device();
    device->Init(ctrlr, nsid);
    device->SetDeviceTransportAddress(trid->traddr);
    g_devices.emplace_back(device);
  }

  return;
};

static auto quit(void *args)
{
  exit(0);
}

void RAIDController::initEcThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetEcThreadCoreId(), true);
  mEcThread = spdk_thread_create("ECThread", &cpumask);
  printf("Create EC processing thread %s %lu\n",
      spdk_thread_get_name(mEcThread),
      spdk_thread_get_id(mEcThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetEcThreadCoreId(), ecWorker, this);
  if (rc < 0) {
    printf("Failed to launch ec thread error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initIndexThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetIndexThreadCoreId(), true);
  mIndexThread = spdk_thread_create("IndexThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mIndexThread),
         spdk_thread_get_id(mIndexThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetIndexThreadCoreId(), indexWorker, this);
  if (rc < 0) {
    printf("Failed to launch index completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initCompletionThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetCompletionThreadCoreId(), true);
  mCompletionThread = spdk_thread_create("CompletionThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mCompletionThread),
         spdk_thread_get_id(mCompletionThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetCompletionThreadCoreId(), completionWorker, this);
  if (rc < 0) {
    printf("Failed to launch completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initDispatchThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetDispatchThreadCoreId(), true);
  mDispatchThread = spdk_thread_create("DispatchThread", &cpumask);
  printf("Create dispatch thread %s %lu\n",
         spdk_thread_get_name(mDispatchThread),
         spdk_thread_get_id(mDispatchThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetDispatchThreadCoreId(), dispatchWorker, this);
  if (rc < 0) {
    printf("Failed to launch dispatch thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
  }
}

void RAIDController::initIoThread()
{
  struct spdk_cpuset cpumask;
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_set_cpu(&cpumask, Configuration::GetIoThreadCoreId(threadId), true);
    mIoThread[threadId].thread = spdk_thread_create("IoThread", &cpumask);
    assert(mIoThread[threadId].thread != nullptr);
    mIoThread[threadId].controller = this;
    int rc = spdk_env_thread_launch_pinned(Configuration::GetIoThreadCoreId(threadId), ioWorker, &mIoThread[threadId]);
    printf("ZNS_RAID io thread %s %lu\n", spdk_thread_get_name(mIoThread[threadId].thread), spdk_thread_get_id(mIoThread[threadId].thread));
    if (rc < 0) {
      printf("Failed to launch IO thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
    }
  }
}

void RAIDController::rebuild(uint32_t failedDriveId)
{
  struct timeval s, e;
  gettimeofday(&s, NULL);

  // Valid (full and open) zones and their headers
  std::map<uint64_t, uint8_t*> zonesAndHeaders[mDevices.size()];
  // Segment ID to (wp, SegmentMetadata)
  std::map<uint32_t, std::vector<std::pair<uint64_t, uint8_t*>>> potentialSegments;
  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    if (i == failedDriveId) {
      continue;
    }

    mDevices[i]->ReadZoneHeaders(zonesAndHeaders[i]);
    for (auto zoneAndHeader : zonesAndHeaders[i]) {
      uint64_t wp = zoneAndHeader.first;
      SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zoneAndHeader.second);
      if (potentialSegments.find(segMeta->segmentId) == potentialSegments.end()) {
        potentialSegments[segMeta->segmentId].resize(mDevices.size());
      }
      potentialSegments[segMeta->segmentId][i] = std::pair(wp, zoneAndHeader.second);
    }
  }

  // Filter out invalid segments
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ) {
    auto &zones = it->second;
    bool isValid = true;

    SegmentMetadata *segMeta = nullptr;
    for (uint32_t i = 0; i < mDevices.size(); ++i) {
      if (i == failedDriveId) {
        continue;
      }

      auto zone = zones[i];
      if (zone.second == nullptr) {
        isValid = false;
        break;
      }

      if (segMeta == nullptr) {
        segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
      }

      if (memcmp(segMeta, zone.second, sizeof(SegmentMetadata)) != 0) {
        isValid = false;
        break;
      }
    }

    if (!isValid) {
      it = potentialSegments.erase(it);
    } else {
      ++it;
    }
  }

  uint32_t numZones = mDevices.size();
  uint32_t zoneSize = 2 * 1024 * 1024 / 4;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();
  // repair; allocate buffer in advance (read the whole zone from other drives)
  uint8_t *buffers[numZones];
  uint8_t *metaBufs[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    buffers[i] = (uint8_t*)spdk_zmalloc(zoneSize * blockSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    metaBufs[i] = (uint8_t*)spdk_zmalloc(
      zoneSize * metadataSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    assert(buffers[i] != nullptr);
    assert(metaBufs[i] != nullptr);
  }

  bool alive[numZones];
  for (uint32_t zoneId = 0; zoneId < numZones; ++zoneId) {
    if (zoneId == failedDriveId) {
      alive[zoneId] = false;
    } else {
      alive[zoneId] = true;
    }
  }

  uint32_t groupSize = Configuration::GetStripeGroupSize();
  uint32_t numStripeGroups = mDataRegionSize / groupSize;
  std::vector< std::vector<uint32_t> > stripeIdx2Offsets;
  stripeIdx2Offsets.resize(groupSize);
  for (uint32_t i = 0; i < groupSize; ++i) {
    stripeIdx2Offsets[i].resize(numZones);
  }

  for (auto it = potentialSegments.begin(); it != potentialSegments.end(); ++it) {
    auto &zones = it->second;
    // read the zones from other devices
    uint32_t offsets[numZones];
    for (uint32_t i = 0; i < numZones; ++i) {
      offsets[i] = 0;
    }
    while (true) {
      uint32_t counter = 0;
      for (uint32_t i = 0; i < numZones; ++i) {
        if (i == failedDriveId) {
          continue;
        }
        auto zone = zones[i];
        uint64_t wp = zone.first;
        uint32_t wpInZone = wp % zoneSize;
        uint64_t zslba = wp - wpInZone;
        uint32_t numBlocks2Fetch = std::min(wpInZone - offsets[i], 256u);

        if (numBlocks2Fetch > 0) {
          // update compact stripe table
          int error = 0;
          if ((error = spdk_nvme_ns_cmd_read_with_md(mDevices[i]->GetNamespace(),
                  mDevices[i]->GetIoQueue(0),
                  buffers[i] + offsets[i] * blockSize, metaBufs[i] + offsets[i] * metadataSize,
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
          if (i == failedDriveId) continue;
          spdk_nvme_qpair_process_completions(mDevices[i]->GetIoQueue(0), 0);
        }
      }
    }

    bool concluded = true;
    uint64_t validEnd = 0;
    // rebuild in memory
    if (Configuration::GetSystemMode() == ZAPRAID) {
      uint32_t stripeGroupId = 0;
      for (uint32_t stripeGroupIdx = 0; stripeGroupIdx < numStripeGroups; ++stripeGroupIdx) {
        uint64_t stripeGroupStart = mHeaderRegionSize + stripeGroupIdx * groupSize;
        uint64_t stripeGroupEnd = stripeGroupStart + groupSize;

        // Whether the current group is concluded
        for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
          if (zoneIdx == failedDriveId) {
            continue;
          }
          auto zone = zones[zoneIdx];
          uint64_t wp = zone.first;
          if (stripeGroupEnd > wp % zoneSize) {
            concluded = false;
            break;
          }
        }

        for (uint32_t i = 0; i < groupSize; ++i) {
          for (uint32_t j = 0; j < numZones; ++j) {
            stripeIdx2Offsets[i][j] = ~0;
          }
        }

        if (concluded) {
          // Get the metadata block
          uint16_t* groupMetadata = reinterpret_cast<uint16_t*>(
              buffers[1] + (stripeGroupEnd - 1) * blockSize);

          for (uint32_t i = 0; i < numZones; ++i) {
            for (uint32_t j = 0; j < groupSize; ++j) {
              uint16_t stripeIdx = 0;
              if (i != failedDriveId) {
                stripeIdx = ((BlockMetadata*)(
                        metaBufs[i] + (stripeGroupStart + j) * metadataSize
                    ))->fields.replicated.stripeId;
              } else {
                stripeIdx = j;
              }
              // the i-th block of stripeId-th stripe is the j-th block in the group.
              stripeIdx2Offsets[stripeIdx][i] = j;
            }
          }

          // Repair all the stripes
          for (uint32_t stripeIdx = 0; stripeIdx < groupSize; ++stripeIdx) {
            uint8_t *stripe[numZones];
            // decode data
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = buffers[zoneIdx] + offsetInBlocks * blockSize;
            }
            DecodeStripe(stripeGroupStart + stripeIdx, stripe, alive, numZones,
                Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
                failedDriveId, blockSize);

            // decode metadata
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                        offsetInBlocks * metadataSize))->fields.coded));
            }
            DecodeStripe(stripeGroupStart + stripeIdx, stripe, alive, numZones,
                Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
                failedDriveId, 16);
            ((BlockMetadata*)stripe[0])->fields.replicated.stripeId = stripeIdx;
          }
        } else {
          printf("NOT concluded.\n");
          for (uint32_t i = 0; i < groupSize; ++i) {
            for (uint32_t j = 0; j < numZones; ++j) {
              stripeIdx2Offsets[i][j] = ~0;
            }
          }

          for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
            if (zoneIdx == failedDriveId) {
              continue;
            }
            auto zone = zones[zoneIdx];
            uint64_t wp = zone.first;
            for (uint32_t blockIdx = stripeGroupStart;
                blockIdx < wp % zoneSize; ++blockIdx) {
              BlockMetadata *blockMetadata = (BlockMetadata*)(metaBufs[zoneIdx] + blockIdx * metadataSize);
              uint32_t stripeIdx = blockMetadata->fields.replicated.stripeId;
              stripeIdx2Offsets[stripeIdx][zoneIdx] = blockIdx - stripeGroupStart;
            }
          }

          uint32_t nextValidBlock = 0;
          // Repair all the stripes
          for (uint32_t stripeIdx = 0; stripeIdx < groupSize; ++stripeIdx) {
            uint8_t *stripe[numZones];
            bool validStripe = true;
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              if (zoneIdx == failedDriveId) {
                stripeIdx2Offsets[stripeIdx][zoneIdx] = nextValidBlock;
              }
              if (stripeIdx2Offsets[stripeIdx][zoneIdx] == ~0) {
                validStripe = false;
                break;
              }
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = buffers[zoneIdx] + offsetInBlocks * blockSize;
            }
            if (!validStripe) {
              continue;
            }
            DecodeStripe(stripeGroupStart + stripeIdx, stripe, alive, numZones,
                Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
                failedDriveId, blockSize);

            // decode metadata
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              // stripe[zoneIdx] = (metaBufs[zoneIdx] + offsetInBlocks * metadataSize);
              stripe[zoneIdx] = reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                        offsetInBlocks * metadataSize))->fields.coded));
            }
            DecodeStripe(stripeGroupStart + stripeIdx, stripe, alive, numZones,
                Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
                failedDriveId, 16);
            ((BlockMetadata*)stripe[0])->fields.replicated.stripeId = stripeIdx;
            nextValidBlock += 1;
          }
          validEnd = stripeGroupStart + nextValidBlock;
        }

        if (!concluded) {
          // The last stripe group
          break;
        }
      }
    } else if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
      for (uint32_t stripeIdx = 0; stripeIdx < mDataRegionSize; ++stripeIdx) {
        uint8_t *dataStripe[numZones];
        uint8_t *metaStripe[numZones];
        for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
          auto zone = zones[zoneIdx];
          if (zoneIdx != failedDriveId && stripeIdx + mHeaderRegionSize >= zone.first % zoneSize) {
            printf("%u %u %u\n", stripeIdx, zone.first, zoneSize);
            concluded = false;
            validEnd = mHeaderRegionSize + stripeIdx;
          }
          dataStripe[zoneIdx] = buffers[zoneIdx] + (mHeaderRegionSize + stripeIdx) * blockSize;
          metaStripe[zoneIdx] =
            reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                      (mHeaderRegionSize + stripeIdx) *
                      metadataSize))->fields.coded));
        }
        if (!concluded) {
          printf("NOT concluded.\n");
          break;
        }

        DecodeStripe(mHeaderRegionSize + stripeIdx, dataStripe, alive, numZones,
            Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
            failedDriveId, blockSize);
        DecodeStripe(mHeaderRegionSize + stripeIdx, metaStripe, alive, numZones,
            Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize(),
            failedDriveId, 16);
      }
    }

    if (concluded) {
    // construct the footer
      for (uint32_t offsetInFooterRegion = 0; offsetInFooterRegion < mFooterRegionSize; ++offsetInFooterRegion) {
        uint32_t begin = offsetInFooterRegion * (blockSize / 20);
        uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

        for (uint32_t offsetInDataRegion = begin; offsetInDataRegion < end; ++offsetInDataRegion) {
          uint8_t *footer = buffers[0] + (mHeaderRegionSize + mDataRegionSize + offsetInFooterRegion) * blockSize + (offsetInDataRegion - begin) * 20;
          BlockMetadata *blockMetadata = (BlockMetadata*)(metaBufs[0] + (mHeaderRegionSize + offsetInDataRegion) * metadataSize);
          uint32_t stripeId = blockMetadata->fields.replicated.stripeId;
          uint32_t stripeOffset = (Configuration::GetSystemMode() == ZONEWRITE_ONLY) ?
              offsetInDataRegion : (offsetInDataRegion - offsetInDataRegion % groupSize) + stripeId;
          if (Configuration::CalculateDiskId(mHeaderRegionSize + stripeOffset, numZones - 1, Configuration::GetRaidLevel(), numZones) == 0) {
            // set invalid LBA in the footer for praity blocks, or we just need to decide when reboot - it is only an implementation considerataion
            *(uint64_t*)(footer + 0) = ~0ull;
            *(uint64_t*)(footer + 8) = 0;
          } else {
            *(uint64_t*)(footer + 0) = blockMetadata->fields.coded.lba;
            *(uint64_t*)(footer + 8) = blockMetadata->fields.coded.timestamp;
          }
          *(uint32_t*)(footer +16) = blockMetadata->fields.replicated.stripeId;
        }
      }
      validEnd = mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;
    }

    uint64_t slba = ((SegmentMetadata*)zones[1].second)->zones[0];
    printf("Write recovered zone to %lu\n", slba);
    memcpy(buffers[0], (SegmentMetadata*)zones[1].second, sizeof(SegmentMetadata));
    // write the zone to the failed drive
    for (uint64_t offset = 0; offset < validEnd; offset += 32) {
      int error = 0;
      bool done = false;
      uint32_t count = std::min(32u, (uint32_t)(validEnd - offset));
      if ((error = spdk_nvme_ns_cmd_write_with_md(
              mDevices[0]->GetNamespace(),
              mDevices[0]->GetIoQueue(0),
              buffers[0] + offset * blockSize, metaBufs[0] + offset * metadataSize,
              slba + offset, count, complete, &done, 0, 0, 0)) < 0) {
        printf("Error in writing %d %s.\n", error, strerror(error));
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(mDevices[0]->GetIoQueue(0), 0);
      }
    }

    if (concluded) {
      bool done = false;
      if (spdk_nvme_zns_finish_zone(
            mDevices[0]->GetNamespace(),
            mDevices[0]->GetIoQueue(0),
            slba, 0, complete, &done) != 0) {
        fprintf(stderr, "Seal error in recovering footer region.\n");
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(mDevices[0]->GetIoQueue(0), 0);
      }
    }
  }

  gettimeofday(&e, NULL);
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Rebuild time: %.6f\n", elapsed);
}

void RAIDController::restart()
{
  uint64_t zoneCapacity = mDevices[0]->GetZoneCapacity();
  std::pair<uint64_t, PhysicalAddr> *indexMap =
    new std::pair<uint64_t, PhysicalAddr>[Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize()];
  std::pair<uint64_t, PhysicalAddr> defaultAddr;
  defaultAddr.first = 0;
  defaultAddr.second.segment = nullptr;
  std::fill(indexMap, indexMap + Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize(), defaultAddr);

  struct timeval s, e;
  gettimeofday(&s, NULL);
  // Mount from an existing new array
  // Reconstruct existing segments
  uint32_t zoneSize = 2 * 1024 * 1024 / 4;
  // Valid (full and open) zones and their headers
  std::map<uint64_t, uint8_t*> zonesAndHeaders[mDevices.size()];
  // Segment ID to (wp, SegmentMetadata)
  std::map<uint32_t, std::vector<std::pair<uint64_t, uint8_t*>>> potentialSegments;

  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    mDevices[i]->ReadZoneHeaders(zonesAndHeaders[i]);
    for (auto zoneAndHeader : zonesAndHeaders[i]) {
      uint64_t wp = zoneAndHeader.first;
      SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zoneAndHeader.second);
      if (potentialSegments.find(segMeta->segmentId) == potentialSegments.end()) {
        potentialSegments[segMeta->segmentId].resize(mDevices.size());
      }
      potentialSegments[segMeta->segmentId][i] = std::pair(wp, zoneAndHeader.second);
    }
  }

  // Filter out invalid segments
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ) {
    auto &zones = it->second;
    bool isValid = true;

    SegmentMetadata *segMeta = nullptr;
    for (uint32_t i = 0; i < mDevices.size(); ++i) {
      auto zone = zones[i];
      if (zone.second == nullptr) {
        isValid = false;
        break;
      }

      if (segMeta == nullptr) {
        segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
      }

      if (memcmp(segMeta, zone.second, sizeof(SegmentMetadata)) != 0) {
        isValid = false;
        break;
      }
    }

    if (!isValid) {
      for (uint32_t i = 0; i < mDevices.size(); ++i) {
        auto zone = zones[i];
        uint64_t wp = zone.first;
        SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
        bool done = false;
        if (spdk_nvme_zns_reset_zone(mDevices[i]->GetNamespace(),
            mDevices[i]->GetIoQueue(0),
            segMeta->zones[i],
            0, complete, &done) != 0) {
          printf("Reset error during recovery\n");
        }
        while (!done) {
          spdk_nvme_qpair_process_completions(mDevices[i]->GetIoQueue(0), 0);
        }
        spdk_free(segMeta); // free the allocated metadata memory
      }
      it = potentialSegments.erase(it);
    } else {
      ++it;
    }
  }

  // reconstruct all open, sealed segments (Segment Table)
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ++it) {
    uint32_t segmentId = it->first;
    auto &zones = it->second;
    RequestContextPool *rqPool = mRequestContextPoolForSegments;
    ReadContextPool *rdPool = mReadContextPool;
    StripeWriteContextPool *wPool = nullptr;

    mNextAssignedSegmentId = std::max(segmentId + 1, mNextAssignedSegmentId);

    // Check the segment is sealed or not
    bool sealed = true;
    uint64_t segWp = zoneSize;
    SegmentMetadata *segMeta = nullptr;
    for (auto zone : zones) {
      segWp = std::min(segWp, zone.first % zoneSize);
      segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
    }
    Segment *seg = nullptr;
    if (segWp == zoneCapacity) {
      // sealed
      seg = new Segment(this, segmentId, rqPool, rdPool, wPool);
      for (uint32_t i = 0; i < segMeta->numZones; ++i) {
        Zone *zone = mDevices[i]->OpenZoneBySlba(segMeta->zones[i]);
        seg->AddZone(zone);
      }
      seg->SetSegmentStatus(SEGMENT_SEALED);
      mSealedSegments.insert(seg);
    } else {
      // We assume at most one open segment is created
      wPool = mStripeWriteContextPools[0];
      // open
      seg = new Segment(this, segmentId, rqPool, rdPool, wPool);
      for (uint32_t i = 0; i < segMeta->numZones; ++i) {
        Zone *zone = mDevices[i]->OpenZoneBySlba(segMeta->zones[i]);
        seg->AddZone(zone);
      }
      mOpenSegments[0] = seg;
      seg->SetSegmentStatus(SEGMENT_NORMAL);
    }
    seg->SetZonesAndWpForRecovery(zones);
  }

  // For open segment: seal it if needed (position >= data region size)
  printf("Load all blocks of open segments.\n");
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    if (mOpenSegments[i] != nullptr) {
      mOpenSegments[i]->RecoverLoadAllBlocks();
      if (mOpenSegments[i]->RecoverFooterRegionIfNeeded()) {
        mSealedSegments.insert(mOpenSegments[i]);
        mOpenSegments[i] = nullptr;
      }
    }
  }

  printf("Recover stripe consistency.\n");
  // For open segment: recover stripe consistency
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    if (mOpenSegments[i] != nullptr) {
      if (mOpenSegments[i]->RecoverNeedRewrite()) {
        printf("Need rewrite.\n");
        Segment *newSeg = new Segment(this, mOpenSegments[i]->GetSegmentId(),
            mRequestContextPoolForSegments,
            mReadContextPool,
            mStripeWriteContextPools[i]);
        for (uint32_t j = 0; i < mOpenSegments[i]->GetZones().size(); ++j) {
          Zone *zone = mDevices[i]->OpenZone();
          newSeg->AddZone(zone);
        }
        newSeg->RecoverFromOldSegment(mOpenSegments[i]);
        // reset old segment
        mOpenSegments[i]->ResetInRecovery();
      } else {
        printf("Recover in-flight stripes.\n");
        mOpenSegments[i]->RecoverState();
      }
    } else {
      printf("NO open segment.\n");
    }
  }

  printf("Recover index from sealed segments.\n");
  // Start recover L2P table and the compact stripe table
  uint8_t *buffer = (uint8_t*)spdk_zmalloc(
      std::max(mFooterRegionSize, mDataRegionSize / Configuration::GetStripeGroupSize())
      * Configuration::GetBlockSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  for (Segment *segment : mSealedSegments) {
    segment->RecoverIndexFromSealedSegment(buffer, indexMap);
  }
  spdk_free(buffer);

  printf("Recover index from open segments.\n");
  for (Segment *segment : mOpenSegments) {
    if (segment) {
      segment->RecoverIndexFromOpenSegment(indexMap);
    }
  }

  for (uint32_t i = 0;
      i < Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize();
      ++i) {
    if (indexMap[i].second.segment == nullptr) {
      continue;
    }
    mAddressMap[i] = indexMap[i].second;
    mAddressMap[i].segment->FinishBlock(
        mAddressMap[i].zoneId,
        mAddressMap[i].offset,
        i * Configuration::GetBlockSize() * 1ull);
  }

  for (Segment *segment : mSealedSegments) {
    segment->FinishRecovery();
  }

  for (Segment *segment : mOpenSegments) {
    if (segment) {
      segment->FinishRecovery();
    }
  }
  gettimeofday(&e, NULL);
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Restart time: %.6f\n", elapsed);
}

void RAIDController::Init(bool need_env)
{
  int ret = 0;
  if (need_env) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.core_mask = "0x1ff";
    if (spdk_env_init(&opts) < 0) {
      fprintf(stderr, "Unable to initialize SPDK env.\n");
      exit(-1);
    }

    ret = spdk_thread_lib_init(nullptr, 0);
    if (ret < 0) {
      fprintf(stderr, "Unable to initialize SPDK thread lib.\n");
      exit(-1);
    }
  }

  ret = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL); 
  if (ret < 0) {
    fprintf(stderr, "Unable to probe devices\n");
    exit(-1);
  }

  // init devices
  mDevices = g_devices;
  std::sort(mDevices.begin(), mDevices.end(), [](const Device *o1, const Device *o2) -> bool {
      // there will be no tie between two PCIe address, so no issue without equality test
        return strcmp(o1->GetDeviceTransportAddress(), o2->GetDeviceTransportAddress()) < 0;
      });

  InitErasureCoding();
  // Adjust the capacity for user data = total capacity - footer size
  // The L2P table information at the end of the segment
  // Each block needs (LBA + timestamp + stripe ID, 20 bytes) for L2P table recovery; we round the number to block size
  uint64_t zoneCapacity = mDevices[0]->GetZoneCapacity();
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t maxFooterSize = round_up(zoneCapacity, (blockSize / 20)) / (blockSize / 20);
  mHeaderRegionSize = 1;
  mDataRegionSize = round_down(zoneCapacity - mHeaderRegionSize - maxFooterSize,
                               Configuration::GetStripeGroupSize());
  mFooterRegionSize = round_up(mDataRegionSize, (blockSize / 20)) / (blockSize / 20);
  printf("HeaderRegion: %u, DataRegion: %u, FooterRegion: %u\n",
         mHeaderRegionSize, mDataRegionSize, mFooterRegionSize);

  uint32_t totalNumZones = round_up(Configuration::GetStorageSpaceInBytes() /
                                    Configuration::GetBlockSize(),
                                    mDataRegionSize) / mDataRegionSize;
  uint32_t numDataBlocks =  Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  uint32_t numZonesNeededPerDevice = round_up(totalNumZones, numDataBlocks) / numDataBlocks;
  uint32_t numZonesReservedPerDevice = std::max(3u, (uint32_t)(numZonesNeededPerDevice * 0.25));

  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    mDevices[i]->SetDeviceId(i);
    mDevices[i]->InitZones(numZonesNeededPerDevice, numZonesReservedPerDevice);
  }

  mStorageSpaceThresholdForGcInSegments = numZonesReservedPerDevice / 2;
  mAvailableStorageSpaceInSegments = numZonesNeededPerDevice + numZonesReservedPerDevice;
  printf("Total available segments: %u, reserved segments: %u\n", mAvailableStorageSpaceInSegments, mStorageSpaceThresholdForGcInSegments);

  // Preallocate contexts for user requests
  // Sufficient to support multiple I/O queues of NVMe-oF target
  mRequestContextPoolForUserRequests = new RequestContextPool(2048);
  mRequestContextPoolForSegments = new RequestContextPool(4096);

  mReadContextPool = new ReadContextPool(512, mRequestContextPoolForSegments);

  // Initialize address map
  mAddressMap = new PhysicalAddr[Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize()];
  PhysicalAddr defaultAddr;
  defaultAddr.segment = nullptr;
  std::fill(mAddressMap, mAddressMap + Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize(), defaultAddr);

  // Create poll groups for the io threads and perform initialization
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    mIoThread[threadId].group = spdk_nvme_poll_group_create(NULL, NULL);
  }
  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    struct spdk_nvme_qpair** ioQueues = mDevices[i]->GetIoQueues();
    for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
      spdk_nvme_ctrlr_disconnect_io_qpair(ioQueues[threadId]);
      int rc = spdk_nvme_poll_group_add(mIoThread[threadId].group, ioQueues[threadId]);
      assert(rc == 0);
    }
    mDevices[i]->ConnectIoPairs();
  }

  // Preallocate segments
  mNumOpenSegments = Configuration::GetNumOpenSegments();

  mStripeWriteContextPools = new StripeWriteContextPool *[mNumOpenSegments + 2];
  for (uint32_t i = 0; i < mNumOpenSegments + 2; ++i) {
    if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
      mStripeWriteContextPools[i] = new StripeWriteContextPool(1, mRequestContextPoolForSegments);
    } else {
      mStripeWriteContextPools[i] = new StripeWriteContextPool(64, mRequestContextPoolForSegments);
    }
  }

  mOpenSegments.resize(mNumOpenSegments);


  if (Configuration::GetRebootMode() == 0) {
    for (uint32_t i = 0; i < mDevices.size(); ++i) {
      mDevices[i]->EraseWholeDevice();
    }
  } else if (Configuration::GetRebootMode() == 1) {
    restart();
  } else { // needs rebuild; rebootMode = 2
    // Suppose drive 0 is broken
    mDevices[0]->EraseWholeDevice();
    rebuild(0); // suppose rebuilding drive 0
    restart();
  }

  if (Configuration::GetEventFrameworkEnabled()) {
    event_call(Configuration::GetDispatchThreadCoreId(),
               registerDispatchRoutine, this, nullptr);
    for (uint32_t threadId = 0;
         threadId < Configuration::GetNumIoThreads(); 
         ++threadId) {
      event_call(Configuration::GetIoThreadCoreId(threadId),
                 registerIoCompletionRoutine, &mIoThread[threadId], nullptr);
    }
  } else {
    initIoThread();
    initDispatchThread();
    initIndexThread();
    initCompletionThread();
    initEcThread();
  }

  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    createSegmentIfNeeded(&mOpenSegments[i], i);
  }

  // init Gc
  initGc();

  Configuration::PrintConfigurations();
}

RAIDController::~RAIDController()
{
  Dump();

  delete mAddressMap;
  if (!Configuration::GetEventFrameworkEnabled()) {
    for (uint32_t i = 0; i < Configuration::GetNumIoThreads(); ++i) {
      thread_send_msg(mIoThread[i].thread, quit, nullptr);
    }
    thread_send_msg(mDispatchThread, quit, nullptr);
    thread_send_msg(mEcThread, quit, nullptr);
    thread_send_msg(mIndexThread, quit, nullptr);
    thread_send_msg(mCompletionThread, quit, nullptr);
  }
}


void RAIDController::initGc()
{
  mGcTask.numBuffers = 32;
  mGcTask.dataBuffer = (uint8_t*)spdk_zmalloc(
                     mGcTask.numBuffers * Configuration::GetBlockSize(), 4096,
                     NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.metaBuffer = (uint8_t*)spdk_zmalloc(
                     mGcTask.numBuffers * Configuration::GetMetadataSize(),
                     4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.contextPool = new RequestContext[mGcTask.numBuffers];
  mGcTask.stage = IDLE;
}

uint32_t RAIDController::GcBatchUpdateIndex(
    const std::vector<uint64_t> &lbas,
    const std::vector<std::pair<PhysicalAddr, PhysicalAddr>> &pbas)
{

  uint32_t numSuccessUpdates = 0;
  assert(lbas.size() == pbas.size());
  for (int i = 0; i < lbas.size(); ++i) {
    uint64_t lba = lbas[i];
    PhysicalAddr oldPba = pbas[i].first;
    PhysicalAddr newPba = pbas[i].second;

    if (mAddressMap[lba / Configuration::GetBlockSize()].segment == nullptr) {
      printf("Missing old lba %lu\n", lba);
    }
    if (mAddressMap[lba / Configuration::GetBlockSize()] == oldPba) {
      numSuccessUpdates += 1;
      UpdateIndex(lba, newPba);
    } else {
      newPba.segment->InvalidateBlock(newPba.zoneId, newPba.offset);
    }
  }
  return numSuccessUpdates;
}

void RAIDController::UpdateIndex(uint64_t lba, PhysicalAddr pba)
{
  // Invalidate the old block
  if (lba >= Configuration::GetStorageSpaceInBytes()) {
    printf("Error\n");
    assert(0);
  }
  if (mAddressMap[lba / Configuration::GetBlockSize()].segment != nullptr) {
    PhysicalAddr oldPba = mAddressMap[lba / Configuration::GetBlockSize()];
    oldPba.segment->InvalidateBlock(oldPba.zoneId, oldPba.offset);
    mNumInvalidBlocks += 1;
  }
  assert(pba.segment != nullptr);
  mAddressMap[lba / Configuration::GetBlockSize()] = pba;  
  mNumBlocks += 1;
}

void RAIDController::Write(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  if (Configuration::GetEventFrameworkEnabled()) {
    Request *req = (Request*)calloc(1, sizeof(Request));
    req->controller = this;
    req->offset = offset;
    req->size = size;
    req->data = data;
    req->type = 'W';
    req->cb_fn = cb_fn;
    req->cb_args = cb_args;
    event_call(Configuration::GetReceiverThreadCoreId(),
        executeRequest, req, nullptr);
  } else {
    Execute(offset, size, data, true, cb_fn, cb_args);
  }
}

void RAIDController::Read(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  if (Configuration::GetEventFrameworkEnabled()) {
    Request *req = (Request*)calloc(1, sizeof(Request));
    req->controller = this;
    req->offset = offset;
    req->size = size;
    req->data = data;
    req->type = 'R';
    req->cb_fn = cb_fn;
    req->cb_args = cb_args;
    event_call(Configuration::GetReceiverThreadCoreId(),
        executeRequest, req, nullptr);
  } else {
    Execute(offset, size, data, false, cb_fn, cb_args);
  }
}

void RAIDController::ReclaimContexts()
{
  int numSuccessfulReclaims = 0;
  for (auto it = mInflightRequestContext.begin();
            it != mInflightRequestContext.end(); ) {
    if ((*it)->available) {
      (*it)->Clear();
      mRequestContextPoolForUserRequests->ReturnRequestContext(*it);
      it = mInflightRequestContext.erase(it);

      numSuccessfulReclaims++;
      if (numSuccessfulReclaims >= 128) {
        break;
      }
    } else {
      ++it;
    }
  }
}

void RAIDController::Flush()
{
  bool remainWrites;
  do {
    remainWrites = false;
    for (auto it = mInflightRequestContext.begin();
              it != mInflightRequestContext.end(); ) {
      if ((*it)->available) {
        (*it)->Clear();
        mRequestContextPoolForUserRequests->ReturnRequestContext(*it);
        it = mInflightRequestContext.erase(it);
      } else {
        if ((*it)->req_type == 'W') {
          remainWrites = true;
        }
        ++it;
      }
    }
  } while (remainWrites);
}

RequestContext* RAIDController::getContextForUserRequest()
{
  RequestContext *ctx = mRequestContextPoolForUserRequests->GetRequestContext(false);
  while (ctx == nullptr) {
    ReclaimContexts();
    ctx = mRequestContextPoolForUserRequests->GetRequestContext(false);
    if (ctx == nullptr) {
//      printf("NO AVAILABLE CONTEXT FOR USER.\n");
    }
  }

  mInflightRequestContext.insert(ctx);
  ctx->Clear();
  ctx->available = false;
  ctx->meta = nullptr;
  ctx->ctrl = this;
  return ctx;
}

void RAIDController::Execute(
    uint64_t offset, uint32_t size, void *data, bool is_write,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  RequestContext *ctx = getContextForUserRequest();
  ctx->type = USER;
  ctx->data = (uint8_t*)data;
  ctx->lba = offset;
  ctx->size = size;
  ctx->targetBytes = size;
  ctx->cb_fn = cb_fn;
  ctx->cb_args = cb_args;
  if (is_write) {
    ctx->req_type = 'W';
    ctx->status = WRITE_REAPING;
  } else {
    ctx->req_type = 'R';
    ctx->status = READ_PREPARE;
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mDispatchThread, enqueueRequest, ctx);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               enqueueRequest2, ctx, nullptr);
  }

  return ;
}

void RAIDController::EnqueueWrite(RequestContext *ctx)
{
  mWriteQueue.push(ctx);
}

void RAIDController::EnqueueReadPrepare(RequestContext *ctx)
{
  mReadPrepareQueue.push(ctx);
}

void RAIDController::EnqueueReadReaping(RequestContext *ctx)
{
  mReadReapingQueue.push(ctx);
}

std::queue<RequestContext*>& RAIDController::GetWriteQueue()
{
  return mWriteQueue;
}

std::queue<RequestContext*>& RAIDController::GetReadPrepareQueue()
{
  return mReadPrepareQueue;
}

std::queue<RequestContext*>& RAIDController::GetReadReapingQueue()
{
  return mReadReapingQueue;
}

void RAIDController::WriteInDispatchThread(RequestContext *ctx)
{
  if (mAvailableStorageSpaceInSegments <= 1) {
    return;
  }

  static uint32_t tick = 0;
  static uint32_t lastTick = 0;
  static uint32_t last = 0;
  static uint32_t stucks = 0;

  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t curOffset = ctx->curOffset;
  uint32_t size = ctx->size;
  uint32_t numBlocks = size / blockSize;

  if (curOffset == 0 && ctx->timestamp == ~0ull) {
    ctx->timestamp = ++mGlobalTimestamp;
    ctx->pbaArray.resize(numBlocks);
  }

  uint32_t pos = ctx->curOffset;
  for (; pos < numBlocks; pos += 1) {
    uint32_t openGroupId = 0;
    bool success = false;

    for (uint32_t trys = 0; trys < mNumOpenSegments; trys += 1) {
      success = mOpenSegments[openGroupId]->Append(ctx, pos);
      if (mOpenSegments[openGroupId]->IsFull()) {
        mSegmentsToSeal.emplace_back(mOpenSegments[openGroupId]);
        mOpenSegments[openGroupId] = nullptr;
        createSegmentIfNeeded(&mOpenSegments[openGroupId], openGroupId);
      }
      if (success) {
        break;
      }
      openGroupId = (openGroupId + 1) % mNumOpenSegments; 
    }

    if (!success) {
      break;
    }
  }
  ctx->curOffset = pos;
}

bool RAIDController::LookupIndex(uint64_t lba, PhysicalAddr *pba)
{
  
  if (mAddressMap[lba / Configuration::GetBlockSize()].segment != nullptr) {
    *pba = mAddressMap[lba / Configuration::GetBlockSize()];
    return true;
  } else {
    pba->segment = nullptr;
    return false;
  }
}

void RAIDController::ReadInDispatchThread(RequestContext *ctx)
{
  uint64_t slba = ctx->lba;
  int size = ctx->size;
  void *data = ctx->data;
  uint32_t numBlocks = size / Configuration::GetBlockSize();

  if (ctx->status == READ_PREPARE) {
    if (mGcTask.stage != INDEX_UPDATE_COMPLETE) {
      // For any reads that may read the input segment,
      // track its progress such that the GC will
      // not delete input segment before they finishes
      mReadsInCurrentGcEpoch.insert(ctx);
    }

    ctx->status = READ_INDEX_QUERYING;
    ctx->pbaArray.resize(numBlocks);
    if (!Configuration::GetEventFrameworkEnabled()) {
      QueryPbaArgs *args = (QueryPbaArgs *)calloc(1, sizeof(QueryPbaArgs));
      args->ctrl = this;
      args->ctx = ctx;
      thread_send_msg(mIndexThread, queryPba, args);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 queryPba2, this, ctx);
    }
  } else if (ctx->status == READ_REAPING) {
    uint32_t i = ctx->curOffset;
    for (; i < numBlocks; ++i) {
      Segment *segment = ctx->pbaArray[i].segment;
      if (segment == nullptr) {
        uint8_t *block = (uint8_t *)data + i * Configuration::GetBlockSize();
        memset(block, 0, Configuration::GetBlockSize());
        ctx->successBytes += Configuration::GetBlockSize();
        if (ctx->successBytes == ctx->targetBytes) {
          ctx->Queue();
        }
      } else {
        if (!segment->Read(ctx, i, ctx->pbaArray[i])) {
          break;
        }
      }
    }
    ctx->curOffset = i;
  }
}

bool RAIDController::scheduleGc()
{
  if (mAvailableStorageSpaceInSegments > mStorageSpaceThresholdForGcInSegments) {
    return false;
  }

  // Use Greedy algorithm to pick segments
  std::vector<Segment*> groups;
  for (Segment *segment : mSealedSegments) {
      groups.emplace_back(segment);
  }
  if (groups.size() == 0) {
    return false;
  }
  std::sort(groups.begin(), groups.end(), [](const Segment *lhs, const Segment *rhs) {
      double score1 = (double)lhs->GetNumInvalidBlocks() / lhs->GetNumBlocks();
      double score2 = (double)rhs->GetNumInvalidBlocks() / rhs->GetNumBlocks(); 
      return score1 > score2;
      });

  mGcTask.inputSegment = groups[0];
  printf("Select: %p, Score: %f, Invalid: %u, Valid: %u\n", mGcTask.inputSegment,
      (double)groups[0]->GetNumInvalidBlocks() / groups[0]->GetNumBlocks(),
      groups[0]->GetNumInvalidBlocks(), groups[0]->GetNumBlocks());

  mGcTask.maxZoneId = mDevices.size();

  printf("Schedule GC. Available storage: %u %u\n",
         mAvailableStorageSpaceInSegments,
         mStorageSpaceThresholdForGcInSegments);

  return true;
}

bool RAIDController::ProceedGc()
{
  bool hasProgress = false;
  if (!Configuration::GetEnableGc()) {
    return hasProgress;
  }

  if (mGcTask.stage == IDLE) { // IDLE
    if (scheduleGc()) {
      hasProgress = true;
      mGcTask.stage = INIT;
    }
  }

  if (mGcTask.stage == INIT) {
    initializeGcTask();
  }

  if (mGcTask.stage == REWRITING) {
    hasProgress |= progressGcWriter();
    hasProgress |= progressGcReader();

    if (mGcTask.curZoneId == mGcTask.maxZoneId) {
      if (mGcTask.numWriteSubmitted == mGcTask.numReads
          && mGcTask.numWriteFinish == mGcTask.numWriteSubmitted) {
        assert(mGcTask.mappings.size() == mGcTask.numWriteFinish);
        mGcTask.stage = REWRITE_COMPLETE;
      }
    }
  } 
  
  if (mGcTask.stage == REWRITE_COMPLETE) {
    hasProgress = true;
    mGcTask.stage = INDEX_UPDATING;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(mIndexThread, progressGcIndexUpdate, this);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
          progressGcIndexUpdate2, this, nullptr);
    }
  }

  if (mGcTask.stage == INDEX_UPDATING_BATCH) {
    if (mGcTask.mappings.size() != 0) {
      hasProgress = true;
      mGcTask.stage = INDEX_UPDATING;
      if (!Configuration::GetEventFrameworkEnabled()) {
        thread_send_msg(mIndexThread, progressGcIndexUpdate, this);
      } else {
        event_call(Configuration::GetIndexThreadCoreId(),
            progressGcIndexUpdate2, this, nullptr);
      }
    } else { // Finish updating all mappings
      hasProgress = true;
      mGcTask.stage = INDEX_UPDATE_COMPLETE;
    }
  }

  if (mGcTask.stage == INDEX_UPDATE_COMPLETE) {
    if (mReadsInCurrentGcEpoch.empty()) {
      hasProgress = true;
      mGcTask.inputSegment->Reset(nullptr);
      mGcTask.stage = RESETTING_INPUT_SEGMENT;
    }
  }

  if (mGcTask.stage == RESETTING_INPUT_SEGMENT) {
    if (mGcTask.inputSegment->IsResetDone()) {
      auto zones = mGcTask.inputSegment->GetZones();
      for (uint32_t i = 0; i < zones.size(); ++i) {
        mDevices[i]->ReturnZone(zones[i]);
      }
      mSealedSegments.erase(mGcTask.inputSegment);
      delete mGcTask.inputSegment;
      mAvailableStorageSpaceInSegments += 1;
      mGcTask.stage = IDLE;
    }
  }

  return hasProgress;
}

void RAIDController::Drain()
{
  printf("Perform draining on the system.\n");
  DrainArgs args;
  args.ctrl = this;
  args.success = false;
  while (!args.success) {
    args.ready = false;
    thread_send_msg(mDispatchThread, tryDrainController, &args);
    busyWait(&args.ready);
  }
}

std::queue<RequestContext*>& RAIDController::GetEventsToDispatch()
{
  return mEventsToDispatch;
}

void RAIDController::EnqueueEvent(RequestContext *ctx)
{
  mEventsToDispatch.push(ctx);
}

int RAIDController::GetNumInflightRequests()
{
  return mInflightRequestContext.size();
}

bool RAIDController::ExistsGc()
{
  return mGcTask.stage != IDLE;
}

void RAIDController::createSegmentIfNeeded(Segment **segment, uint32_t spId)
{
  if (*segment != nullptr) return;
  // Check there are available zones
  if (mAvailableStorageSpaceInSegments == 0) {
    assert(0);
    printf("No available storage; this should never happen!\n");
    return ;
  }

  mAvailableStorageSpaceInSegments -= 1;
  Segment *seg = new Segment(this, mNextAssignedSegmentId++,
                             mRequestContextPoolForSegments, mReadContextPool,
                             mStripeWriteContextPools[spId]);
  for (uint32_t i = 0; i < mDevices.size(); ++i) {
    Zone* zone = mDevices[i]->OpenZone();
    if (zone == nullptr) {
      printf("No available zone in device %d, storage space is exhuasted!\n", i);
    }
    seg->AddZone(zone);
  }

  if (spId == mNumOpenSegments + 1) {
    printf("Create spare segment %p\n", seg);
  } else {
    printf("Create normal segment %p\n", seg);
  }
  seg->FinalizeCreation();
  *segment = seg;
}

std::queue<RequestContext*>& RAIDController::GetRequestQueue()
{
  return mRequestQueue;
}

std::mutex& RAIDController::GetRequestQueueMutex()
{
  return mRequestQueueMutex;
}

struct spdk_thread *RAIDController::GetIoThread(int id)
{
  return mIoThread[id].thread;
}

struct spdk_thread *RAIDController::GetDispatchThread()
{
  return mDispatchThread;
}

struct spdk_thread *RAIDController::GetEcThread()
{
  return mEcThread;
}

struct spdk_thread *RAIDController::GetIndexThread()
{
  return mIndexThread;
}

struct spdk_thread *RAIDController::GetCompletionThread()
{
  return mCompletionThread;
}

void RAIDController::initializeGcTask()
{
  mGcTask.curZoneId = 0;
  mGcTask.nextOffset = 0;
  mGcTask.stage = REWRITING;

  mGcTask.writerPos = 0;
  mGcTask.readerPos = 0;

  mGcTask.numWriteSubmitted = 0;
  mGcTask.numWriteFinish = 0;
  mGcTask.numReads = 0;

  mGcTask.mappings.clear();

  // Initialize the status of the context pool
  for (uint32_t i = 0; i < mGcTask.numBuffers; ++i) {
    mGcTask.contextPool[i].Clear();
    mGcTask.contextPool[i].available = true;
    mGcTask.contextPool[i].ctrl = this;
    mGcTask.contextPool[i].pbaArray.resize(1);
    mGcTask.contextPool[i].gcTask = &mGcTask;
    mGcTask.contextPool[i].type = GC;
    mGcTask.contextPool[i].lba = ~0ull;
    mGcTask.contextPool[i].data = (uint8_t*)mGcTask.dataBuffer + i * Configuration::GetStripeUnitSize();
    mGcTask.contextPool[i].meta = (uint8_t*)mGcTask.metaBuffer + i * Configuration::GetMetadataSize();
    mGcTask.contextPool[i].targetBytes = Configuration::GetBlockSize();
    mGcTask.contextPool[i].status = WRITE_COMPLETE;
  }
}


bool RAIDController::progressGcReader()
{
  bool hasProgress = false;
  // Find contexts that are available, schedule read for valid blocks
  RequestContext *nextReader = &mGcTask.contextPool[mGcTask.readerPos];
  while (nextReader->available && (nextReader->status == WRITE_COMPLETE)) {
    if (nextReader->lba != ~0ull) {
      // The sign of valid lba means a successful rewrite a valid block
      // So we update the information here
      mGcTask.numWriteFinish += 1;
      mGcTask.mappings[nextReader->lba].second = nextReader->pbaArray[0];
    }

    nextReader->available = false;
    nextReader->lba = 0;

    bool valid = false;
    bool success = true;
    if (mGcTask.curZoneId != mGcTask.maxZoneId) {
      nextReader->req_type = 'R';
      nextReader->status = READ_REAPING;
      nextReader->successBytes = 0;

      do {
        nextReader->segment = mGcTask.inputSegment;
        nextReader->zoneId = mGcTask.curZoneId;
        nextReader->offset = mGcTask.nextOffset;

        success = mGcTask.inputSegment->ReadValid(nextReader, 0, nextReader->GetPba(), &valid);
        if (!success) break;

        mGcTask.nextOffset += 1;
        if (mGcTask.nextOffset == mHeaderRegionSize + mDataRegionSize) {
          mGcTask.nextOffset = mHeaderRegionSize;
          mGcTask.curZoneId += 1;
        }
      } while (!valid && mGcTask.curZoneId != mGcTask.maxZoneId);
      if (valid) {
        mGcTask.numReads += 1;
      }
    }
    if (!success) {
      // will retry later
      nextReader->status = WRITE_COMPLETE;
      nextReader->available = true;
      nextReader->lba = ~0ull;
      break;
    }
    hasProgress = true;
    mGcTask.readerPos = (mGcTask.readerPos + 1) % mGcTask.numBuffers;
    nextReader = &mGcTask.contextPool[mGcTask.readerPos];
  }

  return hasProgress;
}

bool RAIDController::progressGcWriter()
{
  bool hasProgress = false;
  // Process blocks that are read and valid, and rewrite them 
  RequestContext *nextWriter = &mGcTask.contextPool[mGcTask.writerPos];
  while (nextWriter->available && nextWriter->status == READ_COMPLETE) {
    uint64_t lba = ((BlockMetadata*)nextWriter->meta)->fields.coded.lba;
    if (lba == ~0ull) {
      fprintf(stderr, "GC write does not expect block with invalid lba!\n");
      exit(-1);
    }
    assert(lba != ~0ull);

    PhysicalAddr oldPba = nextWriter->GetPba();
    RequestContext backup; // Backup prevents from context lost due to retry
    backup.CopyFrom(*nextWriter);
    nextWriter->lba = lba;
    nextWriter->req_type = 'W';
    nextWriter->status = WRITE_REAPING;
    nextWriter->successBytes = 0;
    nextWriter->available = false;
    nextWriter->timestamp = ((BlockMetadata*)nextWriter->meta)->fields.coded.timestamp;

    bool success = false;
    for (uint32_t i = 0; i < mNumOpenSegments; i += 1) {
      success = mOpenSegments[i]->Append(nextWriter, 0);
      if (mOpenSegments[i]->IsFull()) {
        mSegmentsToSeal.emplace_back(mOpenSegments[i]);
        mOpenSegments[i] = nullptr;
        createSegmentIfNeeded(&mOpenSegments[i], i);
      }
      if (success) {
        break;
      }
    }
    if (!success) {
      nextWriter->CopyFrom(backup);
      break;
    }

    mGcTask.mappings[lba] = std::make_pair(oldPba, PhysicalAddr());
    mGcTask.numWriteSubmitted += 1;

    mGcTask.writerPos = (mGcTask.writerPos + 1) % mGcTask.numBuffers;
    nextWriter = &mGcTask.contextPool[mGcTask.writerPos];

    hasProgress = true;
  }
  return hasProgress;
}

bool RAIDController::CheckSegments()
{
  bool stateChanged = false;
  for (uint32_t i = 0; i < mOpenSegments.size(); ++i) {
    if (mOpenSegments[i] != nullptr) {
      stateChanged |= mOpenSegments[i]->StateTransition();
      if (mOpenSegments[i]->IsFull()) {
        mSegmentsToSeal.emplace_back(mOpenSegments[i]);
        mOpenSegments[i] = nullptr;
        createSegmentIfNeeded(&mOpenSegments[i], i);
      }
    }
  }

  for (auto it = mSegmentsToSeal.begin();
      it != mSegmentsToSeal.end();
      )
  {
    stateChanged |= (*it)->StateTransition();
    if ((*it)->GetStatus() == SEGMENT_SEALED) {
      printf("Sealed segment %p.\n", (*it));
      mSealedSegments.insert(*it);
      it = mSegmentsToSeal.erase(it);
    } else {
      ++it;
    }
  }

  return stateChanged;
}

GcTask* RAIDController::GetGcTask()
{
  return &mGcTask;
}

uint32_t RAIDController::GetHeaderRegionSize()
{
  return mHeaderRegionSize;
}

uint32_t RAIDController::GetDataRegionSize()
{
  return mDataRegionSize;
}

uint32_t RAIDController::GetFooterRegionSize()
{
  return mFooterRegionSize;
}

void RAIDController::RemoveRequestFromGcEpochIfNecessary(RequestContext *ctx)
{
  if (mReadsInCurrentGcEpoch.empty()) {
    return;
  }

  if (mReadsInCurrentGcEpoch.find(ctx) != mReadsInCurrentGcEpoch.end()) {
    mReadsInCurrentGcEpoch.erase(ctx);
  }
}

void RAIDController::Dump()
{
  // Dump address map
//  for (uint32_t i = 0; i < Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize(); ++i) {
//    if (mAddressMap[i].segment != nullptr) {
//      printf("%llu %u %u %u\n", 
//          i * Configuration::GetBlockSize() * 1ull, 
//          mAddressMap[i].segment->GetSegmentId(),
//          mAddressMap[i].zoneId,
//          mAddressMap[i].offset);
//    }
//  }

  std::map<uint64_t, Segment*> orderedSegments;
  // Dump the information of each segment
  for (auto segment : mOpenSegments) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto segment : mSegmentsToSeal) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto segment : mSealedSegments) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto pr : orderedSegments) {
    pr.second->Dump();
  }
}
