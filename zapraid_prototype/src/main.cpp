#include "raid_controller.h"
#include <sys/time.h>
#include "zns_raid.h"
#include <unistd.h>
#include <thread>
#include <chrono>
#include <thread>

#include <rte_errno.h>

// a simple test program to ZapRAID
uint64_t gSize = 20u * 1024 * 1024 * 1024 * 3 / Configuration::GetBlockSize();
uint32_t numLoops = 2;
uint32_t numBuffers = 1024 * 128;

RAIDController *gRaidController;
uint8_t *buffer_pool;

struct LatencyBucket
{
  struct timeval s, e;
  uint8_t *buffer;
  bool done;
  void print() {
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
    printf("%f\n", elapsed);
  }
};


void readCallback(void *arg) {
  LatencyBucket *b = (LatencyBucket*)arg;
  b->done = true;
}

void setdone(void *arg) {
  bool *done = (bool*)arg;
  *done = true;
}

void validate()
{
  uint8_t *readValidateBuffer = buffer_pool;
  char buffer[Configuration::GetBlockSize()];
  char tmp[Configuration::GetBlockSize()];
  printf("Validating...\n");
  struct timeval s, e;
  gettimeofday(&s, NULL);
//  Configuration::SetEnableDegradedRead(true);
  for (uint64_t i = 0; i < gSize / 10; ++i) {
    LatencyBucket b;
    gettimeofday(&b.s, NULL);
    bool done;
    done = false;
    gRaidController->Read(
        i * Configuration::GetBlockSize(), Configuration::GetBlockSize(),
        readValidateBuffer + i % numBuffers * Configuration::GetBlockSize(),
        setdone, &done);
    while (!done) {
      std::this_thread::yield();
    }
    sprintf(buffer, "temp%u", i * 7);
    if (strcmp(buffer, 
          (char*)readValidateBuffer + i % numBuffers * Configuration::GetBlockSize()) != 0) {
      printf("Mismatch %u\n", i);
      assert(0);
      break;
    }
  }
  printf("Read finish\n");
  gRaidController->Drain();
  gettimeofday(&e, NULL);
  double mb = (double)gSize * Configuration::GetBlockSize() / 1024 / 1024;
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Throughtput: %.2fMiB/s\n", mb / elapsed);
  printf("Validate successful!\n");
}

LatencyBucket *buckets;
static void latency_puncher(void *arg)
{
  LatencyBucket *b = (LatencyBucket*)arg;
  gettimeofday(&(b->e), NULL);
//  delete b->buffer;
//  b->print();
}

int main(int argc, char *argv[])
{
  // Retrieve the options:
  int opt;
  while ((opt = getopt(argc, argv, "m:n:s:")) != -1) {  // for each option...
    switch (opt) {
      case 'm':
        Configuration::SetSystemMode(SystemMode(atoi(optarg)));
        break;
      case 'n':
        Configuration::SetRebootMode(atoi(optarg));
        break;
      case 's':
        gSize = atoi(optarg) * 1024 * 1024 * 1024ull / Configuration::GetBlockSize();
        break;
      default:
        fprintf(stderr, "Unknown option %c\n", opt);
        break;
    }
  }
  gRaidController = new RAIDController();
  gRaidController->Init(true);

  buckets = new LatencyBucket[gSize];
  
  buffer_pool = (uint8_t*)spdk_zmalloc(
      numBuffers * Configuration::GetBlockSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);

  if (Configuration::GetRebootMode() == 0) {
    struct timeval s, e;
    gettimeofday(&s, NULL);
    uint64_t totalSize = 0;
    for (uint64_t i = 0; i < gSize; i += 1) {
      buckets[i].buffer = buffer_pool + i % numBuffers * Configuration::GetBlockSize();
      sprintf((char*)buckets[i].buffer, "temp%u", i * 7);
      gettimeofday(&buckets[i].s, NULL);
      gRaidController->Write(i * Configuration::GetBlockSize(),
          1 * Configuration::GetBlockSize(),
          buckets[i].buffer,
          nullptr, nullptr);
    //  std::this_thread::sleep_for(std::chrono::milliseconds(1));

      totalSize += 4096;
    }
    for (int loop = 0; loop < numLoops; ++loop) {
      for (uint64_t i = 0; i < gSize; i += random() % 7 + 1) {
        buckets[i].buffer = buffer_pool + i % numBuffers * Configuration::GetBlockSize();
        memset(buckets[i].buffer, 0, 4096);
        sprintf((char*)buckets[i].buffer, "temp%u", i * 7);
        gettimeofday(&buckets[i].s, NULL);
        gRaidController->Write(i * Configuration::GetBlockSize(),
                               1 * Configuration::GetBlockSize(),
                               buckets[i].buffer,
                               nullptr, nullptr);
      }
      totalSize += 4096;
    }
    gRaidController->Drain();

    gettimeofday(&e, NULL);
    double mb = totalSize / 1024 / 1024;
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
    printf("Total: %.2f MiB/s, Throughtput: %.2f MiB/s\n", mb, mb / elapsed);
  }

  validate();
  delete gRaidController;
}
