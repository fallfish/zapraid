#include "raid_controller.h"
#include <sys/time.h>
#include "zns_raid.h"
#include <unistd.h>
#include <thread>

#include <rte_errno.h>

//  uint32_t gSize = 128 * 1024 * 1024ull / Configuration::GetBlockSize();
uint32_t gSize = 4 * 1024 * 1024 * 1024ull / Configuration::GetBlockSize();

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
  for (uint64_t i = 0; i < gSize; ++i) {
    LatencyBucket b;
    gettimeofday(&b.s, NULL);
    bool done;
    done = false;
    gRaidController->Read(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), readValidateBuffer + i % 1024 * Configuration::GetBlockSize(), setdone, &done);
    while (!done) {
      std::this_thread::yield();
    //  std::this_thread::sleep_for(std::chrono::seconds(0));
    }
    sprintf(buffer, "temp%d", i * 7);
    if (strcmp(buffer, (char*)readValidateBuffer + i % 1024 * Configuration::GetBlockSize()) != 0) {
      printf("Mismatch %d\n", i);
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
  while ((opt = getopt(argc, argv, "m:")) != -1) {  // for each option...
    switch (opt) {
      case 'm':
        Configuration::SetSystemMode(SystemMode(atoi(optarg)));
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
      16384 * Configuration::GetBlockSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY,
      SPDK_MALLOC_DMA);
  struct timeval s, e;
  gettimeofday(&s, NULL);
  for (uint64_t i = 0; i < gSize; i += 1) {
    buckets[i].buffer = buffer_pool + i % 1024 * Configuration::GetBlockSize();
    sprintf((char*)buckets[i].buffer, "temp%d", i * 7);
    gettimeofday(&buckets[i].s, NULL);
//    gRaidController->Write(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), buckets[i].buffer, latency_puncher, &buckets[i]);
    gRaidController->Write(i * Configuration::GetBlockSize(),
                           1 * Configuration::GetBlockSize(),
                           buckets[i].buffer,
                           nullptr, nullptr);
  }
  for (uint64_t i = 0; i < gSize; i += 1) {
    buckets[i].buffer = buffer_pool + i % 1024 * Configuration::GetBlockSize();
    sprintf((char*)buckets[i].buffer, "temp%d", i * 7);
    gettimeofday(&buckets[i].s, NULL);
    gRaidController->Write(i * Configuration::GetBlockSize(),
                           1 * Configuration::GetBlockSize(),
                           buckets[i].buffer,
                           nullptr, nullptr);
  }
  gRaidController->Drain();

  gettimeofday(&e, NULL);
  double mb = (double)gSize * Configuration::GetBlockSize() / 1024 / 1024;
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Throughtput: %.2fMiB/s\n", mb / elapsed);

  validate();
  delete gRaidController;
}
