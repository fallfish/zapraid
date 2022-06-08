#include "raid_controller.h"
#include <sys/time.h>
#include "zns_raid.h"
#include <unistd.h>

uint32_t gSize = 1 * 1024 * 1024 * 1024ull / Configuration::GetBlockSize();

RAIDController *gRaidController;
void validate()
{
  char *readValidateBuffer;
  readValidateBuffer = new char[gSize * Configuration::GetBlockSize()];
  char buffer[Configuration::GetBlockSize()];
  char tmp[Configuration::GetBlockSize()];
  printf("Validating...\n");
  struct timeval s, e;
  gettimeofday(&s, NULL);
  for (uint64_t i = 0; i < gSize; ++i) {
    gRaidController->Read(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), readValidateBuffer + i * Configuration::GetBlockSize(), nullptr, nullptr);
  }
  printf("Read finish\n");
  gRaidController->Drain();
  gettimeofday(&e, NULL);
  double mb = (double)gSize * Configuration::GetBlockSize() / 1024 / 1024;
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Throughtput: %.2fMiB/s\n", mb / elapsed);
  for (uint64_t i = 0; i < gSize; ++i) {
    sprintf(buffer, "temp%d", i * 7);
    if (strcmp(buffer, readValidateBuffer + i * Configuration::GetBlockSize()) != 0) {
      printf("Mismatch %d\n", i);
      assert(0);
      break;
    }
  }
  printf("Validate successful!\n");
}

struct LatencyBucket
{
  struct timeval s, e;
  char *buffer;
  void print() {
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
    printf("%f\n", elapsed);
  }
};

LatencyBucket *buckets;
static void latency_puncher(void *arg)
{
  LatencyBucket *b = (LatencyBucket*)arg;
  gettimeofday(&(b->e), NULL);
  delete b->buffer;
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
  
  struct timeval s, e;
  gettimeofday(&s, NULL);
  for (uint64_t i = 0; i < gSize; i += 1) {
    buckets[i].buffer = new char[gSize];
//    memset(buffer, 0, sizeof(buffer));
    sprintf(buckets[i].buffer, "temp%d", i * 7);
    gettimeofday(&buckets[i].s, NULL);
    gRaidController->Write(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), buckets[i].buffer, latency_puncher, &buckets[i]);
  }
  gRaidController->Drain();
  for (uint64_t i = 0; i < gSize; i += 2) {
    buckets[i].buffer = new char[gSize];
    sprintf(buckets[i].buffer, "temp%d", i * 7);
    gettimeofday(&buckets[i].s, NULL);
    gRaidController->Write(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), buckets[i].buffer, latency_puncher, &buckets[i]);
  }
  gRaidController->Drain();
  for (uint64_t i = 1; i < gSize; i += 2) {
    buckets[i].buffer = new char[gSize];
    sprintf(buckets[i].buffer, "temp%d", i * 7);
    gettimeofday(&buckets[i].s, NULL);
    gRaidController->Write(i * Configuration::GetBlockSize(), Configuration::GetBlockSize(), buckets[i].buffer, latency_puncher, &buckets[i]);
  }
  gRaidController->Drain();

  gettimeofday(&e, NULL);
  double mb = (double)gSize * Configuration::GetBlockSize() / 1024 / 1024;
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Throughtput: %.2fMiB/s\n", mb / elapsed);

  validate();
  delete gRaidController;
}
