#include <sys/time.h>

#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"

uint64_t dataset_size = 64 * 1024 * 1024 / 4; // 64GiB / 4KiB

struct timeval g_time_s, g_time_e;
struct timeval t_time_s, t_time_e;
struct spdk_nvme_ctrlr *g_ctrlr = nullptr;
struct spdk_nvme_ns *g_ns = nullptr;
struct spdk_nvme_qpair* g_qpair = nullptr;
uint8_t *g_data = nullptr;
uint32_t g_num_issued = 0, g_num_completed = 0;
uint32_t g_lba = 0;
uint32_t g_zone_size = 2 * 1024 * 1024 / 4;
uint32_t g_zone_capacity = 1077 * 1024 / 4;
uint32_t g_mode = 0;
uint32_t g_num_concurrent_writes = 1;
uint32_t g_req_size = 1;

void write_block(uint64_t lba);

static uint64_t round_up(uint64_t a, uint64_t b)
{
  return a / b * b + b;
}

static uint64_t round_down(uint64_t a, uint64_t b)
{
  return a / b * b;
}

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {

  if (g_ctrlr == nullptr) {
    g_ctrlr = ctrlr;
    g_ns = spdk_nvme_ctrlr_get_ns(ctrlr, 1);

    g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
  }

  return;
};

void complete(void *arg, const struct spdk_nvme_cpl *completion) {
  gettimeofday(&t_time_e, NULL);
  double elapsed = t_time_e.tv_sec - t_time_s.tv_sec 
    + t_time_e.tv_usec / 1000000. - t_time_s.tv_usec / 1000000.;

  g_num_completed += g_req_size;

  if (g_num_issued < dataset_size) {
    write_block(g_lba);
    g_lba += g_req_size;
    if (g_lba % g_zone_size == g_zone_capacity) {
      g_lba = round_up(g_lba, g_zone_size);
    }
  }

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write zone I/O failed, aborting run\n");
    exit(1);
  }
};

void write_block(uint64_t lba)
{
  g_num_issued += g_req_size;
  if (g_mode == 0) {
    int rc = spdk_nvme_ns_cmd_write(
        g_ns, g_qpair,
        g_data, lba, g_req_size,
        complete, nullptr, 0);
    if (rc < 0) {
      printf("write error.");
    }
  } else {
    uint64_t zslba = round_down(lba, g_zone_size);
    int rc = spdk_nvme_zns_zone_append(
        g_ns, g_qpair,
        g_data, zslba, g_req_size, 
        complete, nullptr, 0);
    if (rc < 0) {
      printf("append error.");
    }
  }
}

int main(int argc, char *argv[])
{
  int opt;
  while ((opt = getopt(argc, argv, "m:c:s:h:")) != -1) {  // for each option...
    switch (opt) {
      case 'm':
        g_mode = atoi(optarg);
        break;
      case 'c':
        g_num_concurrent_writes = atoi(optarg);
        break;
      case 's':
        g_req_size = atoi(optarg);
        break;
      case 'h':
      default:
        printf(
            "-m: write primitive, 0 - use zone write, 1 - use zone append\n"
            "-c: number of concurrent write requests\n"
            "-s: request size\n");
        return 0;
    }
  }

  int ret = 0;
  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  if (spdk_env_init(&opts) < 0) {
    fprintf(stderr, "Unable to initialize SPDK env.\n");
    exit(-1);
  }

  ret = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL); 
  if (ret < 0) {
    fprintf(stderr, "Unable to probe devices\n");
    exit(-1);
  }

  bool done = false;
  auto resetComplete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  if (spdk_nvme_zns_reset_zone(g_ns, g_qpair, 0, true, resetComplete, &done) < 0) {
    printf("Reset zone failed.\n");
  }

  printf("Mode: %d, Concurrent writes: %d, Request size: %d\n", g_mode, g_num_concurrent_writes, g_req_size);
  while (!done) {
    spdk_nvme_qpair_process_completions(g_qpair, 0);
  }

  g_data = (uint8_t*)spdk_zmalloc(4096 * g_req_size, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  for (uint32_t i = 0; i < 4096 * g_req_size; ++i) {
    g_data[i] = rand() % 256;
  }

  gettimeofday(&g_time_s, NULL);

  // initialize
  for (uint32_t i = 0; i < g_num_concurrent_writes; ++i) {
     write_block(i);
  }
  g_lba = g_num_concurrent_writes * g_req_size;

  while (g_num_completed < dataset_size) { // 64GiB / 4KiB
    // report the performance and exit the program
    spdk_nvme_qpair_process_completions(g_qpair, 0);
  }
  
  gettimeofday(&g_time_e, NULL);
  double elapsed = g_time_e.tv_sec - g_time_s.tv_sec 
    + g_time_e.tv_usec / 1000000. - g_time_s.tv_usec / 1000000.;
  printf("Throughput: %.6fMiB/s\n", dataset_size * 4ull / 1024 / elapsed);
}
