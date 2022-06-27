#include "raid_controller.h"

extern "C" void *zns_raid_create(void)
{
  RAIDController *ctrl = new RAIDController();
  ctrl->Init(false);
  
  return ctrl;
}

extern "C" void zns_raid_destroy(void *ctrl)
{
  RAIDController *ctrl1 = (RAIDController*)ctrl;
  delete ctrl1;
}

extern "C" void zns_raid_write(void *ctrl, uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *args)
{
  RAIDController *ctrl1 = (RAIDController*)ctrl;
  ctrl1->Write(offset, size, data, cb_fn, args);
}

extern "C" void zns_raid_read(void *ctrl, uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *args)
{
  RAIDController *ctrl1 = (RAIDController*)ctrl;
  ctrl1->Read(offset, size, data, cb_fn, args);
}

extern "C" void zns_raid_set_system_mode(int mode)
{
  Configuration::SetSystemMode(SystemMode(mode));
}

extern "C" void zns_raid_set_raid_mode(int n_data_blocks, int n_parity_blocks)
{
  Configuration::SetStripeDataSize(Configuration::GetBlockSize() * n_data_blocks);
  Configuration::SetStripeParitySize(Configuration::GetBlockSize() * n_parity_blocks);
}

extern "C" void zns_raid_set_gc(bool enable)
{
  Configuration::SetEnableGc(enable);
}

extern "C" void zns_raid_set_sync_group_size(uint32_t group_size) {
  Configuration::SetSyncGroupSize(group_size);
}

extern "C" void zns_raid_set_degraded_read(bool enable) {
  Configuration::SetEnableDegradedRead(enable);
}
