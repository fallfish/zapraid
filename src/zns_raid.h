#ifndef __ZNS_RAID_H__
#define __ZNS_RAID_H__

typedef void (*zns_raid_request_complete)(void *cb_arg);

void *zns_raid_create(void);
void zns_raid_destroy(void *ctrl);

void zns_raid_write(void *ctrl, uint64_t offset, uint32_t size, void *data,
    zns_raid_request_complete cb_fn, void *args);
void zns_raid_read(void *ctrl, uint64_t offset, uint32_t size, void *data,
    zns_raid_request_complete cb_fn, void *args);

void zns_raid_set_system_mode(int mode);

#endif
