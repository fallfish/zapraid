/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/json.h"
#include "spdk/string.h"
#include "spdk/likely.h"

#include "spdk/bdev_module.h"
#include "spdk/log.h"

#include "bdev_zns_raid.h"
// #include "../zns_raid.h"
#include "zns_raid.h"

struct zns_raid_bdev {
  struct spdk_bdev	bdev;
  void *raid_controller;
  TAILQ_ENTRY(zns_raid_bdev)	tailq;
};

struct zns_raid_io_channel {
  struct spdk_poller		*poller;
  TAILQ_HEAD(, spdk_bdev_io)	io;
};

struct zns_raid_task {
  int                           num_outstanding;
  enum spdk_bdev_io_status      status;
  TAILQ_ENTRY(zns_raid_task)	tailq;
};

static int bdev_zns_raid_get_ctx_size()
{
  return sizeof(struct zns_raid_task);
}


static TAILQ_HEAD(, zns_raid_bdev) g_zns_raid_bdev_head = TAILQ_HEAD_INITIALIZER(g_zns_raid_bdev_head);

static int bdev_zns_raid_initialize(void);
static void bdev_zns_raid_finish(void);

static struct spdk_bdev_module zns_raid_if = {
  .name = "zns_raid",
  .module_init = bdev_zns_raid_initialize,
  .module_fini = bdev_zns_raid_finish,
  .async_fini = true,
  .get_ctx_size = bdev_zns_raid_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(zns_raid, &zns_raid_if)

static int
bdev_zns_raid_destruct(void *ctx)
{
  struct zns_raid_bdev *bdev = ctx;
  zns_raid_destroy(bdev->raid_controller);

  TAILQ_REMOVE(&g_zns_raid_bdev_head, bdev, tailq);
  free(bdev->bdev.name);
  free(bdev);

  return 0;
}

static void
bdev_zns_raid_io_complete_local(void *args)
{
  struct zns_raid_task *task = (struct zns_raid_task *)args;
//  printf("task: %p, Outstanding entries: %u\n", task, task->num_outstanding);
  if (--task->num_outstanding == 0) {
    struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(task);
    spdk_bdev_io_complete(bdev_io, task->status);
  }
//  spdk_bdev_io_complete((struct spdk_bdev_io*)args, SPDK_BDEV_IO_STATUS_SUCCESS);
//  TAILQ_INSERT_TAIL(&ch->io, bdev_io, module_link);
}

static void
zns_raid_task_done(void *ref)
{
  struct zns_raid_task *task = (struct zns_raid_task *)ref;
  struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(task);

  spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io), bdev_zns_raid_io_complete_local, task);
}

// static void
// bdev_zns_raid_io_complete_cb(void *cb_args)
// {
//   struct spdk_bdev_io *bdev_io = cb_args;
// //  struct spdk_io_channel *_ch = spdk_bdev_io_get_io_channel(bdev_io);
// //  struct zns_raid_io_channel *ch = spdk_io_channel_get_ctx(_ch);
// 
//   spdk_thread_send_msg(spdk_bdev_io_get_thread(bdev_io), bdev_zns_raid_io_complete_local, bdev_io);
// }

static void
bdev_zns_raid_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
    struct zns_raid_bdev *zns_raid_disk;
    struct zns_raid_task *zns_raid_task;
    uint64_t offset = (uint64_t)bdev_io->u.bdev.offset_blocks * bdev_io->bdev->blocklen;

    if (!success) {
      spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
      return;
    }

//    if (bdev_io->u.bdev.iovcnt > 1) {
//      spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
//      SPDK_ERRLOG("Not supported more than 1 io\n");
//      return;
//    }

    zns_raid_disk = (struct zns_raid_bdev*)bdev_io->bdev->ctxt;
    zns_raid_task = (struct zns_raid_task*)bdev_io->driver_ctx;
    zns_raid_task->num_outstanding = bdev_io->u.bdev.iovcnt;
    zns_raid_task->status = SPDK_BDEV_IO_STATUS_SUCCESS;

    switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
      for (int i = 0; i < bdev_io->u.bdev.iovcnt; ++i) {
        zns_raid_read(zns_raid_disk->raid_controller,
            offset,
            bdev_io->u.bdev.iovs[i].iov_len,
            bdev_io->u.bdev.iovs[i].iov_base,
            zns_raid_task_done, zns_raid_task);
        offset += bdev_io->u.bdev.iovs[i].iov_len;
      }
      break;
    case SPDK_BDEV_IO_TYPE_WRITE:
      for (int i = 0; i < bdev_io->u.bdev.iovcnt; ++i) {
        zns_raid_write(zns_raid_disk->raid_controller,
            offset,
            bdev_io->u.bdev.iovs[i].iov_len,
            bdev_io->u.bdev.iovs[i].iov_base,
            zns_raid_task_done, zns_raid_task);
        offset += bdev_io->u.bdev.iovs[i].iov_len;
      }
      break;
    }
}

static void
bdev_zns_raid_submit_request(struct spdk_io_channel *_ch, struct spdk_bdev_io *bdev_io)
{
  struct zns_raid_bdev *zns_raid_disk;
  struct zns_raid_task *zns_raid_task;
  uint64_t offset = (uint64_t)bdev_io->u.bdev.offset_blocks * bdev_io->bdev->blocklen;

  zns_raid_disk = (struct zns_raid_bdev*)bdev_io->bdev->ctxt;
  zns_raid_task = (struct zns_raid_task*)bdev_io->driver_ctx;
  zns_raid_task->num_outstanding = bdev_io->u.bdev.iovcnt;
  zns_raid_task->status = SPDK_BDEV_IO_STATUS_SUCCESS;

//  printf("Submit: num_iovs: %u, type: %d\n", bdev_io->u.bdev.iovcnt, bdev_io->type == SPDK_BDEV_IO_TYPE_READ);

  switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
      spdk_bdev_io_get_buf(bdev_io, bdev_zns_raid_get_buf_cb,
                           bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
      break;
    case SPDK_BDEV_IO_TYPE_WRITE:
      spdk_bdev_io_get_buf(bdev_io, bdev_zns_raid_get_buf_cb,
                           bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
      break;
    case SPDK_BDEV_IO_TYPE_FLUSH:
      spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
      break;
    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
    case SPDK_BDEV_IO_TYPE_RESET:
    case SPDK_BDEV_IO_TYPE_ABORT:
    case SPDK_BDEV_IO_TYPE_UNMAP:
    default:
      spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
      break;
  }
}

static bool
bdev_zns_raid_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
  switch (io_type) {
    case SPDK_BDEV_IO_TYPE_READ:
    case SPDK_BDEV_IO_TYPE_WRITE:
      return true;
    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
    case SPDK_BDEV_IO_TYPE_RESET:
    case SPDK_BDEV_IO_TYPE_ABORT:
    case SPDK_BDEV_IO_TYPE_FLUSH:
    case SPDK_BDEV_IO_TYPE_UNMAP:
    default:
      return false;
  }
}

static struct spdk_io_channel *
bdev_zns_raid_get_io_channel(void *ctx)
{
  return spdk_get_io_channel(&g_zns_raid_bdev_head);
}

static void
bdev_zns_raid_write_config_json(struct spdk_bdev *bdev, struct spdk_json_write_ctx *w)
{
  char uuid_str[SPDK_UUID_STRING_LEN];

  spdk_json_write_object_begin(w);

  spdk_json_write_named_string(w, "method", "bdev_zns_raid_create");

  spdk_json_write_named_object_begin(w, "params");
  spdk_json_write_named_string(w, "name", bdev->name);
  spdk_json_write_named_uint64(w, "num_blocks", bdev->blockcnt);
  spdk_json_write_named_uint32(w, "block_size", bdev->blocklen);
  spdk_uuid_fmt_lower(uuid_str, sizeof(uuid_str), &bdev->uuid);
  spdk_json_write_named_string(w, "uuid", uuid_str);
  spdk_json_write_object_end(w);

  spdk_json_write_object_end(w);
}

static const struct spdk_bdev_fn_table zns_raid_fn_table = {
  .destruct		= bdev_zns_raid_destruct,
  .submit_request		= bdev_zns_raid_submit_request,
  .io_type_supported	= bdev_zns_raid_io_type_supported,
  .get_io_channel		= bdev_zns_raid_get_io_channel,
  .write_config_json	= bdev_zns_raid_write_config_json,
};

int
bdev_zns_raid_create(struct spdk_bdev **bdev, const struct spdk_zns_raid_bdev_opts *opts)
{
  struct zns_raid_bdev *zns_raid_disk;
  int rc;

  if (!opts) {
    SPDK_ERRLOG("No options provided for zns raid bdev.\n");
    return -EINVAL;
  }

  if (opts->block_size % 512 != 0) {
    SPDK_ERRLOG("Data block size %u is not a multiple of 512.\n", opts->block_size);
    return -EINVAL;
  }

  if (opts->num_blocks == 0) {
    SPDK_ERRLOG("Disk must be more than 0 blocks\n");
    return -EINVAL;
  }

  zns_raid_disk = calloc(1, sizeof(*zns_raid_disk));
  if (!zns_raid_disk) {
    SPDK_ERRLOG("could not allocate zns_raid_bdev\n");
    return -ENOMEM;
  }

  zns_raid_disk->bdev.name = strdup(opts->name);
  if (!zns_raid_disk->bdev.name) {
    free(zns_raid_disk);
    return -ENOMEM;
  }
  zns_raid_disk->bdev.product_name = "zns raid disk";

  zns_raid_disk->bdev.write_cache = 0;
  zns_raid_disk->bdev.blocklen = opts->block_size;
  zns_raid_disk->bdev.blockcnt = opts->num_blocks;

  if (opts->uuid) {
    zns_raid_disk->bdev.uuid = *opts->uuid;
  } else {
    spdk_uuid_generate(&zns_raid_disk->bdev.uuid);
  }

  zns_raid_set_system_mode(opts->system_mode);
  zns_raid_set_raid_parameters(opts->num_data_blocks, opts->num_parity_blocks, opts->raid_level);
  zns_raid_set_gc(opts->enable_gc);
  zns_raid_set_sync_group_size(opts->sync_group_size);
  zns_raid_set_num_open_segments(opts->num_open_segments);
  zns_raid_set_degraded_read(opts->inject_degraded_read);
  zns_raid_set_enable_event_framework(opts->enable_event_framework);
  zns_raid_set_storage_space_in_bytes((uint64_t)opts->block_size * opts->num_blocks);
  zns_raid_disk->raid_controller = zns_raid_create();
  zns_raid_disk->bdev.ctxt = zns_raid_disk;
  zns_raid_disk->bdev.fn_table = &zns_raid_fn_table;
  zns_raid_disk->bdev.module = &zns_raid_if;

  rc = spdk_bdev_register(&zns_raid_disk->bdev);
  if (rc) {
    free(zns_raid_disk->bdev.name);
    free(zns_raid_disk);
    return rc;
  }

  *bdev = &(zns_raid_disk->bdev);


  TAILQ_INSERT_TAIL(&g_zns_raid_bdev_head, zns_raid_disk, tailq);

  return rc;
}

void
bdev_zns_raid_delete(struct spdk_bdev *bdev, spdk_delete_zns_raid_complete cb_fn, void *cb_arg)
{
  if (!bdev || bdev->module != &zns_raid_if) {
    cb_fn(cb_arg, -ENODEV);
    return;
  }

  // ((struct *zns_raid_bdev)bdev->ctxt)->raid_controller = NULL;
  spdk_bdev_unregister(bdev, cb_fn, cb_arg);
}

static int
zns_raid_bdev_create_cb(void *io_device, void *ctx_buf)
{
  struct zns_raid_io_channel *ch = ctx_buf;


  printf("callback of creating zns raid, current thread: %s%d\n", spdk_thread_get_name(spdk_get_thread()), spdk_thread_get_id(spdk_get_thread()));
  TAILQ_INIT(&ch->io);
//  ch->poller = SPDK_POLLER_REGISTER(zns_raid_io_poll, ch, 0);

  return 0;
}

static void
zns_raid_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
  struct zns_raid_io_channel *ch = ctx_buf;

//  spdk_poller_unregister(&ch->poller);
}

static int
bdev_zns_raid_initialize(void)
{
  /*
   * We need to pick some unique address as our "io device" - so just use the
   *  address of the global tailq.
   */
  spdk_io_device_register(&g_zns_raid_bdev_head, zns_raid_bdev_create_cb, zns_raid_bdev_destroy_cb,
      sizeof(struct zns_raid_io_channel), "zns_raid_bdev");

  return 0;
}

  int
bdev_zns_raid_resize(struct spdk_bdev *bdev, const uint64_t new_size_in_mb)
{
  uint64_t current_size_in_mb;
  uint64_t new_size_in_byte;
  int rc;

  if (bdev->module != &zns_raid_if) {
    return -EINVAL;
  }

  current_size_in_mb = bdev->blocklen * bdev->blockcnt / (1024 * 1024);
  if (new_size_in_mb < current_size_in_mb) {
    SPDK_ERRLOG("The new bdev size must not be smaller than current bdev size.\n");
    return -EINVAL;
  }

  new_size_in_byte = new_size_in_mb * 1024 * 1024;

  rc = spdk_bdev_notify_blockcnt_change(bdev, new_size_in_byte / bdev->blocklen);
  if (rc != 0) {
    SPDK_ERRLOG("failed to notify block cnt change.\n");
    return rc;
  }

  return 0;
}

static void
_bdev_zns_raid_finish_cb(void *arg)
{
  spdk_bdev_module_fini_done();
}

static void
bdev_zns_raid_finish(void)
{
  // ((struct *zns_raid_bdev)bdev->ctxt)->raid_controller = NULL;
  spdk_io_device_unregister(&g_zns_raid_bdev_head, _bdev_zns_raid_finish_cb);
}

SPDK_LOG_REGISTER_COMPONENT(bdev_zns_raid)
