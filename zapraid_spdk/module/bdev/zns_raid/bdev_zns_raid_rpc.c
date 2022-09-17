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

#include "spdk/rpc.h"
#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/bdev_module.h"
#include "spdk/log.h"

#include "bdev_zns_raid.h"

struct rpc_construct_zns_raid {
	char *name;
	char *uuid;
	uint64_t num_blocks;
	uint32_t block_size;
	uint32_t system_mode;
	uint32_t num_data_blocks;
	uint32_t num_parity_blocks;
	uint32_t raid_level;
	uint32_t enable_gc;
	uint32_t sync_group_size;
	uint32_t num_open_segments;
	uint32_t inject_degraded_read;
        uint32_t enable_event_framework;
};

static void
free_rpc_construct_zns_raid(struct rpc_construct_zns_raid *req)
{
        free(req->name);
	free(req->uuid);
}

static const struct spdk_json_object_decoder rpc_construct_zns_raid_decoders[] = {
	{"name", offsetof(struct rpc_construct_zns_raid, name), spdk_json_decode_string},
	{"uuid", offsetof(struct rpc_construct_zns_raid, uuid), spdk_json_decode_string, true},
	{"num_blocks", offsetof(struct rpc_construct_zns_raid, num_blocks), spdk_json_decode_uint64},
	{"block_size", offsetof(struct rpc_construct_zns_raid, block_size), spdk_json_decode_uint32},
	{"system_mode", offsetof(struct rpc_construct_zns_raid, system_mode), spdk_json_decode_uint32},
	{"num_data_blocks", offsetof(struct rpc_construct_zns_raid, num_data_blocks), spdk_json_decode_uint32},
	{"num_parity_blocks", offsetof(struct rpc_construct_zns_raid, num_parity_blocks), spdk_json_decode_uint32},
	{"raid_level", offsetof(struct rpc_construct_zns_raid, raid_level), spdk_json_decode_uint32},
	{"enable_gc", offsetof(struct rpc_construct_zns_raid, enable_gc), spdk_json_decode_uint32},
	{"sync_group_size", offsetof(struct rpc_construct_zns_raid, sync_group_size), spdk_json_decode_uint32},
	{"num_open_segments", offsetof(struct rpc_construct_zns_raid, num_open_segments), spdk_json_decode_uint32},
	{"inject_degraded_read", offsetof(struct rpc_construct_zns_raid, inject_degraded_read), spdk_json_decode_uint32},
	{"enable_event_framework", offsetof(struct rpc_construct_zns_raid, enable_event_framework), spdk_json_decode_uint32},
};

static void
rpc_bdev_zns_raid_create(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_construct_zns_raid req = {};
	struct spdk_json_write_ctx *w;
	struct spdk_uuid *uuid = NULL;
	struct spdk_uuid decoded_uuid;
	struct spdk_bdev *bdev;
	struct spdk_zns_raid_bdev_opts opts = {};
	uint32_t data_block_size;
	int rc = 0;

	if (spdk_json_decode_object(params, rpc_construct_zns_raid_decoders,
				    SPDK_COUNTOF(rpc_construct_zns_raid_decoders),
				    &req)) {
		SPDK_DEBUGLOG(bdev_zns_raid, "spdk_json_decode_object failed\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	data_block_size = req.block_size;
	if (data_block_size % 512 != 0) {
		spdk_jsonrpc_send_error_response_fmt(request, -EINVAL,
						     "Data block size %u is not a multiple of 512", req.block_size);
		goto cleanup;
	}

	if (req.num_blocks == 0) {
		spdk_jsonrpc_send_error_response(request, -EINVAL,
						 "Disk num_blocks must be greater than -1");
		goto cleanup;
	}

	if (req.uuid) {
		if (spdk_uuid_parse(&decoded_uuid, req.uuid)) {
			spdk_jsonrpc_send_error_response(request, -EINVAL,
							 "Failed to parse bdev UUID");
			goto cleanup;
		}
		uuid = &decoded_uuid;
	}

	opts.name = req.name;
	opts.uuid = uuid;
	opts.num_blocks = req.num_blocks;
	opts.block_size = req.block_size;
        opts.system_mode = req.system_mode;
        opts.num_data_blocks = req.num_data_blocks;
        opts.num_parity_blocks = req.num_parity_blocks;
        opts.raid_level = req.raid_level;
        opts.enable_gc = req.enable_gc;
        opts.sync_group_size = req.sync_group_size;
        opts.num_open_segments = req.num_open_segments;
        opts.inject_degraded_read = req.inject_degraded_read;
        opts.enable_event_framework = req.enable_event_framework;
	rc = bdev_zns_raid_create(&bdev, &opts);
	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, bdev->name);
	spdk_jsonrpc_end_result(request, w);
	free_rpc_construct_zns_raid(&req);
	return;

cleanup:
	free_rpc_construct_zns_raid(&req);
}
SPDK_RPC_REGISTER("bdev_zns_raid_create", rpc_bdev_zns_raid_create, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_zns_raid_create, construct_zns_raid_bdev)

struct rpc_delete_zns_raid {
	char *name;
};

static void
free_rpc_delete_zns_raid(struct rpc_delete_zns_raid *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder rpc_delete_zns_raid_decoders[] = {
	{"name", offsetof(struct rpc_delete_zns_raid, name), spdk_json_decode_string},
};

static void
rpc_bdev_zns_raid_delete_cb(void *cb_arg, int bdeverrno)
{
	struct spdk_jsonrpc_request *request = cb_arg;

	spdk_jsonrpc_send_bool_response(request, bdeverrno == 0);
}

static void
rpc_bdev_zns_raid_delete(struct spdk_jsonrpc_request *request,
		     const struct spdk_json_val *params)
{
	struct rpc_delete_zns_raid req = {NULL};
	struct spdk_bdev *bdev;

	if (spdk_json_decode_object(params, rpc_delete_zns_raid_decoders,
				    SPDK_COUNTOF(rpc_delete_zns_raid_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	bdev_zns_raid_delete(bdev, rpc_bdev_zns_raid_delete_cb, request);

	free_rpc_delete_zns_raid(&req);

	return;

cleanup:
	free_rpc_delete_zns_raid(&req);
}
SPDK_RPC_REGISTER("bdev_zns_raid_delete", rpc_bdev_zns_raid_delete, SPDK_RPC_RUNTIME)
SPDK_RPC_REGISTER_ALIAS_DEPRECATED(bdev_zns_raid_delete, delete_zns_raid_bdev)

struct rpc_bdev_zns_raid_resize {
	char *name;
	uint64_t new_size;
};

static const struct spdk_json_object_decoder rpc_bdev_zns_raid_resize_decoders[] = {
	{"name", offsetof(struct rpc_bdev_zns_raid_resize, name), spdk_json_decode_string},
	{"new_size", offsetof(struct rpc_bdev_zns_raid_resize, new_size), spdk_json_decode_uint64}
};

static void
free_rpc_bdev_zns_raid_resize(struct rpc_bdev_zns_raid_resize *req)
{
	free(req->name);
}

static void
spdk_rpc_bdev_zns_raid_resize(struct spdk_jsonrpc_request *request,
			  const struct spdk_json_val *params)
{
	struct rpc_bdev_zns_raid_resize req = {};
	struct spdk_bdev *bdev;
	int rc;

	if (spdk_json_decode_object(params, rpc_bdev_zns_raid_resize_decoders,
				    SPDK_COUNTOF(rpc_bdev_zns_raid_resize_decoders),
				    &req)) {
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
						 "spdk_json_decode_object failed");
		goto cleanup;
	}

	bdev = spdk_bdev_get_by_name(req.name);
	if (bdev == NULL) {
		spdk_jsonrpc_send_error_response(request, -ENODEV, spdk_strerror(ENODEV));
		goto cleanup;
	}

	rc = bdev_zns_raid_resize(bdev, req.new_size);
	if (rc) {
		spdk_jsonrpc_send_error_response(request, rc, spdk_strerror(-rc));
		goto cleanup;
	}

	spdk_jsonrpc_send_bool_response(request, true);
cleanup:
	free_rpc_bdev_zns_raid_resize(&req);
}
SPDK_RPC_REGISTER("bdev_zns_raid_resize", spdk_rpc_bdev_zns_raid_resize, SPDK_RPC_RUNTIME)
