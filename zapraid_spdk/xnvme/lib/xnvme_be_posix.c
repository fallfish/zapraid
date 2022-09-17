// Copyright (C) Simon A. F. Lund <simon.lund@samsung.com>
// SPDX-License-Identifier: Apache-2.0
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif
#include <xnvme_be.h>
#include <xnvme_be_nosys.h>
#ifdef XNVME_BE_POSIX_ENABLED
#include <xnvme_be_posix.h>

static struct xnvme_be_mixin g_xnvme_be_mixin_posix[] = {
	{
		.mtype = XNVME_BE_MEM,
		.name = "posix",
		.descr = "Use libc malloc()/free() with sysconf for alignment",
		.mem = &g_xnvme_be_posix_mem,
		.check_support = xnvme_be_supported,
	},

	{
		.mtype = XNVME_BE_ASYNC,
		.name = "emu",
		.descr = "Use emulated asynchronous I/O",
		.async = &g_xnvme_be_posix_async_emu,
		.check_support = xnvme_be_supported,
	},
	{
		.mtype = XNVME_BE_ASYNC,
		.name = "posix",
		.descr = "Use POSIX aio for Asynchronous I/O",
		.async = &g_xnvme_be_posix_async_aio,
		.check_support = xnvme_be_supported,
	},
	{
		.mtype = XNVME_BE_ASYNC,
		.name = "thrpool",
		.descr = "Use thread pool for Asynchronous I/O",
		.async = &g_xnvme_be_posix_async_thrpool,
		.check_support = xnvme_be_supported,
	},
	{
		.mtype = XNVME_BE_ASYNC,
		.name = "nil",
		.descr = "Use nil-io; For introspective perf. evaluation",
		.async = &g_xnvme_be_posix_async_nil,
		.check_support = xnvme_be_supported,
	},

	{
		.mtype = XNVME_BE_SYNC,
		.name = "psync",
		.descr = "Use pread()/write() for synchronous I/O",
		.sync = &g_xnvme_be_posix_sync_psync,
		.check_support = xnvme_be_supported,
	},

	{
		.mtype = XNVME_BE_ADMIN,
		.name = "file_as_ns",
		.descr = "Use file-stat as to construct NVMe idfy responses",
		.admin = &g_xnvme_be_posix_admin_shim,
		.check_support = xnvme_be_supported,
	},

	{
		.mtype = XNVME_BE_DEV,
		.name = "posix",
		.descr = "Use POSIX file/dev handles and no device enumeration",
		.dev = &g_xnvme_be_posix_dev,
		.check_support = xnvme_be_supported,
	},
};
#endif

struct xnvme_be xnvme_be_posix = {
	.mem = XNVME_BE_NOSYS_MEM,
	.admin = XNVME_BE_NOSYS_ADMIN,
	.sync = XNVME_BE_NOSYS_SYNC,
	.async = XNVME_BE_NOSYS_QUEUE,
	.dev = XNVME_BE_NOSYS_DEV,
	.attr =
		{
			.name = "posix",
#ifdef XNVME_BE_POSIX_ENABLED
			.enabled = 1,
#endif
		},
#ifdef XNVME_BE_POSIX_ENABLED
	.nobjs = sizeof g_xnvme_be_mixin_posix / sizeof *g_xnvme_be_mixin_posix,
	.objs = g_xnvme_be_mixin_posix,
#endif
};
