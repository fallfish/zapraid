// Copyright (C) Simon A. F. Lund <simon.lund@samsung.com>
// SPDX-License-Identifier: Apache-2.0
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif
#include <xnvme_be.h>
#include <xnvme_be_nosys.h>
#ifdef XNVME_BE_POSIX_ENABLED
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libxnvme_spec_fs.h>
#include <xnvme_be_posix.h>
#include <xnvme_dev.h>

static int
_idfy_ctrlr_iocs_fs(struct xnvme_dev *XNVME_UNUSED(dev), void *dbuf)
{
	struct xnvme_spec_fs_idfy_ctrlr *ctrlr = dbuf;

	ctrlr->caps.direct = 1;

	ctrlr->iosizes.min = 1;
	ctrlr->iosizes.max = 1024 * 1024 * 128;
	ctrlr->iosizes.opt = 1024 * 64;

	ctrlr->limits.file_data_size = 1;

	ctrlr->ac = 0xAC;
	ctrlr->dc = 0xDC;

	return 0;
}

static int
_idfy_ns_iocs_fs(struct xnvme_dev *dev, void *dbuf)
{
	struct xnvme_be_posix_state *state = (void *)dev->be.state;
	struct xnvme_spec_fs_idfy_ns *ns = dbuf;
	struct stat stat = {0};
	int err;

	err = fstat(state->fd, &stat);
	if (err) {
		XNVME_DEBUG("FAILED: fstat, err: %d", err);
		return -ENOSYS;
	}

	ns->nsze = stat.st_size;
	ns->ncap = stat.st_size;
	ns->nuse = stat.st_size;

	ns->ac = 0xAC;
	ns->dc = 0xDC;

	return 0;
}

static int
_idfy_ctrlr(struct xnvme_dev *XNVME_UNUSED(dev), void *dbuf)
{
	struct xnvme_spec_idfy_ctrlr *ctrlr = dbuf;

	ctrlr->mdts = XNVME_ILOG2(1024 * 1024);

	return 0;
}

static int
_idfy_ns(struct xnvme_dev *dev, void *dbuf)
{
	struct xnvme_be_posix_state *state = (void *)dev->be.state;
	struct xnvme_spec_idfy_ns *ns = dbuf;
	struct stat stat = {0};
	int err;

	err = fstat(state->fd, &stat);
	if (err) {
		XNVME_DEBUG("FAILED: fstat, err: %d", err);
		return -ENOSYS;
	}

	ns->nsze = stat.st_size;
	ns->ncap = stat.st_size;
	ns->nuse = stat.st_size;

	ns->nlbaf = 0;        ///< This means that there is only one
	ns->flbas.format = 0; ///< using the first one

	ns->lbaf[0].ms = 0;
	ns->lbaf[0].ds = XNVME_ILOG2(512);
	ns->lbaf[0].rp = 0;

	return 0;
}

static int
_idfy(struct xnvme_cmd_ctx *ctx, void *dbuf)
{
	switch (ctx->cmd.idfy.cns) {
	case XNVME_SPEC_IDFY_NS:
		return _idfy_ns(ctx->dev, dbuf);

	case XNVME_SPEC_IDFY_CTRLR:
		return _idfy_ctrlr(ctx->dev, dbuf);

	case XNVME_SPEC_IDFY_NS_IOCS:
		switch (ctx->cmd.idfy.csi) {
		case XNVME_SPEC_CSI_FS:
			return _idfy_ns_iocs_fs(ctx->dev, dbuf);

		default:
			break;
		}
		break;

	case XNVME_SPEC_IDFY_CTRLR_IOCS:
		switch (ctx->cmd.idfy.csi) {
		case XNVME_SPEC_CSI_FS:
			return _idfy_ctrlr_iocs_fs(ctx->dev, dbuf);

		default:
			break;
		}
		break;

	default:
		break;
	}

	///< TODO: set some appropriate status-code for other idfy-cmds
	ctx->cpl.status.sc = 0x3;
	ctx->cpl.status.sct = 0x3;
	return 1;
}

static int
_gfeat(struct xnvme_cmd_ctx *ctx, void *XNVME_UNUSED(dbuf))
{
	struct xnvme_spec_feat feat = {0};

	switch (ctx->cmd.gfeat.cdw10.fid) {
	case XNVME_SPEC_FEAT_NQUEUES:
		feat.nqueues.nsqa = 63;
		feat.nqueues.ncqa = 63;
		ctx->cpl.cdw0 = feat.val;
		break;

	default:
		XNVME_DEBUG("FAILED: unsupported fid: %d", ctx->cmd.gfeat.cdw10.fid);
		return -ENOSYS;
	}

	return 0;
}

int
_xnvme_be_posix_admin_cmd_admin(struct xnvme_cmd_ctx *ctx, void *dbuf,
				size_t XNVME_UNUSED(dbuf_nbytes), void *XNVME_UNUSED(mbuf),
				size_t XNVME_UNUSED(mbuf_nbytes))
{
	///< NOTE: opcode-dispatch (admin)
	switch (ctx->cmd.common.opcode) {
	case XNVME_SPEC_ADM_OPC_IDFY:
		return _idfy(ctx, dbuf);

	case XNVME_SPEC_ADM_OPC_GFEAT:
		return _gfeat(ctx, dbuf);

	case XNVME_SPEC_ADM_OPC_LOG:
		XNVME_DEBUG("FAILED: not implemented yet.");
		return -ENOSYS;

	default:
		XNVME_DEBUG("FAILED: ENOSYS opcode: %d", ctx->cmd.common.opcode);
		return -ENOSYS;
	}
}
#endif

struct xnvme_be_admin g_xnvme_be_posix_admin_shim = {
	.id = "file_as_ns",
#ifdef XNVME_BE_POSIX_ENABLED
	.cmd_admin = _xnvme_be_posix_admin_cmd_admin,
#else
	.cmd_admin = xnvme_be_nosys_sync_cmd_admin,
#endif
};
