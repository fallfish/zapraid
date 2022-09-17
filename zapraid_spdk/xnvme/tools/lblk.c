#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <libxnvme.h>
#include <libxnvme_ident.h>
#include <libxnvme_dev.h>
#include <libxnvme_adm.h>
#include <libxnvme_nvm.h>
#include <libxnvmec.h>

int
enumerate_cb(struct xnvme_dev *dev, void *cb_args)
{
	uint32_t *ns_count_ref = cb_args;
	const struct xnvme_ident *ident;

	ident = xnvme_dev_get_ident(dev);
	if (ident->csi != XNVME_SPEC_CSI_NVM) {
		return XNVME_ENUMERATE_DEV_CLOSE;
	}

	if (*ns_count_ref == 0) {
		fprintf(stdout, "\n");
	}

	fprintf(stdout, "  - {");
	xnvme_ident_yaml(stdout, ident, 0, ", ", 0);
	fprintf(stdout, "}\n");

	*ns_count_ref = *ns_count_ref + 1;

	return XNVME_ENUMERATE_DEV_CLOSE;
}

static int
sub_enumerate(struct xnvmec *cli)
{
	struct xnvme_opts opts = {0};
	uint32_t ns_count = 0;
	int err = 0;

	err = xnvmec_cli_to_opts(cli, &opts);
	if (err) {
		xnvmec_perr("xnvmec_cli_to_opts()", err);
		return err;
	}

	fprintf(stdout, "xnvme_enumeration:");

	err = xnvme_enumerate(cli->args.sys_uri, &opts, *enumerate_cb, &ns_count);
	if (err) {
		xnvmec_perr("xnvme_enumerate()", err);
		return err;
	}

	if (ns_count == 0) {
		fprintf(stdout, "~\n");
	}

	return 0;
}

static int
sub_info(struct xnvmec *cli)
{
	struct xnvme_dev *dev = cli->args.dev;

	xnvme_dev_pr(dev, XNVME_PR_DEF);

	return 0;
}

static inline int
sub_idfy(struct xnvmec *cli)
{
	struct xnvme_dev *dev = cli->args.dev;
	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev);
	struct xnvme_spec_idfy *idfy = NULL;
	uint32_t nsid = xnvme_dev_get_nsid(cli->args.dev);
	int err;

	xnvmec_pinf("xnvme_adm_idfy: {nsid: 0x%x}", nsid);

	idfy = xnvme_buf_alloc(dev, sizeof(*idfy));
	if (!idfy) {
		err = -errno;
		xnvmec_perr("xnvme_buf_alloc()", err);
		goto exit;
	}

	err = xnvme_adm_idfy_ns(&ctx, nsid, idfy);
	if (err || xnvme_cmd_ctx_cpl_status(&ctx)) {
		xnvmec_perr("xnvme_adm_idfy()", err);
		xnvme_cmd_ctx_pr(&ctx, XNVME_PR_DEF);
		err = err ? err : -EIO;
		goto exit;
	}

	xnvme_spec_idfy_ns_pr(&idfy->ns, XNVME_PR_DEF);

	if (cli->args.data_output) {
		xnvmec_pinf("Dumping to: '%s'", cli->args.data_output);
		err = xnvmec_buf_to_file((char *)idfy, sizeof(*idfy), cli->args.data_output);
		if (err) {
			xnvmec_perr("xnvmec_buf_to_file()", err);
		}
	}

exit:
	xnvme_buf_free(dev, idfy);

	return err;
}

static int
sub_read(struct xnvmec *cli)
{
	struct xnvme_dev *dev = cli->args.dev;
	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev);
	const struct xnvme_geo *geo = cli->args.geo;
	const uint64_t slba = cli->args.slba;
	const size_t nlb = cli->args.nlb;
	uint8_t nsid = cli->args.nsid;

	void *dbuf = NULL, *mbuf = NULL;
	size_t dbuf_nbytes, mbuf_nbytes;
	int err;

	if (!cli->given[XNVMEC_OPT_NSID]) {
		nsid = xnvme_dev_get_nsid(dev);
	}

	dbuf_nbytes = (nlb + 1) * geo->lba_nbytes;
	mbuf_nbytes = geo->lba_extended ? 0 : (nlb + 1) * geo->nbytes_oob;

	xnvmec_pinf("Reading nsid: 0x%x, slba: 0x%016lx, nlb: %zu", nsid, slba, nlb);

	xnvmec_pinf("Alloc/clear dbuf, dbuf_nbytes: %zu", dbuf_nbytes);
	dbuf = xnvme_buf_alloc(dev, dbuf_nbytes);
	if (!dbuf) {
		err = -errno;
		xnvmec_perr("xnvme_buf_alloc()", err);
		goto exit;
	}
	memset(dbuf, 0, dbuf_nbytes);

	if (mbuf_nbytes) {
		xnvmec_pinf("Alloc/clear mbuf, mbuf_nbytes: %zu", mbuf_nbytes);
		mbuf = xnvme_buf_alloc(dev, mbuf_nbytes);
		if (!mbuf) {
			err = -errno;
			xnvmec_perr("xnvme_buf_alloc()", err);
			goto exit;
		}
		memset(mbuf, 0, mbuf_nbytes);
	}

	xnvmec_pinf("Sending the command...");
	err = xnvme_nvm_read(&ctx, nsid, slba, nlb, dbuf, mbuf);
	if (err || xnvme_cmd_ctx_cpl_status(&ctx)) {
		xnvmec_perr("xnvme_nvm_read()", err);
		xnvme_cmd_ctx_pr(&ctx, XNVME_PR_DEF);
		err = err ? err : -EIO;
		goto exit;
	}

	if (cli->args.data_output) {
		xnvmec_pinf("dumping to: '%s'", cli->args.data_output);
		err = xnvmec_buf_to_file(dbuf, dbuf_nbytes, cli->args.data_output);
		if (err) {
			xnvmec_perr("xnvmec_buf_to_file()", err);
		}
	}

exit:
	xnvme_buf_free(dev, dbuf);
	xnvme_buf_free(dev, mbuf);

	return err;
}

static int
sub_write(struct xnvmec *cli)
{
	struct xnvme_dev *dev = cli->args.dev;
	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev);
	const struct xnvme_geo *geo = cli->args.geo;
	const uint64_t slba = cli->args.slba;
	const size_t nlb = cli->args.nlb;
	uint32_t nsid = cli->args.nsid;

	void *dbuf = NULL, *mbuf = NULL;
	size_t dbuf_nbytes, mbuf_nbytes;
	int err;

	if (!cli->given[XNVMEC_OPT_NSID]) {
		nsid = xnvme_dev_get_nsid(cli->args.dev);
	}

	dbuf_nbytes = (nlb + 1) * geo->lba_nbytes;
	mbuf_nbytes = geo->lba_extended ? 0 : (nlb + 1) * geo->nbytes_oob;

	xnvmec_pinf("Writing nsid: 0x%x, slba: 0x%016lx, nlb: %zu", nsid, slba, nlb);

	xnvmec_pinf("Alloc/fill dbuf, dbuf_nbytes: %zu", dbuf_nbytes);
	dbuf = xnvme_buf_alloc(dev, dbuf_nbytes);
	if (!dbuf) {
		err = -errno;
		xnvmec_perr("xnvme_buf_alloc()", err);
		goto exit;
	}
	err = xnvmec_buf_fill(dbuf, dbuf_nbytes,
			      cli->args.data_input ? cli->args.data_input : "anum");
	if (err) {
		xnvmec_perr("xnvmec_buf_fill()", err);
		goto exit;
	}

	if (mbuf_nbytes) {
		xnvmec_pinf("Alloc/fill mbuf, mbuf_nbytes: %zu", mbuf_nbytes);
		mbuf = xnvme_buf_alloc(dev, mbuf_nbytes);
		if (!mbuf) {
			err = -errno;
			xnvmec_perr("xnvme_buf_alloc()", err);
			goto exit;
		}
		err = xnvmec_buf_fill(mbuf, mbuf_nbytes, "anum");
		if (err) {
			xnvmec_perr("xnvmec_buf_fill()", err);
			goto exit;
		}
	}

	xnvmec_pinf("Sending the command...");
	err = xnvme_nvm_write(&ctx, nsid, slba, nlb, dbuf, mbuf);
	if (err || xnvme_cmd_ctx_cpl_status(&ctx)) {
		xnvmec_perr("xnvme_nvm_write()", err);
		xnvme_cmd_ctx_pr(&ctx, XNVME_PR_DEF);
		err = err ? err : -EIO;
		goto exit;
	}

exit:
	xnvme_buf_free(dev, dbuf);
	xnvme_buf_free(dev, mbuf);

	return err;
}

static int
sub_write_zeroes(struct xnvmec *cli)
{
	int err = 0;
	struct xnvme_dev *dev = cli->args.dev;
	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev);
	const uint64_t slba = cli->args.slba;
	const size_t nlb = cli->args.nlb;
	uint32_t nsid = xnvme_dev_get_nsid(cli->args.dev);

	err = xnvme_nvm_write_zeroes(&ctx, nsid, slba, nlb);
	if (err) {
		xnvmec_perr("xnvme_nvm_write_zeroes()", err);
		goto exit;
	}

exit:
	return err;
}

static int
sub_write_uncor(struct xnvmec *cli)
{
	int err = 0;
	struct xnvme_dev *dev = cli->args.dev;
	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(dev);
	const uint64_t slba = cli->args.slba;
	const size_t nlb = cli->args.nlb;
	uint32_t nsid = xnvme_dev_get_nsid(cli->args.dev);

	err = xnvme_nvm_write_uncorrectable(&ctx, nsid, slba, nlb);
	if (err) {
		xnvmec_perr("xnvme_nvm_write_uncorrectable()", err);
		goto exit;
	}

exit:
	return err;
}

//
// Command-Line Interface (CLI) definition
//
static struct xnvmec_sub g_subs[] = {
	{
		"enum",
		"Enumerate Logical Block Namespaces on the system",
		"Enumerate Logical Block Namespaces on the system",
		sub_enumerate,
		{
			{XNVMEC_OPT_SYS_URI, XNVMEC_LOPT},
			{XNVMEC_OPT_FLAGS, XNVMEC_LOPT},

			{XNVMEC_OPT_BE, XNVMEC_LOPT},
		},
	},
	{
		"info",
		"Retrieve derived information for the given URI",
		"Retrieve derived information for the given URI",
		sub_info,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
		},
	},
	{
		"idfy",
		"Identify the namespace for the given URI",
		"Identify the namespace for the given URI",
		sub_idfy,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},
			{XNVMEC_OPT_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_DATA_OUTPUT, XNVMEC_LOPT},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
		},
	},
	{
		"read",
		"Read data and optionally metadata",
		"Read data and optionally metadata",
		sub_read,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},
			{XNVMEC_OPT_SLBA, XNVMEC_LREQ},
			{XNVMEC_OPT_NLB, XNVMEC_LREQ},
			{XNVMEC_OPT_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_DATA_OUTPUT, XNVMEC_LOPT},
			{XNVMEC_OPT_META_OUTPUT, XNVMEC_LOPT},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
			{XNVMEC_OPT_SYNC, XNVMEC_LOPT},
		},
	},
	{
		"write",
		"Writes data and optionally metadata",
		"Writes data and optionally metadata",
		sub_write,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},
			{XNVMEC_OPT_SLBA, XNVMEC_LREQ},
			{XNVMEC_OPT_NLB, XNVMEC_LREQ},
			{XNVMEC_OPT_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_DATA_INPUT, XNVMEC_LOPT},
			{XNVMEC_OPT_META_INPUT, XNVMEC_LOPT},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
			{XNVMEC_OPT_SYNC, XNVMEC_LOPT},
		},
	},
	{
		"write-zeros",
		"Set a range of logical blocks to zero",
		"Set a range of logical blocks to zero",
		sub_write_zeroes,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},
			{XNVMEC_OPT_SLBA, XNVMEC_LREQ},
			{XNVMEC_OPT_NLB, XNVMEC_LREQ},
			{XNVMEC_OPT_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_DATA_INPUT, XNVMEC_LOPT},
			{XNVMEC_OPT_META_INPUT, XNVMEC_LOPT},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
			{XNVMEC_OPT_SYNC, XNVMEC_LOPT},
		},
	},
	{
		"write-uncor",
		"Mark a range of logical blocks as invalid",
		"Mark a range of logical blocks as invalid",
		sub_write_uncor,
		{
			{XNVMEC_OPT_URI, XNVMEC_POSA},
			{XNVMEC_OPT_SLBA, XNVMEC_LREQ},
			{XNVMEC_OPT_NLB, XNVMEC_LREQ},
			{XNVMEC_OPT_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_DATA_INPUT, XNVMEC_LOPT},
			{XNVMEC_OPT_META_INPUT, XNVMEC_LOPT},

			{XNVMEC_OPT_DEV_NSID, XNVMEC_LOPT},
			{XNVMEC_OPT_BE, XNVMEC_LOPT},
			{XNVMEC_OPT_ADMIN, XNVMEC_LOPT},
			{XNVMEC_OPT_SYNC, XNVMEC_LOPT},
		},
	},
};

static struct xnvmec g_cli = {
	.title = "Logical Block Namespace Utility",
	.descr_short = "Logical Block Namespace Utility",
	.descr_long = "",
	.subs = g_subs,
	.nsubs = sizeof g_subs / sizeof(*g_subs),
};

int
main(int argc, char **argv)
{
	return xnvmec(&g_cli, argc, argv, XNVMEC_INIT_DEV_OPEN);
}
