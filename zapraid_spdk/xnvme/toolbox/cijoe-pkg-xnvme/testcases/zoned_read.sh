#!/bin/bash
#
# Verify that CLI `zoned read` runs without error
#
# NOTE: As is, then this test will most likely fail due to the condition of the
# hard-coded zone address (0x0).
#
# shellcheck disable=SC2119
#
CIJ_TEST_NAME=$(basename "${BASH_SOURCE[0]}")
export CIJ_TEST_NAME
# shellcheck source=modules/cijoe.sh
source "$CIJ_ROOT/modules/cijoe.sh"
test.enter

: "${XNVME_URI:?Must be set and non-empty}"

: "${XNVME_DEV_NSID:?Must be set and non-empty}"
: "${XNVME_BE:?Must be set and non-empty}"
: "${XNVME_ADMIN:?Must be set and non-empty}"
: "${XNVME_SYNC:?Must be set and non-empty}"

# Instrumentation of the xNVMe runtime
XNVME_RT_ARGS=""
XNVME_RT_ARGS="${XNVME_RT_ARGS} --dev-nsid ${XNVME_DEV_NSID}"
XNVME_RT_ARGS="${XNVME_RT_ARGS} --be ${XNVME_BE}"
XNVME_RT_ARGS="${XNVME_RT_ARGS} --admin ${XNVME_ADMIN}"
XNVME_RT_ARGS="${XNVME_RT_ARGS} --sync ${XNVME_SYNC}"

: "${CMD_SLBA:=0x1}"
: "${CMD_NLB:=0}"

if ! cij.cmd "zoned read ${XNVME_URI} --slba ${CMD_SLBA} --nlb ${CMD_NLB} ${XNVME_RT_ARGS}"; then
  test.fail
fi

test.pass
