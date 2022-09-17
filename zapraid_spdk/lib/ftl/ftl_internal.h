/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 */

#ifndef FTL_INTERNAL_H
#define FTL_INTERNAL_H

#include "spdk/stdinc.h"
#include "spdk/crc32.h"
#include "spdk/util.h"
#include "spdk/uuid.h"

/* Marks address as invalid */
#define FTL_ADDR_INVALID	((ftl_addr)-1)

/*
 * This type represents address in the ftl address space. Values from 0 to based bdev size are
 * mapped directly to base device lbas. Values above that represent nv cache lbas.
 */
typedef uint64_t ftl_addr;

#endif /* FTL_INTERNAL_H */
