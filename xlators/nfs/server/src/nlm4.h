/*
  Copyright (c) 2010-2011 Gluster, Inc. <http://www.gluster.com>
  This file is part of GlusterFS.

  GlusterFS is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  GlusterFS is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/

#ifndef _NLM4_H_
#define _NLM4_H_

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "rpcsvc.h"
#include "dict.h"
#include "xlator.h"
#include "iobuf.h"
#include "nfs.h"
#include "list.h"
#include "xdr-nfs3.h"
#include "locking.h"
#include "nfs3-fh.h"
#include "uuid.h"
#include "nlm4-xdr.h"

/* Registered with portmap */
#define GF_NLM4_PORT            38468
#define GF_NLM                  GF_NFS"-NLM"

extern rpcsvc_program_t *
nlm4svc_init (xlator_t *nfsx);

extern int
nlm4_init_state (xlator_t *nfsx);

#define NLM_PROGRAM 100021
#define NLM_V4 4

typedef struct nlm4_lwowner {
        char temp[1024];
} nlm4_lkowner_t;
/*
typedef struct nlm4_state {
        xlator_t *nfsx;
        struct iobuf_pool *iobpool;
} nlm4_state_t;
*/

typedef struct nlm_client {
        struct sockaddr_storage sa;
        pid_t uniq;
        struct list_head nlm_clients;
        struct list_head fds;
} nlm_client_t;

#endif
