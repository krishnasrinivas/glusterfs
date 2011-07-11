/*
  Copyright (c) 2007-2010 Gluster, Inc. <http://www.gluster.com>
  This file is part of GlusterFS.

  GlusterFS is free software; you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published
  by the Free Software Foundation; either version 3 of the License,
  or (at your option) any later version.

  GlusterFS is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see
  <http://www.gnu.org/licenses/>.
*/


#ifndef _XDR_GENERIC_H
#define _XDR_GENERIC_H

#include <sys/uio.h>
//#include <rpc/rpc.h>
#include <rpc/types.h>
#include <rpc/xdr.h>

#define xdr_decoded_remaining_addr(xdr)        ((&xdr)->x_private)
#define xdr_decoded_remaining_len(xdr)         ((&xdr)->x_handy)
#define xdr_encoded_length(xdr) (((size_t)(&xdr)->x_private) - ((size_t)(&xdr)->x_base))
#define xdr_decoded_length(xdr) (((size_t)(&xdr)->x_private) - ((size_t)(&xdr)->x_base))

#define XDR_BYTES_PER_UNIT      4

ssize_t
xdr_serialize_generic (struct iovec outmsg, void *res, xdrproc_t proc);

ssize_t
xdr_to_generic (struct iovec inmsg, void *args, xdrproc_t proc);

ssize_t
xdr_to_generic_payload (struct iovec inmsg, void *args, xdrproc_t proc,
                        struct iovec *pendingpayload);


extern int
xdr_bytes_round_up (struct iovec *vec, size_t bufsize);

extern ssize_t
xdr_length_round_up (size_t len, size_t bufsize);

void
xdr_vector_round_up (struct iovec *vec, int vcount, uint32_t count);

#endif /* !_XDR_GENERIC_H */
