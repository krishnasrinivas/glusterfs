/*
  Copyright (c) 2006-2011 Gluster, Inc. <http://www.gluster.com>
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

#ifndef __COMMON_H__
#define __COMMON_H__

#define SET_FLOCK_PID(flock, lock) ((flock)->l_pid = lock->client_pid)
posix_lock_t *
new_posix_lock (struct gf_flock *flock, void *transport, pid_t client_pid,
                uint64_t owner, fd_t *fd);

pl_inode_t *
pl_inode_get (xlator_t *this, inode_t *inode);

posix_lock_t *
pl_getlk (pl_inode_t *inode, posix_lock_t *lock);

int
pl_setlk (xlator_t *this, pl_inode_t *inode, posix_lock_t *lock,
          int can_block);

void
grant_blocked_locks (xlator_t *this, pl_inode_t *inode);

void
posix_lock_to_flock (posix_lock_t *lock, struct gf_flock *flock);

int
locks_overlap (posix_lock_t *l1, posix_lock_t *l2);

int
same_owner (posix_lock_t *l1, posix_lock_t *l2);

void __delete_lock (pl_inode_t *, posix_lock_t *);

void __destroy_lock (posix_lock_t *);

pl_dom_list_t *
get_domain (pl_inode_t *pl_inode, const char *volume);

void
grant_blocked_inode_locks (xlator_t *this, pl_inode_t *pl_inode, pl_dom_list_t *dom);

void
__delete_inode_lock (pl_inode_lock_t *lock);

void
__destroy_inode_lock (pl_inode_lock_t *lock);

void
grant_blocked_entry_locks (xlator_t *this, pl_inode_t *pl_inode,
                           pl_entry_lock_t *unlocked, pl_dom_list_t *dom);

void pl_update_refkeeper (xlator_t *this, inode_t *inode);

int32_t
get_inodelk_count (xlator_t *this, inode_t *inode);

int32_t
get_entrylk_count (xlator_t *this, inode_t *inode);

void pl_trace_in (xlator_t *this, call_frame_t *frame, fd_t *fd, loc_t *loc,
                  int cmd, struct gf_flock *flock, const char *domain);

void pl_trace_out (xlator_t *this, call_frame_t *frame, fd_t *fd, loc_t *loc,
                   int cmd, struct gf_flock *flock, int op_ret, int op_errno, const char *domain);

void pl_trace_block (xlator_t *this, call_frame_t *frame, fd_t *fd, loc_t *loc,
                     int cmd, struct gf_flock *flock, const char *domain);

void pl_trace_flush (xlator_t *this, call_frame_t *frame, fd_t *fd);

void entrylk_trace_in (xlator_t *this, call_frame_t *frame, const char *volume,
                       fd_t *fd, loc_t *loc, const char *basename,
                       entrylk_cmd cmd, entrylk_type type);

void entrylk_trace_out (xlator_t *this, call_frame_t *frame, const char *volume,
                        fd_t *fd, loc_t *loc, const char *basename,
                        entrylk_cmd cmd, entrylk_type type,
                        int op_ret, int op_errno);

void entrylk_trace_block (xlator_t *this, call_frame_t *frame, const char *volume,
                          fd_t *fd, loc_t *loc, const char *basename,
                          entrylk_cmd cmd, entrylk_type type);

void
pl_print_verdict (char *str, int size, int op_ret, int op_errno);

void
pl_print_lockee (char *str, int size, fd_t *fd, loc_t *loc);

void
pl_print_locker (char *str, int size, xlator_t *this, call_frame_t *frame);

void
pl_print_inodelk (char *str, int size, int cmd, struct gf_flock *flock, const char *domain);

void
pl_trace_release (xlator_t *this, fd_t *fd);

unsigned long
fd_to_fdnum (fd_t *fd);

fd_t *
fd_from_fdnum (posix_lock_t *lock);

int
pl_reserve_setlk (xlator_t *this, pl_inode_t *pl_inode, posix_lock_t *lock,
                  int can_block);
int
reservelks_equal (posix_lock_t *l1, posix_lock_t *l2);

int
pl_verify_reservelk (xlator_t *this, pl_inode_t *pl_inode,
                     posix_lock_t *lock, int can_block);
int
pl_reserve_unlock (xlator_t *this, pl_inode_t *pl_inode, posix_lock_t *reqlock);

int
pl_locks_by_lkowner (pl_inode_t *pl_inode, uint64_t lk_owner);

#endif /* __COMMON_H__ */
