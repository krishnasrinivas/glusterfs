#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "defaults.h"
#include "rpcsvc.h"
#include "dict.h"
#include "xlator.h"
#include "nfs.h"
#include "mem-pool.h"
#include "logging.h"
#include "nfs-fops.h"
#include "inode.h"
#include "mount3.h"
#include "nfs3.h"
#include "nfs-mem-types.h"
#include "nfs3-helpers.h"
#include "nfs3-fh.h"
#include "nlm4.h"
#include "nlm4-xdr.h"
#include "msg-nfs3.h"
#include "nfs-generics.h"
#include "rpc-clnt.h"
#include <unistd.h>
#include <rpc/pmap_clnt.h>

typedef ssize_t (*nlm4_serializer) (struct iovec outmsg, void *args);
extern void
nfs3_call_state_wipe (nfs3_call_state_t *cs);

nfsstat3
nlm4_errno_to_nlm4stat (int errnum)
{
        nlm4_stats        stat = nlm4_denied;

        switch (errnum) {

        case 0:
                stat = nlm4_granted;
                break;
        default:
                stat = nlm4_denied;
                break;
        }

        return stat;
}

#define nlm4_validate_nfs3_state(request, state, status, label, retval) \
        do      {                                                       \
                state = rpcsvc_request_program_private (request);       \
                if (!state) {                                           \
                        gf_log (GF_NLM, GF_LOG_ERROR, "NFSv3 state "    \
                                "missing from RPC request");            \
                        rpcsvc_request_seterr (req, SYSTEM_ERR);        \
                        status = nlm4_failed;                           \
                        goto label;                                     \
                }                                                       \
        } while (0);                                                    \

nfs3_call_state_t *
nfs3_call_state_init (struct nfs3_state *s, rpcsvc_request_t *req, xlator_t *v);

#define nlm4_handle_call_state_init(nfs3state, calls, rq, opstat, errlabel)\
        do {                                                            \
                calls = nlm4_call_state_init ((nfs3state), (rq));       \
                if (!calls) {                                           \
                        gf_log (GF_NLM, GF_LOG_ERROR, "Failed to "      \
                                "init call state");                     \
                        opstat = nlm4_failed;                           \
                        rpcsvc_request_seterr (req, SYSTEM_ERR);        \
                        goto errlabel;                                  \
                }                                                       \
        } while (0)                                                     \

#define nlm4_validate_gluster_fh(handle, status, errlabel)              \
        do {                                                            \
                if (!nfs3_fh_validate (handle)) {                       \
                        status = nlm4_stale_fh;                         \
                        goto errlabel;                                  \
                }                                                       \
        } while (0)                                                     \

xlator_t *
nfs3_fh_to_xlator (struct nfs3_state *nfs3, struct nfs3_fh *fh);

#define nlm4_map_fh_to_volume(nfs3state, handle, rqst, volume, status, label) \
        do {                                                            \
                volume = nfs3_fh_to_xlator ((nfs3state), handle);       \
                if (!volume) {                                          \
                        gf_log (GF_NLM, GF_LOG_ERROR, "Failed to map "  \
                                "FH to vol");                           \
                        status = nlm4_stale_fh;                         \
                        goto label;                                     \
                } else {                                                \
                        gf_log (GF_NLM, GF_LOG_TRACE, "FH to Volume: %s"\
                                ,volume->name);                         \
                        rpcsvc_request_set_private (req, volume);       \
                }                                                       \
        } while (0);                                                    \

#define nlm4_volume_started_check(nfs3state, vlm, rtval, erlbl)         \
        do {                                                            \
              if ((!nfs_subvolume_started (nfs_state (nfs3state->nfsx), vlm))){\
                      gf_log (GF_NLM, GF_LOG_ERROR, "Volume is disabled: %s",\
                              vlm->name);                               \
                      rtval = RPCSVC_ACTOR_IGNORE;                      \
                      goto erlbl;                                       \
              }                                                         \
        } while (0)                                                     \

#define nlm4_check_fh_resolve_status(cst, _stat, erlabl)               \
        do {                                                            \
                if ((cst)->resolve_ret < 0) {                           \
                        _stat = nlm4_errno_to_nlm4stat (cst->resolve_errno);\
                        goto erlabl;                                    \
                }                                                       \
        } while (0)                                                     \

nfs3_call_state_t *
nlm4_call_state_init (struct nfs3_state *s, rpcsvc_request_t *req)
{
        nfs3_call_state_t       *cs = NULL;

        if ((!s) || (!req))
                return NULL;

        cs = (nfs3_call_state_t *) mem_get (s->localpool);
        if (!cs)
                return NULL;

        memset (cs, 0, sizeof (*cs));
        INIT_LIST_HEAD (&cs->entries.list);
        INIT_LIST_HEAD (&cs->openwait_q);
        cs->operrno = EINVAL;
        cs->req = req;
        cs->nfsx = s->nfsx;
        cs->nfs3state = s;

        return cs;
}

int
nlm4svc_submit_reply (rpcsvc_request_t *req, void *arg, nlm4_serializer sfunc)
{
        struct iovec            outmsg = {0, };
        struct iobuf            *iob = NULL;
        struct nfs3_state       *nfs3 = NULL;
        int                     ret = -1;
        struct iobref           *iobref = NULL;

        if (!req)
                return -1;

        nfs3 = (struct nfs3_state *)rpcsvc_request_program_private (req);
        if (!nfs3) {
                gf_log (GF_NLM, GF_LOG_ERROR, "mount state not found");
                goto ret;
        }

        /* First, get the io buffer into which the reply in arg will
         * be serialized.
         */
        iob = iobuf_get (nfs3->iobpool);
        if (!iob) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobuf");
                goto ret;
        }

        iobuf_to_iovec (iob, &outmsg);
        /* Use the given serializer to translate the give C structure in arg
         * to XDR format which will be written into the buffer in outmsg.
         */
        outmsg.iov_len = sfunc (outmsg, arg);

        iobref = iobref_new ();
        if (iobref == NULL) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobref");
                goto ret;
        }

        iobref_add (iobref, iob);

        /* Then, submit the message for transmission. */
        ret = rpcsvc_submit_message (req, &outmsg, 1, NULL, 0, iobref);
        iobuf_unref (iob);
        iobref_unref (iobref);
        if (ret == -1) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Reply submission failed");
                goto ret;
        }

        ret = 0;
ret:
        return ret;
}

int
nlm4svc_submit_request (rpcsvc_t *rpc, rpc_transport_t *trans,
                        rpcsvc_program_t *prog, int procnum,
                        struct nfs3_state *nfs3,
                        void *arg, nlm4_serializer sfunc)
{
        struct iovec            outmsg = {0, };
        struct iobuf            *iob = NULL;
        int                     ret = -1;
        struct iobref           *iobref = NULL;

/*
        nfs3 = (struct nfs3_state *)rpcsvc_request_program_private (req);
        if (!nfs3) {
                gf_log (GF_NLM, GF_LOG_ERROR, "mount state not found");
                goto ret;
        }
*/
        /* First, get the io buffer into which the reply in arg will
         * be serialized.
         */
        iob = iobuf_get (nfs3->iobpool);
        if (!iob) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobuf");
                goto ret;
        }

        iobuf_to_iovec (iob, &outmsg);
        /* Use the given serializer to translate the give C structure in arg
         * to XDR format which will be written into the buffer in outmsg.
         */
        outmsg.iov_len = sfunc (outmsg, arg);

        iobref = iobref_new ();
        if (iobref == NULL) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobref");
                goto ret;
        }

        iobref_add (iobref, iob);

        /* Then, submit the message for transmission. */
        ret = rpcsvc_submit_request (rpc, trans, prog, procnum,
                                     &outmsg, 1, iobref);
/*
        ret = rpcsvc_submit_message (req, &outmsg, 1, NULL, 0, iobref);
*/
        iobuf_unref (iob);
        iobref_unref (iobref);
        if (ret == -1) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Reply submission failed");
                goto ret;
        }

        ret = 0;
ret:
        return ret;
}

int
nlm4_generic_reply (rpcsvc_request_t *req, netobj cookie, nlm4_stats stat)
{
        nlm4_res res;

        memset (&res, 0, sizeof (res));
        res.cookie = cookie;
        res.stat.stat = stat;

        nlm4svc_submit_reply (req, (void *)&res,
                              (nlm4_serializer)xdr_serialize_nlm4_res);
        return 0;
}

int
nlm4svc_null (rpcsvc_request_t *req)
{
        struct iovec    dummyvec = {0, };

        if (!req) {
                gf_log (GF_MNT, GF_LOG_ERROR, "Got NULL request!");
                return 0;
        }
        rpcsvc_submit_generic (req, &dummyvec, 1,  NULL, 0, NULL);
        return 0;
}

int
nlm4_gf_flock_to_holder (nlm4_holder *holder, struct gf_flock *flock)
{
        switch (flock->l_type) {
        case GF_LK_F_WRLCK:
                holder->exclusive = 1;
                break;
        }

        holder->svid = flock->l_pid;
        holder->l_offset = flock->l_start;
        holder->l_len = flock->l_len;
        return 0;
}

int
nlm4_lock_to_gf_flock (struct gf_flock *flock, nlm4_lock *lock, int excl)
{
        flock->l_pid = lock->svid;
        flock->l_start = lock->l_offset;
        flock->l_len = lock->l_len;
        if (excl)
                flock->l_type = F_WRLCK;
        else
                flock->l_type = F_RDLCK;
        flock->l_whence = SEEK_SET;
        memcpy ((void *)&flock->l_owner, lock->oh.n_bytes, lock->oh.n_len);
        return 0;
}

int
nlm4_test_reply (nfs3_call_state_t *cs, nlm4_stats stat, struct gf_flock *flock)
{
        nlm4_testres res;

        memset (&res, 0, sizeof (res));
        res.cookie = cs->args.nlm4_testargs.cookie;
        res.stat.stat = stat;
        if (stat == nlm4_denied)
                nlm4_gf_flock_to_holder (&res.stat.nlm4_testrply_u.holder,
                                         flock);

        nlm4svc_submit_reply (cs->req, (void *)&res,
                              (nlm4_serializer)xdr_serialize_nlm4_testres);
        return 0;
}

int
nlm4svc_test_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno, struct gf_flock *flock)
{
        nlm4_stats                      stat = nlm4_denied;
        nfs3_call_state_t              *cs = NULL;

        cs = frame->local;
        if (op_ret == -1) {
                stat = nlm4_errno_to_nlm4stat (op_errno);
                goto err;
        } else if (flock->l_type == F_UNLCK)
                stat = nlm4_granted;

err:
        nlm4_test_reply (cs, stat, flock);
        nfs3_call_state_wipe (cs);
        return 0;
}

int
nlm4_test_fd_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_denied;
        int                             ret = -EFAULT;
        nfs_user_t                      nfu = {0, };
        nfs3_call_state_t               *cs = NULL;
        struct gf_flock flock = {0, };

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        nfs_request_user_init (&nfu, cs->req);
        nlm4_lock_to_gf_flock (&flock, &cs->args.nlm4_testargs.alock,
                               cs->args.nlm4_testargs.exclusive);
        nfu.lk_owner = flock.l_owner;
        ret = nfs_lk (cs->nfsx, cs->vol, &nfu, cs->fd, F_GETLK, &flock,
                      nlm4svc_test_cbk, cs);
        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);
nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_test_reply (cs, stat, &flock);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}


int
nlm4_test_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_failed;
        int                             ret = -1;
        nfs3_call_state_t               *cs = NULL;

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        ret = nfs3_file_open_and_resume (cs, nlm4_test_fd_resume);
        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);

nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_test_reply (cs, stat, NULL);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}

void
nlm4_prep_nlm4_testargs (nlm4_testargs *args, struct nfs3_fh *fh,
                         nlm4_lkowner_t *oh, char *cookiebytes)
{
        memset (args, 0, sizeof (*args));
        args->alock.fh.n_bytes = (void *)fh;
        args->alock.oh.n_bytes = (void *)oh;
        args->cookie.n_bytes = (void *)cookiebytes;
}

void
nlm4_prep_nlm4_lockargs (nlm4_lockargs *args, struct nfs3_fh *fh,
                         nlm4_lkowner_t *oh, char *cookiebytes)
{
        memset (args, 0, sizeof (*args));
        args->alock.fh.n_bytes = (void *)fh;
        args->alock.oh.n_bytes = (void *)oh;
        args->cookie.n_bytes = (void *)cookiebytes;
}

void
nlm4_prep_nlm4_cancargs (nlm4_cancargs *args, struct nfs3_fh *fh,
                           nlm4_lkowner_t *oh, char *cookiebytes)
{
        memset (args, 0, sizeof (*args));
        args->alock.fh.n_bytes = (void *)fh;
        args->alock.oh.n_bytes = (void *)oh;
        args->cookie.n_bytes = (void *)cookiebytes;
}

void
nlm4_prep_nlm4_unlockargs (nlm4_unlockargs *args, struct nfs3_fh *fh,
                           nlm4_lkowner_t *oh, char *cookiebytes)
{
        memset (args, 0, sizeof (*args));
        args->alock.fh.n_bytes = (void *)fh;
        args->alock.oh.n_bytes = (void *)oh;
        args->cookie.n_bytes = (void *)cookiebytes;
}

int
nlm4svc_test (rpcsvc_request_t *req)
{
        xlator_t                        *vol = NULL;
        nlm4_stats                      stat = nlm4_failed;
        struct nfs_state               *nfs = NULL;
        nfs3_state_t                   *nfs3 = NULL;
        nfs3_call_state_t               *cs = NULL;
        int                             ret = RPCSVC_ACTOR_ERROR;
        struct nfs3_fh                  fh = {{0}, };

        gf_log (GF_NLM, GF_LOG_INFO, "enter");

        if (!req)
                return ret;

        nlm4_validate_nfs3_state (req, nfs3, stat, rpcerr, ret);
        nfs = nfs_state (nfs3->nfsx);
        nlm4_handle_call_state_init (nfs->nfs3state, cs, req,
                                     stat, rpcerr);

        nlm4_prep_nlm4_testargs (&cs->args.nlm4_testargs, &fh, &cs->lkowner,
                                 cs->cookiebytes);
        if (xdr_to_nlm4_testargs(req->msg[0], &cs->args.nlm4_testargs) <= 0) {
                gf_log (GF_NFS3, GF_LOG_ERROR, "Error decoding args");
                rpcsvc_request_seterr (req, GARBAGE_ARGS);
                goto rpcerr;
        }

        nlm4_validate_gluster_fh (&fh, stat, nlm4err);
        nlm4_map_fh_to_volume (cs->nfs3state, &fh, req, vol, stat, nlm4err);
        cs->vol = vol;
        nlm4_volume_started_check (nfs3, vol, ret, rpcerr);

        ret = nfs3_fh_resolve_and_resume (cs, &fh,
                                          NULL, nlm4_test_resume);

nlm4err:
        if (ret < 0) {
//                nlm4_log_common_res (rpcsvc_request_xid (req), "READ", stat,
//                                     -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_test_reply (cs, stat, NULL);
                nfs3_call_state_wipe (cs);
                return 0;
        }

rpcerr:
        if (ret < 0) {
                nfs3_call_state_wipe (cs);
        }
        return ret;
}

rpc_clnt_procedure_t nlm4_clnt_actors[NLM4_PROC_COUNT] = {
        [NLM4_NULL] = {"NULL", NULL},
        [NLM4_GRANTED] = {"GRANTED", NULL},
};

char *nlm4_clnt_names[NLM4_PROC_COUNT] = {
        [NLM4_NULL] = "NULL",
        [NLM4_GRANTED] = "GRANTED",
};

rpc_clnt_prog_t nlm4clntprog = {
        .progname = "NLMv4",
        .prognum = NLM_PROGRAM,
        .progver = NLM_V4,
        .numproc = NLM4_PROC_COUNT,
        .proctable = nlm4_clnt_actors,
        .procnames = nlm4_clnt_names,
};

int
nlm4svc_send_granted_cbk (struct rpc_req *req, struct iovec *iov, int count,
                          void *myframe)
{
        // destroy the frame
        return 0;
}

void
nlm4svc_send_granted (nfs3_call_state_t *cs);

void *
nlm4_establish_callback (void *csarg)
{
        nfs3_call_state_t *cs;
        struct sockaddr_storage sa;
        struct sockaddr_in *sin;
        dict_t *options = NULL;
        char *peerip = NULL, *portstr;
        rpc_clnt_t *rpc_clnt;
        int port;
        int ret;

        cs = (nfs3_call_state_t *) csarg;
        rpc_transport_get_peeraddr (cs->trans, NULL, 0, &sa, sizeof (sa));
        sin = (struct sockaddr_in*) &sa;
        peerip = inet_ntoa (sin->sin_addr);
        port = pmap_getport (sin, NLM_PROGRAM, NLM_V4, IPPROTO_TCP);

        options = dict_new();
        ret = dict_set_str (options, "transport-type", "socket");
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        ret = dict_set_dynstr (options, "remote-host", strdup (peerip));
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        ret = gf_asprintf (&portstr, "%d", port);
        if (ret == -1)
                goto err;

        ret = dict_set_dynstr (options, "remote-port",
                               portstr);
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_dynstr error");
                goto err;
        }

        ret = dict_set_str (options, "non-blocking-io", "off");
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_dynstr error");
                goto err;
        }

        rpc_clnt = rpc_clnt_new (options, cs->nfsx->ctx, "NLM-client");
        rpc_transport_connect (rpc_clnt->conn.trans, port);
        rpc_clnt_set_connected (&rpc_clnt->conn);
        cs->nfs3state->rpc_clnt = rpc_clnt;
        ret = dict_set_static_ptr (cs->nfs3state->nlm_cbk_clnt, peerip, &rpc_clnt);
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_ptr error");
                goto err;
        }
        nlm4svc_send_granted (cs);
err:
        return NULL;
}

void
nlm4svc_send_granted (nfs3_call_state_t *cs)
{
        int ret = -1;
        rpc_clnt_t *rpc_clnt = NULL;
        struct iovec            outmsg = {0, };
        nlm4_testargs testargs;
        struct iobuf *iobuf;
        struct iobref *iobref;
        struct nfs_state *nfs;
        char *peerip;
        pthread_t thr;
        struct sockaddr_storage sa;
        struct sockaddr_in *sin;

        nfs = cs->nfsx->private;


        rpc_transport_get_peeraddr (cs->trans, NULL, 0, &sa, sizeof (sa));
        sin = (struct sockaddr_in *) &sa;
        peerip = inet_ntoa (sin->sin_addr);
        ret = dict_get_ptr (cs->nfs3state->nlm_cbk_clnt, peerip, (void *) &rpc_clnt);
        if (ret != 0) {
                pthread_create (&thr, NULL, nlm4_establish_callback, (void*)cs);
                return;
        }
//        rpc_clnt = cs->nfs3state->rpc_clnt;

        testargs.cookie = cs->args.nlm4_lockargs.cookie;
        testargs.exclusive = cs->args.nlm4_lockargs.exclusive;
        testargs.alock = cs->args.nlm4_lockargs.alock;

        iobuf = iobuf_get (cs->nfs3state->iobpool);
        if (!iobuf) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobuf");
                goto ret;
        }

        iobuf_to_iovec (iobuf, &outmsg);
        /* Use the given serializer to translate the give C structure in arg
         * to XDR format which will be written into the buffer in outmsg.
         */
        outmsg.iov_len = xdr_serialize_nlm4_testargs (outmsg, &testargs);

        iobref = iobref_new ();
        if (iobref == NULL) {
                gf_log (GF_NLM, GF_LOG_ERROR, "Failed to get iobref");
                goto ret;
        }

        iobref_add (iobref, iobuf);

        ret = rpc_clnt_submit (rpc_clnt, &nlm4clntprog, NLM4_GRANTED,
                               nlm4svc_send_granted_cbk,
                               &outmsg, 1,
                               NULL,
                               0, iobref, cs->frame, NULL, 0,
                               NULL, 0, NULL);

        if (ret < 0) {
                gf_log (GF_NLM, GF_LOG_ERROR, "rpc_clnt_submit error");
                goto ret;
        }
ret:
        nfs3_call_state_wipe (cs);
        return;
}

int
nlm4svc_lock_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                  int32_t op_ret, int32_t op_errno, struct gf_flock *flock)
{
        nlm4_stats                      stat = nlm4_denied;
        nfs3_call_state_t              *cs = NULL;

        cs = frame->local;

        if (op_ret == -1) {
                stat = nlm4_errno_to_nlm4stat (op_errno);
                goto err;
        } else
                stat = nlm4_granted;

err:
        if (cs->args.nlm4_lockargs.block) {
                cs->frame = copy_frame (frame);
                nlm4svc_send_granted (cs);
        } else {
                nlm4_generic_reply (cs->req, cs->args.nlm4_lockargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }
        return 0;
}

int
nlm4_lock_fd_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_denied;
        int                             ret = -EFAULT;
        nfs_user_t                      nfu = {0, };
        nfs3_call_state_t               *cs = NULL;
        struct gf_flock                 flock = {0, };

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        nfs_request_user_init (&nfu, cs->req);
        nlm4_lock_to_gf_flock (&flock, &cs->args.nlm4_lockargs.alock,
                               cs->args.nlm4_lockargs.exclusive);
        nfu.lk_owner = flock.l_owner;
        if (cs->args.nlm4_lockargs.block) {
                nlm4_generic_reply (cs->req, cs->args.nlm4_lockargs.cookie,
                                    nlm4_blocked);
                ret = nfs_lk (cs->nfsx, cs->vol, &nfu, cs->fd, F_SETLKW,
                              &flock, nlm4svc_lock_cbk, cs);
        } else
                ret = nfs_lk (cs->nfsx, cs->vol, &nfu, cs->fd, F_SETLK,
                              &flock, nlm4svc_lock_cbk, cs);

        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);
nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_lockargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}


int
nlm4_lock_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_failed;
        int                             ret = -1;
        nfs3_call_state_t               *cs = NULL;

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        ret = nfs3_file_open_and_resume (cs, nlm4_lock_fd_resume);
        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);

nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_lockargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}


int
nlm4svc_lock (rpcsvc_request_t *req)
{
        xlator_t                        *vol = NULL;
        nlm4_stats                      stat = nlm4_failed;
        struct nfs_state               *nfs = NULL;
        nfs3_state_t                   *nfs3 = NULL;
        nfs3_call_state_t               *cs = NULL;
        int                             ret = RPCSVC_ACTOR_ERROR;
        struct nfs3_fh                  fh = {{0}, };

        gf_log (GF_NLM, GF_LOG_INFO, "enter");

        if (!req)
                return ret;

        nlm4_validate_nfs3_state (req, nfs3, stat, rpcerr, ret);
        nfs = nfs_state (nfs3->nfsx);
        nlm4_handle_call_state_init (nfs->nfs3state, cs, req,
                                     stat, rpcerr);

        nlm4_prep_nlm4_lockargs (&cs->args.nlm4_lockargs, &cs->lockfh,
                                 &cs->lkowner, cs->cookiebytes);
        if (xdr_to_nlm4_lockargs(req->msg[0], &cs->args.nlm4_lockargs) <= 0) {
                gf_log (GF_NFS3, GF_LOG_ERROR, "Error decoding args");
                rpcsvc_request_seterr (req, GARBAGE_ARGS);
                goto rpcerr;
        }
        fh = cs->lockfh;
        nlm4_validate_gluster_fh (&fh, stat, nlm4err);
        nlm4_map_fh_to_volume (cs->nfs3state, &fh, req, vol, stat, nlm4err);
        cs->vol = vol;
        cs->trans = rpcsvc_request_transport_ref(req);
        nlm4_volume_started_check (nfs3, vol, ret, rpcerr);

        ret = nfs3_fh_resolve_and_resume (cs, &fh,
                                          NULL, nlm4_lock_resume);

nlm4err:
        if (ret < 0) {
//                nlm4_log_common_res (rpcsvc_request_xid (req), "READ", stat,
//                                     -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_lockargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
                return 0;
        }

rpcerr:
        if (ret < 0) {
                nfs3_call_state_wipe (cs);
        }
        return ret;
}

int
nlm4svc_cancel_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                    int32_t op_ret, int32_t op_errno, struct gf_flock *flock)
{
        nlm4_stats                      stat = nlm4_denied;
        nfs3_call_state_t              *cs = NULL;

        cs = frame->local;
        if (op_ret == -1) {
                stat = nlm4_errno_to_nlm4stat (op_errno);
                goto err;
        } else
                stat = nlm4_granted;

err:
        nlm4_generic_reply (cs->req, cs->args.nlm4_cancargs.cookie,
                            stat);
        nfs3_call_state_wipe (cs);
        return 0;
}

int
nlm4_cancel_fd_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_denied;
        int                             ret = -EFAULT;
        nfs_user_t                      nfu = {0, };
        nfs3_call_state_t               *cs = NULL;
        struct gf_flock                 flock = {0, };

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        nfs_request_user_init (&nfu, cs->req);
        nlm4_lock_to_gf_flock (&flock, &cs->args.nlm4_cancargs.alock,
                               cs->args.nlm4_cancargs.exclusive);
        flock.l_type = F_UNLCK;
        nfu.lk_owner = flock.l_owner;
        ret = nfs_lk (cs->nfsx, cs->vol, &nfu, cs->fd, F_SETLK,
                      &flock, nlm4svc_cancel_cbk, cs);

        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);
nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_cancargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}

int
nlm4_cancel_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_failed;
        int                             ret = -1;
        nfs3_call_state_t               *cs = NULL;

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        ret = nfs3_file_open_and_resume (cs, nlm4_cancel_fd_resume);
        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);

nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_cancargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}

int
nlm4svc_cancel (rpcsvc_request_t *req)
{
        xlator_t                        *vol = NULL;
        nlm4_stats                      stat = nlm4_failed;
        struct nfs_state               *nfs = NULL;
        nfs3_state_t                   *nfs3 = NULL;
        nfs3_call_state_t               *cs = NULL;
        int                             ret = RPCSVC_ACTOR_ERROR;
        struct nfs3_fh                  fh = {{0}, };

        gf_log (GF_NLM, GF_LOG_INFO, "enter");

        if (!req)
                return ret;

        nlm4_validate_nfs3_state (req, nfs3, stat, rpcerr, ret);
        nfs = nfs_state (nfs3->nfsx);
        nlm4_handle_call_state_init (nfs->nfs3state, cs, req,
                                     stat, rpcerr);

        nlm4_prep_nlm4_cancargs (&cs->args.nlm4_cancargs, &fh, &cs->lkowner,
                                   cs->cookiebytes);
        if (xdr_to_nlm4_cancelargs(req->msg[0], &cs->args.nlm4_cancargs) <= 0) {
                gf_log (GF_NFS3, GF_LOG_ERROR, "Error decoding args");
                rpcsvc_request_seterr (req, GARBAGE_ARGS);
                goto rpcerr;
        }

        nlm4_validate_gluster_fh (&fh, stat, nlm4err);
        nlm4_map_fh_to_volume (cs->nfs3state, &fh, req, vol, stat, nlm4err);
        cs->vol = vol;
        nlm4_volume_started_check (nfs3, vol, ret, rpcerr);

        ret = nfs3_fh_resolve_and_resume (cs, &fh,
                                          NULL, nlm4_cancel_resume);

nlm4err:
        if (ret < 0) {
//                nlm4_log_common_res (rpcsvc_request_xid (req), "READ", stat,
//                                     -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_cancargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
                return 0;
        }

rpcerr:
        if (ret < 0) {
                nfs3_call_state_wipe (cs);
        }
        return ret;
}

int
nlm4svc_unlock_cbk (call_frame_t *frame, void *cookie, xlator_t *this,
                    int32_t op_ret, int32_t op_errno, struct gf_flock *flock)
{
        nlm4_stats                      stat = nlm4_denied;
        nfs3_call_state_t              *cs = NULL;

        cs = frame->local;
        if (op_ret == -1) {
                stat = nlm4_errno_to_nlm4stat (op_errno);
                goto err;
        } else
                stat = nlm4_granted;

err:
        nlm4_generic_reply (cs->req, cs->args.nlm4_unlockargs.cookie, stat);
        nfs3_call_state_wipe (cs);
        return 0;
}

int
nlm4_unlock_fd_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_denied;
        int                             ret = -EFAULT;
        nfs_user_t                      nfu = {0, };
        nfs3_call_state_t               *cs = NULL;
        struct gf_flock                 flock = {0, };

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        nfs_request_user_init (&nfu, cs->req);
        nlm4_lock_to_gf_flock (&flock, &cs->args.nlm4_unlockargs.alock, 0);
        flock.l_type = F_UNLCK;
        nfu.lk_owner = flock.l_owner;
        ret = nfs_lk (cs->nfsx, cs->vol, &nfu, cs->fd, F_SETLK,
                      &flock, nlm4svc_unlock_cbk, cs);

        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);
nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_unlockargs.cookie,
                                    stat);
                nfs3_call_state_wipe (cs);
        }

        return ret;
}

int
nlm4_unlock_resume (void *carg)
{
        nlm4_stats                      stat = nlm4_failed;
        int                             ret = -1;
        nfs3_call_state_t               *cs = NULL;

        if (!carg)
                return ret;

        cs = (nfs3_call_state_t *)carg;
        nlm4_check_fh_resolve_status (cs, stat, nlm4err);
        ret = nfs3_file_open_and_resume (cs, nlm4_unlock_fd_resume);
        if (ret < 0)
                stat = nlm4_errno_to_nlm4stat (-ret);

nlm4err:
        if (ret < 0) {
//                nfs3_log_common_res (rpcsvc_request_xid (cs->req), "READ",
//                                     stat, -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (cs->req, cs->args.nlm4_unlockargs.cookie,
                                    stat);

                nfs3_call_state_wipe (cs);
        }

        return ret;
}

int
nlm4svc_unlock (rpcsvc_request_t *req)
{
        xlator_t                        *vol = NULL;
        nlm4_stats                      stat = nlm4_failed;
        struct nfs_state               *nfs = NULL;
        nfs3_state_t                   *nfs3 = NULL;
        nfs3_call_state_t               *cs = NULL;
        int                             ret = RPCSVC_ACTOR_ERROR;
        struct nfs3_fh                  fh = {{0}, };

        gf_log (GF_NLM, GF_LOG_INFO, "enter");

        if (!req)
                return ret;

        nlm4_validate_nfs3_state (req, nfs3, stat, rpcerr, ret);
        nfs = nfs_state (nfs3->nfsx);
        nlm4_handle_call_state_init (nfs->nfs3state, cs, req,
                                     stat, rpcerr);

        nlm4_prep_nlm4_unlockargs (&cs->args.nlm4_unlockargs, &fh, &cs->lkowner,
                                   cs->cookiebytes);
        if (xdr_to_nlm4_unlockargs(req->msg[0], &cs->args.nlm4_unlockargs) <= 0)
        {
                gf_log (GF_NFS3, GF_LOG_ERROR, "Error decoding args");
                rpcsvc_request_seterr (req, GARBAGE_ARGS);
                goto rpcerr;
        }

        nlm4_validate_gluster_fh (&fh, stat, nlm4err);
        nlm4_map_fh_to_volume (cs->nfs3state, &fh, req, vol, stat, nlm4err);
        cs->vol = vol;
        nlm4_volume_started_check (nfs3, vol, ret, rpcerr);

        ret = nfs3_fh_resolve_and_resume (cs, &fh,
                                          NULL, nlm4_unlock_resume);

nlm4err:
        if (ret < 0) {
//                nlm4_log_common_res (rpcsvc_request_xid (req), "READ", stat,
//                                     -ret);
                gf_log (GF_NLM, GF_LOG_ERROR, "error here");
                nlm4_generic_reply (req, cs->args.nlm4_unlockargs.cookie, stat);
                nfs3_call_state_wipe (cs);
                return 0;
        }

rpcerr:
        if (ret < 0) {
                nfs3_call_state_wipe (cs);
        }
        return ret;
}
/*
int
nlm4svc_notify (rpcsvc_t *rpcsvc, void *data1, rpcsvc_event_t event,
                void *data2)
{
        dict_t *options = NULL;
        struct sockaddr_storage sa;
        struct sockaddr_in *sin;
        char *peer, *portstr;
        int ret = -1;
        rpc_clnt_t *rpc_clnt;
        rpc_transport_t *trans = NULL;
        xlator_t *nfsx;
        struct nfs_state *nfs = NULL;

        nfsx = data1;
        trans = data2;

        if (strcmp("socket.NLM", trans->name)|| event != RPCSVC_EVENT_ACCEPT)
                return 0;
        else
                gf_log (GF_NFS, GF_LOG_ERROR, "nlm conn");

        ret = rpc_transport_get_peeraddr (trans, NULL, 0, &sa, sizeof (sa));
        sin = (struct sockaddr_in *) &sa;
        peer = inet_ntoa (sin->sin_addr);

        options = dict_new();

        ret = dict_set_str (options, "transport-type", "socket");
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        ret = dict_set_dynstr (options, "remote-host", strdup (peer));
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        ret = gf_asprintf (&portstr, "%d", 5000);
        if (ret == -1)
                goto err;

        ret = dict_set_dynstr (options, "remote-port",
                               portstr);
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        dict_set_str (options, "non-blocking-io", "off");

        rpc_clnt = rpc_clnt_new (options, nfsx->ctx, trans->name);
        rpc_transport_connect (rpc_clnt->conn.trans, 5000);

        nfs = nfsx->private;
        nfs->rpc_clnt = rpc_clnt;
err:
        return 0;
}
*/

rpcsvc_actor_t  nlm4svc_actors[NLM4_PROC_COUNT] = {
        {"NULL", NLM4_NULL, nlm4svc_null, NULL, NULL},
        {"TEST", NLM4_TEST, nlm4svc_test, NULL, NULL},
        {"LOCK", NLM4_LOCK, nlm4svc_lock, NULL, NULL},
        {"CANCEL", NLM4_CANCEL, nlm4svc_cancel, NULL, NULL},
        {"UNLOCK", NLM4_UNLOCK, nlm4svc_unlock, NULL, NULL},
        {"GRANTED", NLM4_GRANTED, NULL, NULL, NULL},
        {"TEST", NLM4_TEST_MSG, NULL, NULL, NULL},
        {"LOCK", NLM4_LOCK_MSG, NULL, NULL, NULL},
        {"CANCEL", NLM4_CANCEL_MSG, NULL, NULL, NULL},
        {"UNLOCK", NLM4_UNLOCK_MSG, NULL, NULL, NULL},
        {"GRANTED", NLM4_GRANTED_MSG, NULL, NULL, NULL},
        {"TEST", NLM4_TEST_RES, NULL, NULL, NULL},
        {"LOCK", NLM4_LOCK_RES, NULL, NULL, NULL},
        {"CANCEL", NLM4_CANCEL_RES, NULL, NULL, NULL},
        {"UNLOCK", NLM4_UNLOCK_RES, NULL, NULL, NULL},
        {"GRANTED", NLM4_GRANTED_RES, NULL, NULL, NULL},
        {"SM_NOTIFY", NLM4_SM_NOTIFY, NULL, NULL, NULL},
};

rpcsvc_program_t        nlm4prog = {
        .progname       = "NLM4",
        .prognum        = NLM_PROGRAM,
        .progver        = NLM_V4,
        .progport       = GF_NLM4_PORT,
        .actors         = nlm4svc_actors,
        .numactors      = NLM4_PROC_COUNT,
        .min_auth       = AUTH_NULL,
};


int
nlm4_init_state (xlator_t *nfsx)
{
        return 0;
/*
        struct nfs3_state *ns = NULL;
        struct nfs_state *nfs = NULL;

        nfs = nfsx->private;
        ns = GF_CALLOC (1, sizeof (*ns), gf_nfs_mt_nfs3_state);
        if (ns == NULL) {
                gf_log (GF_MNT, GF_LOG_ERROR, "Memory allocation failed");
                return -1;
        }

        ns->iobpool = nfsx->ctx->iobuf_pool;
        ns->nfsx = nfsx;
        nfs->nlm4state = ns;
        return 0;
*/
}

extern rpcsvc_program_t *
nlm4svc_init(xlator_t *nfsx)
{
        struct nfs3_state *ns = NULL;
        struct nfs_state *nfs = NULL;
        dict_t *options = NULL;
        int ret = -1;
        char *portstr = NULL;

        nfs = (struct nfs_state*)nfsx->private;

        ns = nfs->nfs3state;
        if (!ns) {
                gf_log (GF_NLM, GF_LOG_ERROR, "NLM4 init failed");
                goto err;
        }
        nlm4prog.private = ns;

        options = dict_new ();

        ret = gf_asprintf (&portstr, "%d", GF_NLM4_PORT);
        if (ret == -1)
                goto err;

        ret = dict_set_dynstr (options, "transport.socket.listen-port",
                               portstr);
        if (ret == -1)
                goto err;
        ret = dict_set_str (options, "transport-type", "socket");
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                goto err;
        }

        if (nfs->allow_insecure) {
                ret = dict_set_str (options, "rpc-auth-allow-insecure", "on");
                if (ret == -1) {
                        gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                        goto err;
                }
                ret = dict_set_str (options, "rpc-auth.ports.insecure", "on");
                if (ret == -1) {
                        gf_log (GF_NFS, GF_LOG_ERROR, "dict_set_str error");
                        goto err;
                }
        }

        rpcsvc_create_listeners (nfs->rpcsvc, options, "NLM");
        if (ret == -1) {
                gf_log (GF_NFS, GF_LOG_ERROR, "Unable to create listeners");
                dict_unref (options);
                goto err;
        }
        ns->nlm_cbk_clnt = dict_new();

//        rpcsvc_register_notify (nfs->rpcsvc, nlm4svc_notify, nfsx);

        return &nlm4prog;
err:
        return NULL;
}
