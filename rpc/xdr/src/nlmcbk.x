const LM_MAXSTRLEN = 1024;

struct nlm_sm_status {
        string mon_name<LM_MAXSTRLEN>; /* name of host */
        int state;                      /* new state */
        opaque priv[16];                /* private data */
};

program NLMCBK_PROGRAM {
	version NLMCBK_V0 {
		void NLMCBK_SM_NOTIFY(struct nlm_sm_status) = 1;
	} = 0;
} = 1238477;

