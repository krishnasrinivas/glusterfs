/*
  Copyright (c) 2008-2011 Gluster, Inc. <http://www.gluster.com>
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


#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

/* TODO: add NS locking */

#include "statedump.h"
#include "dht-common.h"

/* TODO:
   - use volumename in xattr instead of "dht"
   - use NS locks
   - handle all cases in self heal layout reconstruction
   - complete linkfile selfheal
*/
struct volume_options options[];

void
dht_layout_dump (dht_layout_t  *layout, const char *prefix)
{

        char    key[GF_DUMP_MAX_BUF_LEN];
        int     i = 0;

        GF_VALIDATE_OR_GOTO ("dht", layout, out);
        GF_VALIDATE_OR_GOTO ("dht", prefix, out);

        gf_proc_dump_build_key(key, prefix, "cnt");
        gf_proc_dump_write(key, "%d", layout->cnt);
        gf_proc_dump_build_key(key, prefix, "preset");
        gf_proc_dump_write(key, "%d", layout->preset);
        gf_proc_dump_build_key(key, prefix, "gen");
        gf_proc_dump_write(key, "%d", layout->gen);
        gf_proc_dump_build_key(key, prefix, "type");
        gf_proc_dump_write(key, "%d", layout->type);

        for (i = 0; i < layout->cnt; i++) {
                gf_proc_dump_build_key(key, prefix,"list[%d].err", i);
                gf_proc_dump_write(key, "%d", layout->list[i].err);
                gf_proc_dump_build_key(key, prefix,"list[%d].start", i);
                gf_proc_dump_write(key, "%u", layout->list[i].start);
                gf_proc_dump_build_key(key, prefix,"list[%d].stop", i);
                gf_proc_dump_write(key, "%u", layout->list[i].stop);
                if (layout->list[i].xlator) {
                        gf_proc_dump_build_key(key, prefix,
                                               "list[%d].xlator.type", i);
                        gf_proc_dump_write(key, "%s",
                                           layout->list[i].xlator->type);
                        gf_proc_dump_build_key(key, prefix,
                                               "list[%d].xlator.name", i);
                        gf_proc_dump_write(key, "%s",
                                           layout->list[i].xlator->name);
                }
        }

out:
        return;
}


int32_t
dht_priv_dump (xlator_t *this)
{
        char            key_prefix[GF_DUMP_MAX_BUF_LEN];
        char            key[GF_DUMP_MAX_BUF_LEN];
        int             i = 0;
        dht_conf_t      *conf = NULL;
        int             ret = -1;

        GF_VALIDATE_OR_GOTO ("dht", this, out);

        conf = this->private;

        if (!conf)
                return -1;

        ret = TRY_LOCK(&conf->subvolume_lock);

        if (ret != 0) {
                gf_log("", GF_LOG_WARNING, "Unable to lock dht subvolume %s",
                       this->name);
                return ret;
        }

        gf_proc_dump_add_section("xlator.cluster.dht.%s.priv", this->name);
        gf_proc_dump_build_key(key_prefix,"xlator.cluster.dht","%s.priv",
                               this->name);
        gf_proc_dump_write("subvol_cnt","%d", conf->subvolume_cnt);
        for (i = 0; i < conf->subvolume_cnt; i++) {
                sprintf (key, "subvolumes[%d]", i);
                gf_proc_dump_write(key, "%s.%s", conf->subvolumes[i]->type,
                                   conf->subvolumes[i]->name);
                if (conf->file_layouts && conf->file_layouts[i]){
                        sprintf (key, "file_layouts[%d]", i);
                        dht_layout_dump(conf->file_layouts[i], key);
                }
                if (conf->dir_layouts && conf->dir_layouts[i]) {
                        sprintf (key, "dir_layouts[%d]", i);
                        dht_layout_dump(conf->dir_layouts[i], key);
                }
                if (conf->subvolume_status) {

                        sprintf (key, "subvolume_status[%d]", i);
                        gf_proc_dump_write(key, "%d",
                                           (int)conf->subvolume_status[i]);
                }

        }

        gf_proc_dump_write("search_unhashed", "%d", conf->search_unhashed);
        gf_proc_dump_write("gen", "%d", conf->gen);
        gf_proc_dump_write("min_free_disk", "%lu", conf->min_free_disk);
	gf_proc_dump_write("min_free_inodes", "%lu", conf->min_free_inodes);
        gf_proc_dump_write("disk_unit", "%c", conf->disk_unit);
        gf_proc_dump_write("refresh_interval", "%d", conf->refresh_interval);
        gf_proc_dump_write("unhashed_sticky_bit", "%d", conf->unhashed_sticky_bit);
        if (conf ->du_stats) {
                gf_proc_dump_write("du_stats.avail_percent", "%lf",
                                   conf->du_stats->avail_percent);
                gf_proc_dump_write("du_stats.avail_space", "%lu",
                                   conf->du_stats->avail_space);
		gf_proc_dump_write("du_stats.avail_inodes", "%lf",
                                   conf->du_stats->avail_inodes);
                gf_proc_dump_write("du_stats.log", "%lu", conf->du_stats->log);
        }
        gf_proc_dump_write("last_stat_fetch", "%s", ctime(&conf->last_stat_fetch.tv_sec));

        UNLOCK(&conf->subvolume_lock);

out:
        return ret;
}

int32_t
dht_inodectx_dump (xlator_t *this, inode_t *inode)
{
        int             ret = -1;
        dht_layout_t    *layout = NULL;
        uint64_t        tmp_layout = 0;

        GF_VALIDATE_OR_GOTO ("dht", this, out);
        GF_VALIDATE_OR_GOTO ("dht", inode, out);

        ret = inode_ctx_get (inode, this, &tmp_layout);

        if (ret != 0)
                return ret;

        layout = (dht_layout_t *)(long)tmp_layout;

        if (!layout)
                return -1;

        gf_proc_dump_add_section("xlator.cluster.dht.%s.inode", this->name);
        dht_layout_dump(layout, "layout");

out:
        return ret;
}

int
notify (xlator_t *this, int event, void *data, ...)
{
        int ret = -1;

        GF_VALIDATE_OR_GOTO ("dht", this, out);

        ret = dht_notify (this, event, data);

out:
        return ret;
}

void
fini (xlator_t *this)
{
        int         i = 0;
        dht_conf_t *conf = NULL;

        GF_VALIDATE_OR_GOTO ("dht", this, out);

        conf = this->private;
        this->private = NULL;
        if (conf) {
                if (conf->file_layouts) {
                        for (i = 0; i < conf->subvolume_cnt; i++) {
                                GF_FREE (conf->file_layouts[i]);
                        }
                        GF_FREE (conf->file_layouts);
                }

                if (conf->subvolumes)
                        GF_FREE (conf->subvolumes);

                if (conf->subvolume_status)
                        GF_FREE (conf->subvolume_status);

                GF_FREE (conf);
        }
out:
        return;
}

int32_t
mem_acct_init (xlator_t *this)
{
        int     ret = -1;

        GF_VALIDATE_OR_GOTO ("dht", this, out);

        ret = xlator_mem_acct_init (this, gf_dht_mt_end + 1);

        if (ret != 0) {
                gf_log (this->name, GF_LOG_ERROR, "Memory accounting init"
                        "failed");
                return ret;
        }
out:
        return ret;
}


int
dht_parse_decommissioned_bricks (xlator_t *this, dht_conf_t *conf,
                                 const char *bricks)
{
        int         i  = 0;
        int         ret  = -1;
        char       *tmpstr = NULL;
        char       *dup_brick = NULL;
        char       *node = NULL;

        if (!conf || !bricks)
                goto out;

        dup_brick = gf_strdup (bricks);
        node = strtok_r (dup_brick, ",", &tmpstr);
        while (node) {
                for (i = 0; i < conf->subvolume_cnt; i++) {
                        if (!strcmp (conf->subvolumes[i]->name, node)) {
                                conf->decommissioned_bricks[i] =
                                        conf->subvolumes[i];
                                gf_log (this->name, GF_LOG_INFO,
                                        "decommissioning subvolume %s",
                                        conf->subvolumes[i]->name);
                                break;
                        }
                }
                if (i == conf->subvolume_cnt) {
                        /* Wrong node given. */
                        goto out;
                }
                node = strtok_r (NULL, ",", &tmpstr);
        }

        ret = 0;
out:
        if (dup_brick)
                GF_FREE (dup_brick);

        return ret;
}

int
reconfigure (xlator_t *this, dict_t *options)
{
        dht_conf_t      *conf = NULL;
        char            *temp_str = NULL;
        gf_boolean_t     search_unhashed;
        int              ret = -1;

        GF_VALIDATE_OR_GOTO ("dht", this, out);
        GF_VALIDATE_OR_GOTO ("dht", options, out);

        conf = this->private;
        if (!conf)
                return 0;

        if (dict_get_str (options, "lookup-unhashed", &temp_str) == 0) {
                /* If option is not "auto", other options _should_ be boolean*/
                if (strcasecmp (temp_str, "auto")) {
                        if (!gf_string2boolean (temp_str, &search_unhashed)) {
                                gf_log(this->name, GF_LOG_DEBUG, "Reconfigure:"
                                       " lookup-unhashed reconfigured (%s)",
                                       temp_str);
                                conf->search_unhashed = search_unhashed;
                        } else {
                                gf_log(this->name, GF_LOG_ERROR, "Reconfigure:"
                                       " lookup-unhashed should be boolean,"
                                       " not (%s), defaulting to (%d)",
                                       temp_str, conf->search_unhashed);
                                //return -1;
                                ret = -1;
                                goto out;
                        }
                } else {
                        gf_log(this->name, GF_LOG_DEBUG, "Reconfigure:"
                               " lookup-unhashed reconfigured auto ");
                        conf->search_unhashed = GF_DHT_LOOKUP_UNHASHED_AUTO;
                }
        }

	GF_OPTION_RECONF ("min-free-disk", conf->min_free_disk, options,
                          percent_or_size, out);
	GF_OPTION_RECONF ("min-free-inodes", conf->min_free_inodes, options,
                          percent, out);
        GF_OPTION_RECONF ("directory-layout-spread", conf->dir_spread_cnt,
                          options, uint32, out);

        if (dict_get_str (options, "decommissioned-bricks", &temp_str) == 0) {
                ret = dht_parse_decommissioned_bricks (this, conf, temp_str);
                if (ret == -1)
                        goto out;
        }

        ret = 0;
out:
        return ret;
}


int
init (xlator_t *this)
{
        dht_conf_t    *conf = NULL;
        char          *temp_str = NULL;
        int            ret = -1;
        int            i = 0;

        GF_VALIDATE_OR_GOTO ("dht", this, err);

        if (!this->children) {
                gf_log (this->name, GF_LOG_CRITICAL,
                        "Distribute needs more than one subvolume");
                return -1;
        }

        if (!this->parents) {
                gf_log (this->name, GF_LOG_WARNING,
                        "dangling volume. check volfile");
        }

        conf = GF_CALLOC (1, sizeof (*conf), gf_dht_mt_dht_conf_t);
        if (!conf) {
                goto err;
        }

        conf->search_unhashed = GF_DHT_LOOKUP_UNHASHED_ON;
        if (dict_get_str (this->options, "lookup-unhashed", &temp_str) == 0) {
                /* If option is not "auto", other options _should_ be boolean */
                if (strcasecmp (temp_str, "auto"))
                        gf_string2boolean (temp_str, &conf->search_unhashed);
                else
                        conf->search_unhashed = GF_DHT_LOOKUP_UNHASHED_AUTO;
        }

        GF_OPTION_INIT ("unhashed-sticky-bit", conf->unhashed_sticky_bit, bool,
                        err);

        GF_OPTION_INIT ("use-readdirp", conf->use_readdirp, bool, err);

	GF_OPTION_INIT ("min-free-disk", conf->min_free_disk, percent_or_size,
			err);

        GF_OPTION_INIT ("min-free-inodes", conf->min_free_inodes, percent,
                        err);

        conf->dir_spread_cnt = conf->subvolume_cnt;
        GF_OPTION_INIT ("directory-layout-spread", conf->dir_spread_cnt,
                        uint32, err);

        GF_OPTION_INIT ("assert-no-child-down", conf->assert_no_child_down,
                        bool, err);

        ret = dht_init_subvolumes (this, conf);
        if (ret == -1) {
                goto err;
        }

        if (dict_get_str (this->options, "decommissioned-bricks", &temp_str) == 0) {
                ret = dht_parse_decommissioned_bricks (this, conf, temp_str);
                if (ret == -1)
                        goto err;
        }

        ret = dht_layouts_init (this, conf);
        if (ret == -1) {
                goto err;
        }

        LOCK_INIT (&conf->subvolume_lock);
        LOCK_INIT (&conf->layout_lock);

        conf->gen = 1;

        /* Create 'syncop' environment */
	conf->env = syncenv_new (0);
        if (!conf->env) {
                gf_log (this->name, GF_LOG_ERROR,
                        "failed to create sync environment %s",
                        strerror (errno));
                goto err;
        }

        this->private = conf;

        return 0;

err:
        if (conf) {
                if (conf->file_layouts) {
                        for (i = 0; i < conf->subvolume_cnt; i++) {
                                GF_FREE (conf->file_layouts[i]);
                        }
                        GF_FREE (conf->file_layouts);
                }

                if (conf->subvolumes)
                        GF_FREE (conf->subvolumes);

                if (conf->subvolume_status)
                        GF_FREE (conf->subvolume_status);

                if (conf->du_stats)
                        GF_FREE (conf->du_stats);

                GF_FREE (conf);
        }

        return -1;
}


struct xlator_fops fops = {
        .lookup      = dht_lookup,
        .mknod       = dht_mknod,
        .create      = dht_create,

        .open        = dht_open,
        .statfs      = dht_statfs,
        .opendir     = dht_opendir,
        .readdir     = dht_readdir,
        .readdirp    = dht_readdirp,
        .fsyncdir    = dht_fsyncdir,
        .symlink     = dht_symlink,
        .unlink      = dht_unlink,
        .link        = dht_link,
        .mkdir       = dht_mkdir,
        .rmdir       = dht_rmdir,
        .rename      = dht_rename,
        .entrylk     = dht_entrylk,
        .fentrylk    = dht_fentrylk,

        /* Inode read operations */
        .stat        = dht_stat,
        .fstat       = dht_fstat,
        .access      = dht_access,
        .readlink    = dht_readlink,
        .getxattr    = dht_getxattr,
        .fgetxattr    = dht_fgetxattr,
        .readv       = dht_readv,
        .flush       = dht_flush,
        .fsync       = dht_fsync,
        .inodelk     = dht_inodelk,
        .finodelk    = dht_finodelk,
        .lk          = dht_lk,

        /* Inode write operations */
        .fremovexattr = dht_fremovexattr,
        .removexattr = dht_removexattr,
        .setxattr    = dht_setxattr,
        .fsetxattr   = dht_fsetxattr,
        .truncate    = dht_truncate,
        .ftruncate   = dht_ftruncate,
        .writev      = dht_writev,
        .xattrop     = dht_xattrop,
        .fxattrop    = dht_fxattrop,
        .setattr     = dht_setattr,
        .fsetattr    = dht_fsetattr,
};

struct xlator_dumpops dumpops = {
        .priv = dht_priv_dump,
        .inodectx = dht_inodectx_dump,
};


struct xlator_cbks cbks = {
//      .release    = dht_release,
//      .releasedir = dht_releasedir,
        .forget     = dht_forget
};


struct volume_options options[] = {
        { .key  = {"lookup-unhashed"},
          .value = {"auto", "yes", "no", "enable", "disable", "1", "0",
                    "on", "off"},
          .type = GF_OPTION_TYPE_STR,
          .default_value = "on",
        },
        { .key  = {"min-free-disk"},
          .type = GF_OPTION_TYPE_PERCENT_OR_SIZET,
          .default_value = "10%",
          .description = "Percentage/Size of disk space that must be "
                         "kept free."
        },
	{ .key  = {"min-free-inodes"},
          .type = GF_OPTION_TYPE_PERCENT,
          .default_value = "5%",
          .description = "Percentage inodes that must be "
                         "kept free."
        },
        { .key = {"unhashed-sticky-bit"},
          .type = GF_OPTION_TYPE_BOOL,
          .default_value = "off",
        },
        { .key = {"use-readdirp"},
          .type = GF_OPTION_TYPE_BOOL,
          .default_value = "on",
        },
        { .key = {"assert-no-child-down"},
          .type = GF_OPTION_TYPE_BOOL,
          .default_value = "off",
        },
        { .key  = {"directory-layout-spread"},
          .type = GF_OPTION_TYPE_INT,
        },
        { .key  = {"decommissioned-bricks"},
          .type = GF_OPTION_TYPE_ANY,
        },
        { .key  = {NULL} },
};
