#include "./zns_ftl.h"
#include "./zns.h"

static void zone_init_write_pointer(
      struct write_pointer *wpp,
      int ch, int ch_step,
      int lun, int lun_step,
      int blk, int blk_step)
{
    wpp->ch_beg = ch;
    wpp->ch_end = ch + ch_step;
    wpp->lun_beg = lun;
    wpp->lun_end = lun_step;
    wpp->blk_beg = blk;
    wpp->blk_end = blk + blk_step;
    wpp->ch = wpp->ch_beg;
    wpp->lun = wpp->lun_beg;
    wpp->pl = 0;
    wpp->blk = wpp->blk_beg;
    wpp->pg = 0;
    wpp->offset = 0;
}

static void zone_reset_write_pointer(struct write_pointer *wpp)
{
  zone_init_write_pointer(wpp, wpp->ch_beg, wpp->ch_end,
                              wpp->lun_beg, wpp->lun_end,
                              wpp->blk_beg, wpp->blk_end);
}

static void zns_init_zones(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct physical_zone_mgmt *pzm = &ssd->pzm;
    struct physical_zone *pz;

    int zid = 0;
    int chnl_step = spp->chnls_per_zone;
    int lun_step = spp->luns_per_chnl_per_zone;
    int blk_step = spp->blks_per_pl_per_zone;

    pzm->tt_physical_zones = spp->tt_pgs / spp->tt_pgs_per_zone;
    pzm->physical_zones = g_malloc0(sizeof(struct physical_zone) * pzm->tt_physical_zones);

    ssd->zone_mapping = g_malloc(sizeof(struct physical_zone*) * pzm->tt_physical_zones);

    QTAILQ_INIT(&pzm->free_physical_zone_list);

    pzm->free_physical_zone_cnt = 0;
    for (int chnl = 0; chnl < spp->nchs; chnl += chnl_step) {
      for (int lun = 0; lun < spp->luns_per_ch; lun += lun_step) {
        for (int blk = 0; blk < spp->blks_per_lun; blk += blk_step) {
          pz = &pzm->physical_zones[zid];
          zone_init_write_pointer(&pz->wp, chnl, chnl_step, lun, lun_step, blk, blk_step);
          pz->id = zid++;
        }
      }
    }

    for (int i = 0; i < pzm->tt_physical_zones; ++i) {
      ssd->zone_mapping[i] = &pzm->physical_zones[i];
    }

    ftl_assert(pzm->free_physical_zone_cnt == pzm->tt_physical_zones);
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static inline void check_addr_min_max(int a, int min, int max)
{
    ftl_assert(a >= min && a <= max);
}

static void physical_zone_advance_write_pointer(
              struct ssd *ssd, struct physical_zone *pz,
              struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = &pz->wp;

    ppa->g.sec = 0;
    ppa->g.pg = wpp->pg;
    ppa->g.blk = wpp->blk;
    ppa->g.pl = wpp->pl;
    ppa->g.lun = wpp->lun;
    ppa->g.ch = wpp->ch;

    check_addr_min_max(wpp->ch, wpp->ch_beg, wpp->ch_end);
    wpp->ch++;
    if (wpp->ch == wpp->ch_end) {
        wpp->ch = wpp->ch_beg;
        check_addr_min_max(wpp->lun, wpp->lun_beg, wpp->lun_end); 
        /* in this case, we should go to next lun */
        wpp->lun++;
        if (wpp->lun == wpp->lun_end) {
            wpp->lun = wpp->lun_beg;
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk);
            wpp->pg++;
            if (wpp->pg == spp->pgs_per_blk) {
                wpp->pg = 0;
                wpp->blk += 1;
            }
            if (wpp->blk == wpp->blk_end) {
                wpp->blk = wpp->blk_beg;
                wpp->pl += 1;
            }

            assert(wpp->blk <= wpp->blk_end);
        }
    }
}

static void get_ppa(struct ssd *ssd, struct physical_zone *pz, uint64_t lpn_in_zone, struct ppa *ppa)
{
  // plane:blk:pg:lun:chnl
    struct ssdparams *spp = &ssd->sp;
    struct write_pointer *wpp = &pz->wp;

    ppa->g.ch = lpn_in_zone % spp->chnls_per_zone + wpp->ch_beg;
    lpn_in_zone /= spp->chnls_per_zone;
    ppa->g.lun = lpn_in_zone % spp->luns_per_chnl_per_zone + wpp->lun_beg;
    lpn_in_zone /= spp->luns_per_chnl_per_zone;
    ppa->g.pg = lpn_in_zone % spp->pgs_per_blk_per_zone;
    lpn_in_zone /= spp->pgs_per_blk_per_zone;
    ppa->g.blk = lpn_in_zone % spp->blks_per_pl_per_zone + wpp->blk_beg;
    lpn_in_zone /= spp->blks_per_pl_per_zone;
    ppa->g.pl = lpn_in_zone % spp->pls_per_lun_per_zone;
}

static void zns_init_params(struct ssdparams *spp)
{
    spp->secsz = 4096;
    spp->secs_per_pg = 1;
    spp->pgs_per_blk = 4096; // 16 MiB
    spp->blks_per_pl = 64; // 1 GiB
    spp->pls_per_lun = 1; 
    spp->luns_per_ch = 4; // 4 GiB
    spp->nchs = 4; // 16 GiB

    spp->pg_rd_lat = NAND_READ_LATENCY;
    spp->pg_wr_lat = NAND_PROG_LATENCY;
    spp->blk_er_lat = NAND_ERASE_LATENCY;
    spp->ch_xfer_lat = 0;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    // each physical zone maps to two luns in two different two channels
    // zone capacity = chnls_pr_zone * luns_per_zone * pgs_per_lun
    spp->pgs_per_blk_per_zone = spp->pgs_per_blk;
    spp->blks_per_pl_per_zone = 1;
    spp->pls_per_lun_per_zone = 1;
    spp->luns_per_chnl_per_zone = 4;
    spp->chnls_per_zone = 4;
//    ftl_assert(spp->nchnls % spp->
//    ftl_assert(spp->luns_per_chnl % spp->luns_per_zone == 0);
    spp->tt_pgs_per_zone = spp->chnls_per_zone *
            spp->luns_per_chnl_per_zone * spp->pls_per_lun_per_zone *
            spp->blks_per_pl_per_zone * spp->pgs_per_blk_per_zone;
    spp->zone_size = (uint64_t)spp->tt_pgs_per_zone * spp->secs_per_pg * spp->secsz;
    ftl_debug("tt_pgs_per_zone: %lu, zonesize: %lu\n", spp->tt_pgs_per_zone, spp->zone_size);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++) {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

void zns_ftl_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    zns_init_params(spp);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize all the lines */
    zns_init_zones(ssd);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
#if 0
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (ncmd->type == USER_IO) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        }
        lat = lun->next_lun_avail_time - cmd_stime;

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat;
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba;
    uint32_t nsecs = (uint32_t)le16_to_cpu(req->nlb);
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",end_lpn=%"PRIu64",nsecs=%d,tt_pgs=%d\n", start_lpn, end_lpn, nsecs, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        struct nand_cmd srd;
        uint64_t lzid = start_lpn / spp->tt_pgs_per_zone;
        struct physical_zone *pz = ssd->zone_mapping[lzid];
        get_ppa(ssd, pz, lpn % spp->tt_pgs_per_zone, &ppa);
//        printf("%lu,%lu %d:%d:%d:%d:%d\n", lpn, lzid, ppa.g.ch, ppa.g.lun, ppa.g.pl, ppa.g.blk, ppa.g.pg);
        
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;
    uint32_t nsecs = (uint32_t)le16_to_cpu(req->nlb);
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t curlat = 0, maxlat = 0;
    uint64_t lpn = 0;
    uint64_t lzid;
    struct ppa ppa;
    struct physical_zone *pz = NULL;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",end_lpn=%"PRIu64",nsecs=%d,tt_pgs=%d\n", start_lpn, end_lpn, nsecs, ssd->sp.tt_pgs);
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        /* need to advance the write pointer here */
        lzid = lpn / spp->tt_pgs_per_zone;
        pz = ssd->zone_mapping[lzid];
        physical_zone_advance_write_pointer(ssd, pz, &ppa);

        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

void zns_process_latency_emulation(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lat = 0;

    ftl_assert(req);

    switch (req->cmd.opcode) {
      case NVME_CMD_WRITE:
        lat = ssd_write(ssd, req);
        break;
      case NVME_CMD_READ:
        lat = ssd_read(ssd, req);
        break;
      case NVME_CMD_ZONE_APPEND:
        lat = ssd_write(ssd, req);
        break;
      case NVME_CMD_ZONE_MGMT_SEND:
        if (le32_to_cpu(req->cmd.cdw13) == NVME_ZONE_ACTION_RESET) {
          lat = req->reqlat; // generated already in zns_erase
        }
        break;
      case NVME_CMD_DSM:
        lat = 0;
        break;
      default:
        //ftl_err("ftl received unkown request type, error\n");
        ;
    }

    req->reqlat = lat;
    req->expire_time += lat;
}

void zns_zone_erase(struct ssd *ssd, NvmeRequest *req, NvmeZone *zone)
{
  uint32_t zslba = zone->d.zslba;
  struct physical_zone *pz = ssd->zone_mapping[zslba / ssd->sp.tt_pgs_per_zone];
  struct write_pointer *wpp = &pz->wp;
  struct ppa ppa;
  uint64_t curlat = 0, maxlat = 0;
  struct nand_cmd ncmd;
  ncmd.type = USER_IO;
  ncmd.cmd = NAND_ERASE;
  ncmd.stime = req->stime;

  for (uint32_t ch = wpp->ch_beg; ch < wpp->ch_end; ++ch) {
    ppa.g.ch = ch;
    for (uint32_t lun = wpp->lun_beg; lun < wpp->lun_end; ++lun) {
      ppa.g.lun = lun;
      for (uint32_t blk = wpp->blk_beg; blk < wpp->blk_end; ++blk) {
        ppa.g.blk = blk;
        curlat = ssd_advance_status(ssd, &ppa, &ncmd);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
      }
    }
  }
  zone_reset_write_pointer(wpp);

  req->reqlat = maxlat;
}


