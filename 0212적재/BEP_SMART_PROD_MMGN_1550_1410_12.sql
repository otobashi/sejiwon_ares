-- 11685 rows 170 sec
-- 11724 rows 862.811 sec
-- BETWEEN '201410' AND '201412'
-- BEP_SMART_PROD_MMGN
        INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
        (
           prcs_seq,
           rs_module_cd,
           rs_clsf_id,
           rs_type_cd,
           rs_type_name,
           div_cd,
           base_yyyymmdd,
           cd_desc,
           sort_seq,
           use_flag,
           attribute1_value,
           attribute2_value,
           attribute3_value,
           attribute4_value,
           attribute5_value,
           attribute6_value,
           attribute7_value,
           attribute8_value,
           attribute9_value,
           attribute10_value,
           attribute11_value,
           attribute12_value,
           attribute13_value,
           attribute14_value,
           attribute15_value,
           attribute16_value
        )
        SELECT /*+ PARALLEL(8)  USE_HASH(A11 A12 A13 A14 A15 A16 A17 A18 A19 A110 A111 A112) */ 
               '1550'                                                          AS prcs_seq
               ,'ARES'                                                         AS rs_module_cd
               ,'BEP_SMART'                                                    AS rs_clsf_id
               ,'BEP_SMART_PROD_MMGN'                                                    AS rs_type_cd
               ,'BEP_SMART_PROD_MMGN'                                                    AS rs_type_name
               ,a11.div_cd                                                     AS div_cd
               ,a11.acctg_yyyymm                                               AS base_yyyymmdd
               ,NULL                                                           AS cd_desc
               ,NULL                                                           AS sort_seq
               ,'Y'                                                            AS use_flag
               ,a11.acctg_yyyymm           -- base_yyyymm,
               ,a17.up_prod_cd             -- prod_cd,
               ,a115.prod_eng_name         -- prod_eng_name,
               ,a16.up_prod_cd             -- prod_cd0,
               ,a17.prod_eng_name          -- prod_eng_name0,
               ,a15.up_prod_cd             -- prod_cd1,
               ,a16.prod_eng_name          -- prod_eng_name1,
               ,a14.usr_prod1_last_cd      -- prod_cd2,
               ,a15.prod_eng_name          -- prod_eng_name2,
               ,a11.scenario_type_cd       -- scenario_type_cd,
               ,a11.div_cd                 -- div_cd,
               ,a13.div_shrt_name          -- div_shrt_name0,
               ,a112.mgt_org_shrt_name     -- mgt_org_shrt_name,
               ,a11.subsdr_cd              -- subsdr_cd1,
               ,sum(a11.mgnl_prf_mdl_cnt)  -- mgnl_prf_mdl_cnt,
               ,sum(a11.mgnl_prf_amt)      -- mgnl_prf_amt
        /*
          sum(a11.mgnl_prf_mdl_cnt)  wjxbfs1,
          sum(a11.sales_qty)  wjxbfs2,
          sum(a11.gross_sales_amt)  wjxbfs3,
          sum(a11.sales_deduct_amt)  wjxbfs4,
          sum(a11.nsales_amt)  wjxbfs5,
          sum(a11.mgnl_prf_amt)  wjxbfs6,
          sum(a11.oi_amt)  wjxbfs7
        */
        FROM  npt_app.nv_dww_bep_mgnl_prf_mdl_s a11
              LEFT OUTER JOIN npt_app.nv_dwd_zone_m a12
              ON  (a11.zone_rnr_cd = a12.zone_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a13
              ON  (a11.div_cd = a13.div_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mdl_period_h  a14
              ON  (a11.mdl_sffx_cd = a14.mdl_sffx_cd 
              AND  a11.subsdr_cd = a14.subsdr_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod4_m  a15
              ON  (a14.usr_prod1_last_cd = a15.prod_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod3_m  a16
              ON  (a15.up_prod_cd = a16.prod_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod2_m  a17
              ON  (a16.up_prod_cd = a17.prod_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_loss_flag_m  a18
              ON  (a11.loss_flag = a18.loss_flag_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_rhq_m  a19
              ON  (a12.rhq_cd = a19.rhq_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_scenario_type_m  a110
              ON  (a11.scenario_type_cd = a110.scenario_type_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a111
              ON  (a11.subsdr_cd = a111.subsdr_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_mgt_org_rnr_m  a112
              ON  (a11.sales_subsdr_rnr_cd = a112.mgt_org_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_div1_m  a113
              ON  (a13.div1_cd = a113.div1_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_disable_type_m  a114
              ON  (a11.disable_type_cd = a114.disable_type_cd)
              LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod1_m  a115
              ON  (a17.up_prod_cd = a115.prod_cd)
         WHERE (a11.scenario_type_cd IN ('AC0')
         AND   a11.acctg_yyyymm BETWEEN '201410' AND '201412'
         AND   a11.vrnc_alc_incl_excl_cd IN ('INCL')
         AND   a11.currency_cd IN ('USD'))
         AND   a11.loss_flag = 'Y'
         GROUP BY a11.acctg_yyyymm
                 ,a17.up_prod_cd
                 ,a115.prod_eng_name
                 ,a16.up_prod_cd
                 ,a17.prod_eng_name
                 ,a15.up_prod_cd
                 ,a16.prod_eng_name
                 ,a14.usr_prod1_last_cd
                 ,a15.prod_eng_name
                 ,a11.scenario_type_cd
                 ,a11.div_cd
                 ,a13.div_shrt_name
                 ,a112.mgt_org_shrt_name
                 ,a11.subsdr_cd
          ;                                                                                                                  
               
