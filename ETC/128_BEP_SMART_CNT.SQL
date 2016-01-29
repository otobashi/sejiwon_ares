
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
              attribute16_value,
              attribute17_value,
              attribute18_value,
              attribute19_value,
              attribute20_value,
              attribute21_value,
              attribute22_value,
              attribute23_value
           )
              SELECT  /*+ use_hash(a11 a12 a13 a14 a15 a16 a17 a18 a19 a110 a111 a112 a113 a114 a115 a116 a117 a118 a119 a120 a121 a122 a123 a124 a125 a126 a127 a128 a129 a130 a131 a132 a133 a134 a135 a136 a137 a138 a139 a140) */
                    '1600'                                                            --prcs_seq
                    ,'ARES'                                                           --rs_module_cd
                    ,'BEP_SMART'                                                      --rs_clsf_id
                    ,'BEP_SMART_CNT'                                                      --rs_type_cd
                    ,'BEP_SMART_CNT'                                                      --rs_type_name
                    ,DECODE(a13.up_prod_cd,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')     --div_cd
                    ,a11.acctg_yyyymm                                                 --base_yyyymmdd
                    ,a112.prod_eng_name                                               --cd_desc
                    ,a111.scrn_dspl_seq                                               --sort_seq
                    ,'Y'                                                              --use_flag
                    ,a11.scenario_type_cd                                             --scenario_type_cd
                    ,DECODE(a13.up_prod_cd,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')     --div_cd
                    ,a11.subsdr_cd                                                    --subsdr_cd
                    ,a19.subsdr_shrt_name                                             --new_subsdr_shrt_name
                    ,a11.zone_rnr_cd                                                  --zone_cd
                    ,a111.zone_name                                                   --zone_name
                    ,a111.scrn_dspl_seq                                               --scrn_dspl_seq
                    ,a11.cntry_rnr_cd                                                 --cntry_rnr_cd
                    ,a15.cntry_name                                                   --cntry_name
                    ,a11.acctg_yyyymm                                                 --base_yyyymm
                    ,a11.pln_yyyymm                                                   --plan_yyyymm
                    ,a13.up_prod_cd                                                   --prod_cd
                    ,a112.prod_eng_name                                               --prod_eng_name
                    ,SUM(DECODE(a11.currency_cd,'USD',a11.nsales_amt))                --nsales_usd_amt
                    ,SUM(DECODE(a11.currency_cd,'KRW',a11.nsales_amt))                --nsales_krw_amt
                    ,SUM(DECODE(a11.currency_cd,'USD',a11.sales_deduct_amt))          --sales_deduct_usd_amt
                    ,SUM(DECODE(a11.currency_cd,'KRW',a11.sales_deduct_amt))          --sales_deduct_krw_amt
                    ,SUM(DECODE(a11.currency_cd,'USD',a11.oth_sales_amt))             --oth_sales_usd_amt
                    ,SUM(DECODE(a11.currency_cd,'KRW',a11.oth_sales_amt))             --oth_sales_krw_amt
                    ,SUM(DECODE(a11.currency_cd,'USD',a11.mgnl_prf_amt))              --mgnl_prf_usd_amt
                    ,SUM(DECODE(a11.currency_cd,'KRW',a11.mgnl_prf_amt))              --mgnl_prf_krw_amt
                    ,SUM(DECODE(a11.currency_cd,'USD',a11.oi_amt))                    --coi_usd_amt
                    ,SUM(DECODE(a11.currency_cd,'KRW',a11.oi_amt))                    --coi_krw_amt
              FROM  npt_app.nv_dww_con_bep_summ_dw_s  a11
                    LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mdl_period_h  a12
                    ON (a11.mdl_sffx_cd = a12.mdl_sffx_cd
                    AND a11.subsdr_cd = a12.subsdr_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod4_m  a13
                    ON (a12.usr_prod1_last_cd = a13.prod_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_prft_confm_scenario_h  a14
                    ON (a11.acctg_yyyymm = a14.acctg_yyyymm
                    AND a11.div_cd = a14.div_cd
                    AND a11.scenario_type_cd = a14.scenario_type_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_cntry_m  a15
                    ON (a11.cntry_rnr_cd = a15.cntry_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a16
                    ON (a11.div_cd = a16.div_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a17
                    ON (a11.production_subsdr_cd = a17.subsdr_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_scenario_type_m  a18
                    ON (a11.scenario_type_cd = a18.scenario_type_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a19
                    ON (a11.subsdr_cd = a19.subsdr_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_mgt_org_rnr_m  a110
                    ON (a11.sales_subsdr_rnr_cd = a110.mgt_org_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_zone_m  a111
                    ON (a11.zone_rnr_cd = a111.zone_cd)
                    LEFT OUTER JOIN  npt_app.nv_dwd_rpt_prod3_m  a112
                    ON (a13.up_prod_cd = a112.prod_cd)
              WHERE (a11.acctg_yyyymm          BETWEEN '201301' AND '201512'
              AND    a11.div_cd                IN ('GNT')
              AND    a11.consld_sales_mdl_flag IN ('Y')
              AND    a11.currm_accum_type_cd   IN ('CURRM')
              AND    a11.vrnc_alc_incl_excl_cd IN ('INCL')
              AND    a11.currency_cd           IN ('USD','KRW')
              AND    a14.confirm_flag          = 'Y')
              GROUP BY a11.scenario_type_cd
                      ,decode(a13.up_prod_cd,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')
                      ,a11.subsdr_cd
                      ,a19.subsdr_shrt_name
                      ,a11.zone_rnr_cd
                      ,a111.zone_name
                      ,a111.scrn_dspl_seq
                      ,a11.cntry_rnr_cd
                      ,a15.cntry_name
                      ,a11.acctg_yyyymm
                      ,a11.pln_yyyymm
                      ,a13.up_prod_cd
                      ,a112.prod_eng_name
