-- 277953 rows 1380.25 sec
-- BETWEEN '201501' AND '201503'
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
           attribute12_value
        )
        /*---------------------------------------------------
           한계이익 구간별 매출액,한계이익,마케팅비용 생성
        ----------------------------------------------------*/
        WITH tempa( basis_yyyymm, subsdr_cd, div_cd, mgnl_prf_type_cd,
                    mgnl_prf_range_cd, oi_range_cd, scenario_type_cd, grade_name, old_new_cd, virt_mdl_flag_cd,mgnl_prf_mdl_cnt,
                    gross_sales_krw_amt, sales_deduct_krw_amt, nsales_krw_amt, mgnl_prf_krw_amt, oi_krw_amt,
                    gross_sales_usd_amt, sales_deduct_usd_amt, nsales_usd_amt, mgnl_prf_usd_amt, oi_usd_amt ) AS
        (
        --AC0
        SELECT  a11.acctg_yyyymm  basis_yyyymm,
                a11.subsdr_cd  subsdr_cd,
                a11.div_cd  div_cd,
                a11.accu6_loss_flag  mgnl_prf_type_cd,
                a11.mgnl_prf_range  mgnl_prf_range_cd,
                a11.oi_range  oi_range_cd,
                a11.scenario_type_cd scenario_type_cd,
                a18.attribute_name  grd_name,
                a12.old_new_cd  old_new_cd,
                CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE a11.virt_mdl_flag END  virt_mdl_flag_cd,
                sum(a11.mgnl_prf_mdl_cnt) mgnl_prf_mdl_cnt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.gross_sales_amt END)   gross_sales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.sales_deduct_amt END)  sales_deduct_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.nsales_amt END)        nsales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.mgnl_prf_amt END)      mgnl_prf_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.oi_amt END)            oi_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.gross_sales_amt END)   gross_sales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.sales_deduct_amt END)  sales_deduct_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.nsales_amt END)        nsales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.mgnl_prf_amt END)      mgnl_prf_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.oi_amt END)            oi_usd_amt
        FROM    npt_app.nv_dww_bep_mgnl_prf_mdl_s  a11
        LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a12
        ON     (a11.acctg_yyyymm = a12.acctg_yyyymm
        AND     a11.mdl_sffx_cd = a12.mdl_sffx_cd
        AND     a11.subsdr_cd = a12.subsdr_cd)
        LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18
        ON     (a12.grd_cd = a18.attribute_cd)
        WHERE  (a11.acctg_yyyymm BETWEEN '201501' AND '201503'
        AND     a11.consld_sales_mdl_flag IN ('Y')
        AND     a11.vrnc_alc_incl_excl_cd IN ('INCL')
        AND     a11.currency_cd IN ('KRW','USD')
        AND     a11.scenario_type_cd IN ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
        AND     a11.div_cd not IN (SELECT cd_id FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND use_flag = 'Y'))
        GROUP BY  a11.acctg_yyyymm  ,
                a11.subsdr_cd,
                a11.div_cd  ,
                a11.accu6_loss_flag  ,
                a11.mgnl_prf_range  ,
                a11.oi_range  ,
                a11.scenario_type_cd,
                a18.attribute_name  ,
                a12.old_new_cd  ,
                  CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE a11.virt_mdl_flag END
        UNION ALL
        SELECT  a11.acctg_yyyymm  basis_yyyymm,
                a11.subsdr_cd,
                a11.div_cd  div_cd,
                a11.accu6_loss_flag  mgnl_prf_type_cd,
                a11.mgnl_prf_range  mgnl_prf_range_cd,
                a11.oi_range  oi_range_cd,
                a11.scenario_type_cd scenario_type_cd,
                a18.attribute_name  grd_name,
                a12.old_new_cd  old_new_cd,
                CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE a11.virt_mdl_flag END  virt_mdl_flag_cd,
                sum(a11.mgnl_prf_mdl_cnt) mgnl_prf_mdl_cnt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.gross_sales_amt END)   gross_sales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.sales_deduct_amt END)  sales_deduct_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.nsales_amt END)        nsales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.mgnl_prf_amt END)      mgnl_prf_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.oi_amt END)            oi_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.gross_sales_amt END)   gross_sales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.sales_deduct_amt END)  sales_deduct_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.nsales_amt END)        nsales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.mgnl_prf_amt END)      mgnl_prf_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.oi_amt END)            oi_usd_amt
        FROM    npt_app.nv_dww_bep_mgnl_prf_mdl_s  a11
        LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a12
        ON     (a11.acctg_yyyymm = a12.acctg_yyyymm
        AND     a11.mdl_sffx_cd = a12.mdl_sffx_cd
        AND     a11.subsdr_cd = a12.subsdr_cd)
        LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18
        ON     (a12.grd_cd = a18.attribute_cd)
        WHERE  (a11.acctg_yyyymm BETWEEN '201501' AND '201503'
        AND     a11.consld_sales_mdl_flag IN ('Y', 'N', '*')
        AND     a11.vrnc_alc_incl_excl_cd IN ('INCL')
        AND     a11.currency_cd IN ('KRW','USD')
        AND     A11.div_cd IN (SELECT cd_id FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND use_flag = 'Y')
        AND     a11.scenario_type_cd IN ('AC0', 'PR1', 'PR2', 'PR3', 'PR4'))
        GROUP BY  a11.acctg_yyyymm  ,
                a11.subsdr_cd,
                a11.div_cd  ,
                a11.accu6_loss_flag  ,
                a11.mgnl_prf_range  ,
                a11.oi_range  ,
                a11.scenario_type_cd,
                a18.attribute_name  ,
                a12.old_new_cd  ,
                CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE a11.virt_mdl_flag END
        UNION ALL
        /* PR1 */
        SELECT  a11.acctg_yyyymm  basis_yyyymm,
                a11.subsdr_cd,
                a11.div_cd  div_cd,
                a11.accu6_loss_flag  mgnl_prf_type_cd,
                a11.mgnl_prf_range  mgnl_prf_range_cd,
                a11.oi_range  oi_range_cd,
                a11.scenario_type_cd scenario_type_cd,
                a18.attribute_name  grd_name,
                a12.old_new_cd  old_new_cd,
                CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE DECODE(a11.virt_mdl_flag,'*','N') END  virt_mdl_flag_cd,
                sum(a11.mgnl_prf_mdl_cnt) mgnl_prf_mdl_cnt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.gross_sales_amt END)   gross_sales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.sales_deduct_amt END)  sales_deduct_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.nsales_amt END)        nsales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.mgnl_prf_amt END)      mgnl_prf_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.oi_amt END)            oi_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.gross_sales_amt END)   gross_sales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.sales_deduct_amt END)  sales_deduct_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.nsales_amt END)        nsales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.mgnl_prf_amt END)      mgnl_prf_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.oi_amt END)            oi_usd_amt
        FROM    npt_app.nv_dww_bep_mgnl_prf_mdl_s  a11
        LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a12
        ON     (a11.acctg_yyyymm = a12.acctg_yyyymm
        AND     a11.mdl_sffx_cd = a12.mdl_sffx_cd
        AND     a11.subsdr_cd = a12.subsdr_cd)
        LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18
        ON     (a12.grd_cd = a18.attribute_cd)
        WHERE  (a11.acctg_yyyymm BETWEEN '201501' AND '201503'
        AND     a11.consld_sales_mdl_flag IN ('Y')
        AND     a11.vrnc_alc_incl_excl_cd IN ('INCL')
        AND     a11.currency_cd IN ('KRW','USD')
        AND     a11.scenario_type_cd IN ('PR1','PR2','PR3','PR4')
        AND     a11.div_cd NOT IN (SELECT cd_id FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND use_flag = 'Y'))
        GROUP BY a11.acctg_yyyymm ,
                 a11.subsdr_cd,
                 a11.div_cd  ,
                 a11.accu6_loss_flag  ,
                 a11.mgnl_prf_range  ,
                 a11.oi_range  ,
                 a11.scenario_type_cd,
                 a18.attribute_name  ,
                 a12.old_new_cd  ,
                 CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE DECODE(a11.virt_mdl_flag,'*','N') END
        UNION ALL
        SELECT  a11.acctg_yyyymm  basis_yyyymm,
                a11.subsdr_cd,
                a11.div_cd  div_cd,
                a11.accu6_loss_flag  mgnl_prf_type_cd,
                a11.mgnl_prf_range  mgnl_prf_range_cd,
                a11.oi_range  oi_range_cd,
                a11.scenario_type_cd scenario_type_cd,
                a18.attribute_name  grd_name,
                a12.old_new_cd  old_new_cd,
                CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE DECODE(a11.virt_mdl_flag,'*','N') END  virt_mdl_flag_cd,
                sum(a11.mgnl_prf_mdl_cnt) mgnl_prf_mdl_cnt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.gross_sales_amt END)   gross_sales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.sales_deduct_amt END)  sales_deduct_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.nsales_amt END)        nsales_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.mgnl_prf_amt END)      mgnl_prf_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'KRW' THEN a11.oi_amt END)            oi_krw_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.gross_sales_amt END)   gross_sales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.sales_deduct_amt END)  sales_deduct_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.nsales_amt END)        nsales_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.mgnl_prf_amt END)      mgnl_prf_usd_amt,
                SUM(CASE WHEN a11.currency_cd = 'USD' THEN a11.oi_amt END)            oi_usd_amt
        FROM    npt_app.nv_dww_bep_mgnl_prf_mdl_s  a11
        LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a12
        ON     (a11.acctg_yyyymm = a12.acctg_yyyymm
        AND     a11.mdl_sffx_cd = a12.mdl_sffx_cd
        AND     a11.subsdr_cd = a12.subsdr_cd)
        LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18
        ON     (a12.grd_cd = a18.attribute_cd)
        WHERE  (a11.acctg_yyyymm BETWEEN '201501' AND '201503'
        AND     a11.consld_sales_mdl_flag IN ('Y', 'N', '*')
        AND     a11.vrnc_alc_incl_excl_cd IN ('INCL')
        AND     a11.currency_cd IN ('KRW','USD')
        AND     A11.div_cd IN (SELECT cd_id FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y')
        AND     a11.scenario_type_cd IN ('PR1','PR2','PR3','PR4'))
        GROUP BY a11.acctg_yyyymm ,
                 a11.subsdr_cd,
                 a11.div_cd  ,
                 a11.accu6_loss_flag  ,
                 a11.mgnl_prf_range  ,
                 a11.oi_range  ,
                 a11.scenario_type_cd,
                 a18.attribute_name  ,
                 a12.old_new_cd  ,
                 CASE WHEN a11.mdl_sffx_cd like 'VM-%.CPS' THEN 'Y' ELSE DECODE(a11.virt_mdl_flag,'*','N') END
        )

        /*---------------------------------------------------
           한계이익 구간별 매출액,한계이익,마케팅비용 생성
        ----------------------------------------------------*/
        SELECT '1550'                                                          AS prcs_seq
              ,'ARES'                                                          AS rs_module_cd
              ,'BEP_SMART'                                                     AS rs_clsf_id
              ,'BEP_SMART_SUBSDR'                                                     AS rs_type_cd
              ,'BEP_SMART_SUBSDR'                                                     AS rs_type_name
              ,mgnl.div_cd                                                     AS div_cd
              ,mgnl.basis_yyyymm                                               AS base_yyyymmdd
              ,NULL                                                            AS cd_desc
              ,NULL                                                            AS sort_seq
              ,'Y'                                                             AS use_flag ,
               mgnl.basis_yyyymm                                               AS attribute1,
               mgnl.scenario_type_cd                                           AS attribute2,
               mgnl.subsdr_cd                                                  AS attribute3,
               mgnl.div_cd                                                     AS attribute4,
               'N'                                                             AS attribute5,
                c2.cd_id                                                       AS attribute6,
               CASE WHEN c2.cd_id IN ('SALE','MGN_PROFIT','MODEL_COUNT') THEN
                   CASE mgnl.mgnl_prf_range_cd
                      WHEN '30%~'    THEN 'MARGINAL_PF_30'
                      WHEN '20%~30%' THEN 'MARGINAL_PF_20_30'
                      WHEN '13%~20%' THEN 'MARGINAL_PF_10_20'
                      WHEN '10%~13%' THEN 'MARGINAL_PF_10_20'
                      WHEN '5%~10%'  THEN 'MARGINAL_PF_5_10'
                      WHEN '0%~5%'   THEN 'MARGINAL_PF_0_5'
                      WHEN '~0%'      THEN 'MARGINAL_PF_(-)'
                      ELSE mgnl.mgnl_prf_range_cd
                    END
               END                                                             AS attribute7,
               MIN(
               CASE mgnl.scenario_type_cd
                    WHEN 'AC0' THEN basis_yyyymm
                    WHEN 'PR1' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                    WHEN 'PR2' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                    WHEN 'PR3' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                    WHEN 'PR4' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 4), 'YYYYMM')
               END)                                                            AS attribute8,
               SUM(CASE WHEN c2.cd_id = 'SALE'  THEN
                         mgnl.nsales_krw_amt
                    WHEN c2.cd_id = 'COI' THEN
                         mgnl.oi_krw_amt
                    WHEN c2.cd_id = 'MGN_PROFIT' THEN
                         mgnl.mgnl_prf_krw_amt
                    WHEN c2.cd_id = 'MODEL_COUNT' THEN
                         mgnl.mgnl_prf_mdl_cnt
                    ELSE 0
               END)                                                            AS attribute9,
               SUM(CASE WHEN c2.cd_id = 'SALE'  THEN
                         mgnl.nsales_usd_amt
                    WHEN c2.cd_id = 'COI' THEN
                         mgnl.oi_usd_amt
                    WHEN c2.cd_id = 'MGN_PROFIT' THEN
                         mgnl.mgnl_prf_usd_amt
                    WHEN c2.cd_id = 'MODEL_COUNT' THEN
                         mgnl.mgnl_prf_mdl_cnt
                    ELSE 0
               END)                                                            AS attribute10,
               NULL                                                            AS attribute11,
               NULL                                                            AS attribute12
       FROM   tempa mgnl
             ,npt_rs_mgr.tb_rs_clss_cd_m C2
       WHERE  C2.cd_clsf_id = 'KPI_TYPE'
       --AND    C2.cd_id IN ('SALE','COI','MGN_PROFIT')
       AND    C2.cd_id IN ('SALE','MGN_PROFIT','MODEL_COUNT') -- 한게이익 구간대별 매출, 한계이익
       AND    mgnl.virt_mdl_flag_cd = 'N'
       GROUP BY
             mgnl.basis_yyyymm,
             mgnl.scenario_type_cd,
             mgnl.subsdr_cd,
             mgnl.div_cd,
             c2.cd_id,
             CASE WHEN c2.cd_id IN ('SALE','MGN_PROFIT','MODEL_COUNT') THEN
                 CASE mgnl.mgnl_prf_range_cd
                    WHEN '30%~'    THEN 'MARGINAL_PF_30'
                    WHEN '20%~30%' THEN 'MARGINAL_PF_20_30'
                    WHEN '13%~20%' THEN 'MARGINAL_PF_10_20'
                    WHEN '10%~13%' THEN 'MARGINAL_PF_10_20'
                    WHEN '5%~10%'  THEN 'MARGINAL_PF_5_10'
                    WHEN '0%~5%'   THEN 'MARGINAL_PF_0_5'
                    WHEN '~0%'      THEN 'MARGINAL_PF_(-)'
                    ELSE mgnl.mgnl_prf_range_cd
                  END
             END

        /*---------------------------------------------------
           영업이익 구간별 매출액,한계이익
        ----------------------------------------------------*/
        UNION ALL

        SELECT '1550'                                                          AS prcs_seq
              ,'ARES'                                                          AS rs_module_cd
              ,'BEP_SMART'                                                     AS rs_clsf_id
              ,'BEP_SMART_SUBSDR'                                                     AS rs_type_cd
              ,'BEP_SMART_SUBSDR'                                                     AS rs_type_name
              ,mgnl.div_cd                                                     AS div_cd
              ,mgnl.basis_yyyymm                                               AS base_yyyymmdd
              ,NULL                                                            AS cd_desc
              ,NULL                                                            AS sort_seq
              ,'Y'                                                             AS use_flag ,
               mgnl.basis_yyyymm,
               mgnl.scenario_type_cd scenario_code,
               mgnl.subsdr_cd,
               mgnl.div_cd division_code,
               'N'         AS manual_adjust_flag,
                c2.cd_id   AS kpi_type_code,
               CASE WHEN c2.cd_id IN ('SALE','COI','MGN_PROFIT','MODEL_COUNT') THEN
                   CASE mgnl.oi_range_cd
                      WHEN '10%~'     THEN 'COI_10'
                      WHEN '0%~10%'   THEN 'COI_0_10'
                      WHEN '-5%~0%'   THEN 'COI_-5_0'
                      WHEN '-10%~-5%' THEN 'COI_-10_-5'
                      WHEN '-15%~-10%'  THEN 'COI_-15_-10'
                      WHEN '~-15%'      THEN 'COI_-15'
                      ELSE NULL
                   END
               END AS category_detail_code,
               MIN(
               CASE mgnl.scenario_type_cd
                    WHEN 'AC0' THEN basis_yyyymm
                    WHEN 'PR1' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                    WHEN 'PR2' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                    WHEN 'PR3' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                    WHEN 'PR4' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 4), 'YYYYMM')
               END) AS YYYYMM,
               SUM(CASE WHEN c2.cd_id = 'SALE'  THEN
                         mgnl.nsales_krw_amt
                    WHEN c2.cd_id = 'COI' THEN
                         mgnl.oi_krw_amt
                    WHEN c2.cd_id = 'MGN_PROFIT' THEN
                         mgnl.mgnl_prf_krw_amt
                    WHEN c2.cd_id = 'MODEL_COUNT' THEN
                         mgnl.mgnl_prf_mdl_cnt
                    ELSE 0
               END) AS curr_mon_krw_amount,
               SUM(CASE WHEN c2.cd_id = 'SALE'  THEN
                         mgnl.nsales_usd_amt
                    WHEN c2.cd_id = 'COI' THEN
                         mgnl.oi_usd_amt
                    WHEN c2.cd_id = 'MGN_PROFIT' THEN
                         mgnl.mgnl_prf_usd_amt
                    WHEN c2.cd_id = 'MODEL_COUNT' THEN
                         mgnl.mgnl_prf_mdl_cnt
                    ELSE 0
               END) AS curr_mon_usd_amount,
               NULL accu_mon_krw_amount,
               NULL accu_mon_usd_amount
       FROM   tempa mgnl
             ,npt_rs_mgr.tb_rs_clss_cd_m C2
       WHERE  C2.cd_clsf_id = 'KPI_TYPE'
       AND    C2.cd_id IN ('SALE','COI','MGN_PROFIT','MODEL_COUNT')
       AND    mgnl.virt_mdl_flag_cd = 'N'
       --AND    mgnl.scenario_type_cd IN ('PR2','PR3','PR4')
       GROUP BY
             mgnl.basis_yyyymm,
             mgnl.scenario_type_cd,
             mgnl.subsdr_cd,
             mgnl.div_cd,
             c2.cd_id,
             CASE WHEN c2.cd_id IN ('SALE','COI','MGN_PROFIT','MODEL_COUNT') THEN
                 CASE mgnl.oi_range_cd
                    WHEN '10%~'     THEN 'COI_10'
                    WHEN '0%~10%'   THEN 'COI_0_10'
                    WHEN '-5%~0%'   THEN 'COI_-5_0'
                    WHEN '-10%~-5%' THEN 'COI_-10_-5'
                    WHEN '-15%~-10%'  THEN 'COI_-15_-10'
                    WHEN '~-15%'      THEN 'COI_-15'
                    ELSE NULL
                 END
             END
          ;                                                                                                                  
              
