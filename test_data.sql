        /*---------------------------------------------------
           한계이익 구간별 매출액,한계이익,마케팅비용 생성
        ----------------------------------------------------*/
            WITH TEMPA( BASIS_YYYYMM, SUBSDR_CD, DIV_CD, MGNL_PRF_TYPE_CD,
                        MGNL_PRF_RANGE_CD, OI_RANGE_CD, SCENARIO_TYPE_CD, GRADE_NAME, OLD_NEW_CD, VIRT_MDL_FLAG_CD,MGNL_PRF_MDL_CNT,
                        GROSS_SALES_KRW_AMT, SALES_DEDUCT_KRW_AMT, NSALES_KRW_AMT, MGNL_PRF_KRW_AMT, OI_KRW_AMT,
                        GROSS_SALES_USD_AMT, SALES_DEDUCT_USD_AMT, NSALES_USD_AMT, MGNL_PRF_USD_AMT, OI_USD_AMT ) AS
            (
            --AC0
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.SUBSDR_CD  SUBSDR_CD,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end  VIRT_MDL_FLAG_CD,
                    sum(a11.MGNL_PRF_MDL_CNT) MGNL_PRF_MDL_CNT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.NSALES_AMT END)        NSALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.OI_AMT END)            OI_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.NSALES_AMT END)        NSALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.OI_AMT END)            OI_USD_AMT
            FROM    NPT_APP.NV_DWW_BEP_MGNL_PRF_MDL_S  a11
            LEFT OUTER JOIN  NPT_DW_MGR.TB_DWD_SUBSDR_MDL_PERIOD_H  a12
            ON     (a11.ACCTG_YYYYMM = a12.ACCTG_YYYYMM
            AND     a11.MDL_SFFX_CD = a12.MDL_SFFX_CD
            AND     a11.SUBSDR_CD = a12.SUBSDR_CD)
            LEFT OUTER JOIN  NPT_APP.NV_DWD_02_GRD_CD  a18
            ON     (a12.GRD_CD = a18.ATTRIBUTE_CD)
            WHERE  (a11.ACCTG_YYYYMM BETWEEN '201301' AND '201601'
            and     a11.SUBSDR_CD = 'EEUK'
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
            AND     A11.DIV_CD NOT IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y'))
            GROUP BY  a11.ACCTG_YYYYMM  ,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                      case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end
            UNION ALL
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end  VIRT_MDL_FLAG_CD,
                    sum(a11.MGNL_PRF_MDL_CNT) MGNL_PRF_MDL_CNT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.NSALES_AMT END)        NSALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.OI_AMT END)            OI_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.NSALES_AMT END)        NSALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.OI_AMT END)            OI_USD_AMT
            FROM    NPT_APP.NV_DWW_BEP_MGNL_PRF_MDL_S  a11
            LEFT OUTER JOIN  NPT_DW_MGR.TB_DWD_SUBSDR_MDL_PERIOD_H  a12
            ON     (a11.ACCTG_YYYYMM = a12.ACCTG_YYYYMM
            AND     a11.MDL_SFFX_CD = a12.MDL_SFFX_CD
            AND     a11.SUBSDR_CD = a12.SUBSDR_CD)
            LEFT OUTER JOIN  NPT_APP.NV_DWD_02_GRD_CD  a18
            ON     (a12.GRD_CD = a18.ATTRIBUTE_CD)
            WHERE  (a11.ACCTG_YYYYMM BETWEEN '201301' AND '201601'
            and     a11.SUBSDR_CD = 'EEUK'
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y', 'N', '*')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     A11.DIV_CD IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y')
            AND     a11.SCENARIO_TYPE_CD in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4'))
            GROUP BY  a11.ACCTG_YYYYMM  ,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end
            UNION ALL
            /* PR1 */
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end  VIRT_MDL_FLAG_CD,
                    sum(a11.MGNL_PRF_MDL_CNT) MGNL_PRF_MDL_CNT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.NSALES_AMT END)        NSALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.OI_AMT END)            OI_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.NSALES_AMT END)        NSALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.OI_AMT END)            OI_USD_AMT
            FROM    NPT_APP.NV_DWW_BEP_MGNL_PRF_MDL_S  a11
            LEFT OUTER JOIN  NPT_DW_MGR.TB_DWD_SUBSDR_MDL_PERIOD_H  a12
            ON     (a11.ACCTG_YYYYMM = a12.ACCTG_YYYYMM
            AND     a11.MDL_SFFX_CD = a12.MDL_SFFX_CD
            AND     a11.SUBSDR_CD = a12.SUBSDR_CD)
            LEFT OUTER JOIN  NPT_APP.NV_DWD_02_GRD_CD  a18
            ON     (a12.GRD_CD = a18.ATTRIBUTE_CD)
            WHERE  (a11.ACCTG_YYYYMM BETWEEN '201301' AND '201601'
            and     a11.SUBSDR_CD = 'EEUK'
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4')
            AND     A11.DIV_CD NOT IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y'))
            GROUP BY  a11.ACCTG_YYYYMM ,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                      case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end
            UNION ALL
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end  VIRT_MDL_FLAG_CD,
                    sum(a11.MGNL_PRF_MDL_CNT) MGNL_PRF_MDL_CNT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.NSALES_AMT END)        NSALES_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'KRW' THEN a11.OI_AMT END)            OI_KRW_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.GROSS_SALES_AMT END)   GROSS_SALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.SALES_DEDUCT_AMT END)  SALES_DEDUCT_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.NSALES_AMT END)        NSALES_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.MGNL_PRF_AMT END)      MGNL_PRF_USD_AMT,
                    sum(CASE WHEN a11.CURRENCY_CD = 'USD' THEN a11.OI_AMT END)            OI_USD_AMT
            FROM    NPT_APP.NV_DWW_BEP_MGNL_PRF_MDL_S  a11
            LEFT OUTER JOIN  NPT_DW_MGR.TB_DWD_SUBSDR_MDL_PERIOD_H  a12
            ON     (a11.ACCTG_YYYYMM = a12.ACCTG_YYYYMM
            AND     a11.MDL_SFFX_CD = a12.MDL_SFFX_CD
            AND     a11.SUBSDR_CD = a12.SUBSDR_CD)
            LEFT OUTER JOIN  NPT_APP.NV_DWD_02_GRD_CD  a18
            ON     (a12.GRD_CD = a18.ATTRIBUTE_CD)
            WHERE  (a11.ACCTG_YYYYMM BETWEEN '201301' AND '201601'
            and     a11.SUBSDR_CD = 'EEUK'
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y', 'N', '*')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     A11.DIV_CD IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4'))
            GROUP BY a11.ACCTG_YYYYMM ,
                    a11.SUBSDR_CD,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end
            )

        /*---------------------------------------------------
           한계이익 구간별 매출액,한계이익,마케팅비용 생성
        ----------------------------------------------------*/
SELECT '1550'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_SUBSDR'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_SUBSDR'                                                  AS RS_TYPE_NAME                             
      ,mgnl.div_cd                                                 AS DIV_CD                                   
      ,mgnl.BASIS_YYYYMM                                                  AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG ,                                
                    mgnl.BASIS_YYYYMM AS ATTRIBUTE1,
                    mgnl.scenario_type_cd AS ATTRIBUTE2,
                    mgnl.SUBSDR_CD AS ATTRIBUTE3,
                    mgnl.div_cd AS ATTRIBUTE4,
                    'N'         AS ATTRIBUTE5,
                     c2.cd_id   AS ATTRIBUTE6,
                    case when c2.cd_id in ('SALE','MGN_PROFIT','MODEL_COUNT') then
                        case mgnl.MGNL_PRF_RANGE_CD
                           when '30%~'    then 'MARGINAL_PF_30'
                           when '20%~30%' then 'MARGINAL_PF_20_30'
                           when '13%~20%' then 'MARGINAL_PF_10_20'
                           when '10%~13%' then 'MARGINAL_PF_10_20'
                           when '5%~10%'  then 'MARGINAL_PF_5_10'
                           when '0%~5%'   then 'MARGINAL_PF_0_5'
                           when '~0%'      then 'MARGINAL_PF_(-)'
                           else mgnl.MGNL_PRF_RANGE_CD
                         end
                    end as ATTRIBUTE7,
                    MIN(
                    CASE mgnl.scenario_type_cd
                         WHEN 'AC0' THEN basis_yyyymm
                         WHEN 'PR1' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                         WHEN 'PR2' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                         WHEN 'PR3' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                         WHEN 'PR4' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 4), 'YYYYMM')
                    END) as ATTRIBUTE8,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_KRW_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_KRW_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_KRW_AMT
                         when c2.cd_id = 'MODEL_COUNT' then
                              mgnl.MGNL_PRF_MDL_CNT
                         else 0
                    end) as ATTRIBUTE9,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         when c2.cd_id = 'MODEL_COUNT' then
                              mgnl.MGNL_PRF_MDL_CNT
                         else 0
                    end) as ATTRIBUTE10,
                    null AS ATTRIBUTE11,
                    null AS ATTRIBUTE12
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            --AND    C2.cd_id in ('SALE','COI','MGN_PROFIT')
            AND    C2.cd_id in ('SALE','MGN_PROFIT','MODEL_COUNT') -- 한게이익 구간대별 매출, 한계이익
            AND    mgnl.VIRT_MDL_FLAG_CD = 'N'
            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.SUBSDR_CD,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE','MGN_PROFIT','MODEL_COUNT') then
                      case mgnl.MGNL_PRF_RANGE_CD
                         when '30%~'    then 'MARGINAL_PF_30'
                         when '20%~30%' then 'MARGINAL_PF_20_30'
                         when '13%~20%' then 'MARGINAL_PF_10_20'
                         when '10%~13%' then 'MARGINAL_PF_10_20'
                         when '5%~10%'  then 'MARGINAL_PF_5_10'
                         when '0%~5%'   then 'MARGINAL_PF_0_5'
                         when '~0%'      then 'MARGINAL_PF_(-)'
                         else mgnl.MGNL_PRF_RANGE_CD
                       end
                  end

        /*---------------------------------------------------
           영업이익 구간별 매출액,한계이익
        ----------------------------------------------------*/
            union all


SELECT '1550'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_SUBSDR'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_SUBSDR'                                                  AS RS_TYPE_NAME                             
      ,mgnl.div_cd                                                 AS DIV_CD                                   
      ,mgnl.BASIS_YYYYMM                                                  AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG ,                                
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.SUBSDR_CD,
                    mgnl.div_cd DIVISION_CODE,
                    'N'         AS MANUAL_ADJUST_FLAG,
                     c2.cd_id   AS KPI_TYPE_CODE,
                    case when c2.cd_id in ('SALE','COI','MGN_PROFIT','MODEL_COUNT') then
                        case mgnl.OI_RANGE_CD
                           when '10%~'     then 'COI_10'
                           when '0%~10%'   then 'COI_0_10'
                           when '-5%~0%'   then 'COI_-5_0'
                           when '-10%~-5%' then 'COI_-10_-5'
                           when '-15%~-10%'  then 'COI_-15_-10'
                           when '~-15%'      then 'COI_-15'
                           else null
                        end
                    end as CATEGORY_DETAIL_CODE,
                    MIN(
                    CASE mgnl.scenario_type_cd
                         WHEN 'AC0' THEN basis_yyyymm
                         WHEN 'PR1' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                         WHEN 'PR2' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                         WHEN 'PR3' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                         WHEN 'PR4' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), 4), 'YYYYMM')
                    END) as YYYYMM,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_KRW_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_KRW_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_KRW_AMT
                         when c2.cd_id = 'MODEL_COUNT' then
                              mgnl.MGNL_PRF_MDL_CNT
                         else 0
                    end) as curr_mon_krw_amount,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         when c2.cd_id = 'MODEL_COUNT' then
                              mgnl.MGNL_PRF_MDL_CNT
                         else 0
                    end) as curr_mon_usd_amount,
                    null accu_mon_krw_amount,
                    null accu_mon_usd_amount
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','COI','MGN_PROFIT','MODEL_COUNT')
            AND    mgnl.VIRT_MDL_FLAG_CD = 'N'
            --AND    mgnl.scenario_type_cd in ('PR2','PR3','PR4')
            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.SUBSDR_CD,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE','COI','MGN_PROFIT','MODEL_COUNT') then
                      case mgnl.OI_RANGE_CD
                         when '10%~'     then 'COI_10'
                         when '0%~10%'   then 'COI_0_10'
                         when '-5%~0%'   then 'COI_-5_0'
                         when '-10%~-5%' then 'COI_-10_-5'
                         when '-15%~-10%'  then 'COI_-15_-10'
                         when '~-15%'      then 'COI_-15'
                         else null
                      end
                  end
