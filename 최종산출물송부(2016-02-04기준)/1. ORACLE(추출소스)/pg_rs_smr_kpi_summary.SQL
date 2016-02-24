CREATE OR REPLACE PACKAGE NPT_APP.pg_rs_smr_kpi_summary
/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_smr_kpi_summary
/* 4.기       능 : 1.월별 재고 Trend 데이터 생성 (SP_RS_SMR_KPI_INV_SUMMARY)
/*                 2.
/*                 3.
/* 5.입 력 변 수 :
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 10.변 경 이 력 :
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
 /*     1.0 KIM.S.K RS    2016.01.05 최초작성
/***************************************************************************************************/


 IS

    cv_module_name CONSTANT VARCHAR2(40) := 'pg_rs_smr_kpi_summary'; --set package name

    PROCEDURE SP_RS_SMR_KPI_INV_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2);

    PROCEDURE SP_RS_SMR_KPI_SCM_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2);

    PROCEDURE SP_RS_SMR_KPI_FFR_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2);

    PROCEDURE SP_RS_SMR_KPI_SVC_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2);

END pg_rs_smr_kpi_summary;
/
CREATE OR REPLACE PACKAGE BODY NPT_APP.pg_rs_smr_kpi_summary IS
    PROCEDURE SP_RS_SMR_KPI_INV_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia
        /* 2.모       듈 : RS (ARES)
        /* 3.프로그램 ID : SP_RS_SMR_KPI_INV_SUMMARY
        /* 4.기       능 : 법인별로 월별 재고 Trend를 위한 데이터를 집계하여
        /*                 TB_RS_KPI_SUBSDR_CNTRY_H에 데이터를 생성함
        /* 5.입 력 변 수 :
        /*                 [필수] iv_yyyymm( 기준월 )
        /*                 [필수] iv_category( 시산구분 - 'SMART_INV')
        /*                 [필수] iv_div_yyyymm( Division기준월 )
        /* 6.Source      : NPT_APP.NV_DWW_INV_AGING_H
        /* 7.사  용   예 :
        /* 8.파 일 위 치 :
        /* 9. Step      : 1) Delete 기준월의 기존 SMART_INV 데이터 삭제
                          2) Insert
                             - step 1 : 
                             - step 2 : 
                             - step 3 : 
        /* 10.변 경 이 력 :
        /* Version  작성자  소속   일    자   내       용                                             요청자
        /* -------- ------ ------ ---------- -------------------------------------------------------- ------
        /*  1.0     KIM.S.K RS    2016.01.06  최초 작성
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_smr_kpi_inv_summary (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

    BEGIN
        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        /*---------------------------------------------
           기준월의 기존 SMART_INV 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   NPT_RS_MGR.TB_RS_KPI_SUBSDR_CNTRY_H
            WHERE  BASE_YYYYMM = iv_yyyymm
            AND    CAT_CD      = iv_category
            AND    MANUAL_ADJ_FLAG = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_subsdr_cntry_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT INTO NPT_RS_MGR.TB_RS_KPI_SUBSDR_CNTRY_H
                 ( BASE_YYYYMM                          /* 기준년월                 */
                  ,SCENARIO_TYPE_CD                     /* 시나리오유형코드         */
                  ,KPI_CD                               /* KPI코드                  */
                  ,CAT_CD                               /* 카테고리코드             */
                  ,DIV_CD                               /* 사업부코드               */
                  ,SUBSDR_CD                            /* 법인코드                 */
                  ,SUBSDR_RNR_CD                        /* 법인R&R코드              */
                  ,AU_RNR_CD                            /* AUR&R코드                */
                  ,REGN_RNR_CD                          /* 지역R&R코드              */
                  ,ZONE_RNR_CD                          /* 구역R&R코드              */
                  ,CNTRY_CD                             /* 국가코드                 */
                  ,REGN_CD                              /* 지역코드                 */
                  ,MANUAL_ADJ_FLAG                      /* 수작업조정여부           */
                  ,CURRM_KRW_AMT                        /* 당월원화금액             */
                  ,CURRM_USD_AMT                        /* 당월달러금액             */
                  ,ACCU_KRW_AMT                         /* 누적원화금액             */
                  ,ACCU_USD_AMT                         /* 누적달러금액             */
                  ,CREATION_DATE                        /* 생성일자                 */
                  ,CREATION_USR_ID                      /* 생성사용자아이디         */
                  ,LAST_UPD_DATE                        /* 최종갱신일자             */
                  ,LAST_UPD_USR_ID                      /* 최종갱신사용자아이디     */
                 )
            -- 재고 Aging
            SELECT a11.BASE_YYYYMM                              AS BASE_YYYYMM       /* 기준년월                 */
                  ,'AC0'                                        AS SCENARIO_TYPE_CD  /* 시나리오유형코드         */
                  ,kpi.KPI_CD                                   AS KPI_CD            /* KPI코드                  */
                  ,iv_category                                  AS CAT_CD            /* 카테고리코드             */
                  ,a11.DIV_CD                                   AS DIV_CD            /* 사업부코드               */
                  ,a11.SUBSDR_CD                                AS SUBSDR_CD         /* 법인코드                 */
                  ,'*'                                          AS SUBSDR_RNR_CD     /* 법인R&R코드              */
                  ,'*'                                          AS AU_RNR_CD         /* AUR&R코드                */
                  ,'*'                                          AS REGN_RNR_CD       /* 지역R&R코드              */
                  ,'*'                                          AS ZONE_RNR_CD       /* 구역R&R코드              */
                  ,'*'                                          AS CNTRY_CD          /* 국가코드                 */
                  ,'*'                                          AS REGN_CD           /* 지역코드                 */
                  ,'N'                                          AS MANUAL_ADJ_FLAG   /* 수작업조정여부           */
                  ,0                                            AS CURRM_KRW_AMT     /* 당월원화금액             */
                  ,SUM(CASE kpi.KPI_CD WHEN 'INV'                       THEN (a11.WH_LOC_INV_AMT              * a11.MONTH_END_USD_XRATE) + 
                                                                             (a11.SEA_LOC_INV_AMT             * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_WH'                    THEN (a11.WH_LOC_INV_AMT              * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_SEA'                   THEN (a11.SEA_LOC_INV_AMT             * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM'             THEN (a11.LONT_WH_LOC_INV_AMT         * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_MTL'         THEN (a11.POTN_LONT_WH_LOC_INV_AMT    * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_AGING'       THEN (a11.MONTH1_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) + 
                                                                             (a11.MONTH2_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH3_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH4_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH5_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH6_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH12_WTHN_WH_LOC_INV_AMT * a11.MONTH_END_USD_XRATE) + 
                                                                             (a11.MONTH12_EX_WH_LOC_INV_AMT   * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_3_6'         THEN (a11.MONTH3_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) + 
                                                                             (a11.MONTH4_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH5_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE) +
                                                                             (a11.MONTH6_WTHN_WH_LOC_INV_AMT  * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_7_12'        THEN (a11.MONTH12_WTHN_WH_LOC_INV_AMT * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_13_Over'     THEN (a11.MONTH12_EX_WH_LOC_INV_AMT   * a11.MONTH_END_USD_XRATE)
                                       WHEN 'INV_LONG_TERM_MTL_3_6'     THEN 0
                                       WHEN 'INV_LONG_TERM_MTL_7_12'    THEN 0
                                       WHEN 'INV_LONG_TERM_MTL_13_Over' THEN 0
                               ELSE 0 END)                      AS CURRM_USD_AMT     /* 당월달러금액             */
                  ,0                                            AS ACCU_KRW_AMT      /* 누적원화금액             */
                  ,0                                            AS ACCU_USD_AMT      /* 누적달러금액             */
                  ,sysdate                                      AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                       AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,sysdate                                      AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                       AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_INV_AGING_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_AU2_M a12
                                ON (a11.AU_CD = a12.AU_CD AND a11.SUBSDR_CD = a12.SUBSDR_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_DIV_LEAF_M a13
                                ON (a11.DIV_CD = a13.DIV_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_INV_LOCN_TYPE_M a14
                                ON (a11.INV_LOCN_TYPE_CD = a14.INV_LOCN_TYPE_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_INV_TYPE_M a15
                                ON (a11.INV_TYPE_CD = a15.INV_TYPE_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_PROD3_M a16
                                ON (a11.PROD_LVL3_CD = a16.PROD_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_VALUE_TYPE_CD_M a17
                                ON (a11.VALUE_TYPE_CD = a17.VALUE_TYPE_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SUBSDR_M a18
                                ON (a11.SUBSDR_CD = a18.SUBSDR_CD)
                  ,(SELECT 'INV'                       AS KPI_CD, '재고금액'                    AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_WH'                    AS KPI_CD, '재고금액_창고'               AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_SEA'                   AS KPI_CD, '재고금액_해상'               AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM'             AS KPI_CD, '장기재고금액'                AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_MTL'         AS KPI_CD, '장기재고금액(자재)'          AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_AGING'       AS KPI_CD, '장기재고금액(월령)'          AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_3_6'         AS KPI_CD, '3~6개월 장기재고금액'        AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_7_12'        AS KPI_CD, '7~12개월 장기재고금액'       AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_13_Over'     AS KPI_CD, '13개월이상 장기재고금액'     AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_MTL_3_6'     AS KPI_CD, '3~6개월 자재장기재고금액'    AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_MTL_7_12'    AS KPI_CD, '7~12개월 자재장기재고금액'   AS KPI_NAME FROM DUAL UNION ALL
                    SELECT 'INV_LONG_TERM_MTL_13_Over' AS KPI_CD, '13개월이상 자재장기재고금액' AS KPI_NAME FROM DUAL
                   ) kpi
            WHERE (a11.BASE_YYYYMM   IN (iv_yyyymm)
            AND    a11.VALUE_TYPE_CD IN ('AMT')
            AND    a11.SUBSDR_CD NOT IN ('ESPZ'))
            GROUP BY a11.BASE_YYYYMM
                    ,a11.SUBSDR_CD
                    ,a11.DIV_CD
                    ,kpi.KPI_CD
            UNION ALL
            -- DIO
            SELECT cal.BASE_YYYYMM                              AS BASE_YYYYMM       /* 기준년월                 */
                  ,'AC0'                                        AS SCENARIO_TYPE_CD  /* 시나리오유형코드         */
                  ,kpi.KPI_CD                                   AS KPI_CD            /* KPI코드                  */
                  ,iv_category                                  AS CAT_CD            /* 카테고리코드             */
                  ,cal.DIV_CD                                   AS DIV_CD            /* 사업부코드               */
                  ,cal.SUBSDR_CD                                AS SUBSDR_CD         /* 법인코드                 */
                  ,'*'                                          AS SUBSDR_RNR_CD     /* 법인R&R코드              */
                  ,'*'                                          AS AU_RNR_CD         /* AUR&R코드                */
                  ,'*'                                          AS REGN_RNR_CD       /* 지역R&R코드              */
                  ,'*'                                          AS ZONE_RNR_CD       /* 구역R&R코드              */
                  ,'*'                                          AS CNTRY_CD          /* 국가코드                 */
                  ,'*'                                          AS REGN_CD           /* 지역코드                 */
                  ,'N'                                          AS MANUAL_ADJ_FLAG   /* 수작업조정여부           */
                  ,0                                            AS CURRM_KRW_AMT     /* 당월원화금액             */
                  ,SUM(CASE kpi.KPI_CD 
                       WHEN 'INV_DIO'     THEN (CASE WHEN cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                     WHEN cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT) THEN
                                                          CASE WHEN cal.RF1_USD_COGS_AMT = 0 OR cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                               ELSE cal.CURRM_USD_INV_AMT / (cal.RF1_USD_COGS_AMT) * 30.42 END
                                                     WHEN (cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT + cal.RF2_USD_COGS_AMT)) THEN
                                                          CASE WHEN cal.RF2_USD_COGS_AMT = 0 OR cal.CURRM_USD_INV_AMT - (cal.RF1_USD_COGS_AMT) = 0 THEN NULL
                                                               ELSE 30.42 + ( (cal.CURRM_USD_INV_AMT - (cal.RF1_USD_COGS_AMT) ) / (cal.RF2_USD_COGS_AMT) * 30.42) END
                                                     WHEN (cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT + cal.RF2_USD_COGS_AMT + cal.RF3_USD_COGS_AMT)) THEN
                                                          CASE WHEN cal.RF3_USD_COGS_AMT = 0 OR cal.CURRM_USD_INV_AMT - ( cal.RF1_USD_COGS_AMT + cal.RF2_USD_COGS_AMT) = 0 THEN NULL
                                                               ELSE 30.42 * 2 + ( (cal.CURRM_USD_INV_AMT - ( cal.RF1_USD_COGS_AMT + cal.RF2_USD_COGS_AMT) ) / (cal.RF3_USD_COGS_AMT) * 30.42) END
                                                     ELSE CASE WHEN cal.AVG_USD_COGS_AMT = 0 OR cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                               ELSE cal.CURRM_USD_INV_AMT / (cal.AVG_USD_COGS_AMT) * 30.42 END
                                                     END)
                       WHEN 'INV_DIO_R'   THEN (CASE WHEN cal.ACCUM_AVG_USD_COGS_AMT = 0 THEN 0 ELSE cal.ACCUM_AVG_USD_INV_AMT / cal.ACCUM_AVG_USD_COGS_AMT * 30.42 END)
                       WHEN 'INV_DIO_MTL' THEN (CASE WHEN cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                     WHEN cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT + cal.RF1_USD_MTL_COST_AMT) THEN
                                                          CASE WHEN cal.RF1_USD_COGS_AMT + cal.RF1_USD_MTL_COST_AMT = 0 OR cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                               ELSE cal.CURRM_USD_INV_AMT / (cal.RF1_USD_COGS_AMT+ cal.RF1_USD_MTL_COST_AMT) * 30.42 END
                                                     WHEN (cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT + cal.RF2_USD_COGS_AMT+ cal.RF1_USD_MTL_COST_AMT+ cal.RF2_USD_MTL_COST_AMT)) THEN
                                                          CASE WHEN cal.RF2_USD_COGS_AMT+ cal.RF2_USD_MTL_COST_AMT = 0 OR cal.CURRM_USD_INV_AMT - (cal.RF1_USD_COGS_AMT+ cal.RF1_USD_MTL_COST_AMT) = 0 THEN NULL
                                                               ELSE 30.42 + ( (cal.CURRM_USD_INV_AMT - (cal.RF1_USD_COGS_AMT+ cal.RF1_USD_MTL_COST_AMT) ) / (cal.RF2_USD_COGS_AMT+ cal.RF2_USD_MTL_COST_AMT) * 30.42) END
                                                     WHEN (cal.CURRM_USD_INV_AMT <= (cal.RF1_USD_COGS_AMT + cal.RF1_USD_MTL_COST_AMT+ cal.RF2_USD_COGS_AMT + cal.RF2_USD_MTL_COST_AMT+ cal.RF3_USD_COGS_AMT+ cal.RF3_USD_MTL_COST_AMT)) THEN
                                                          CASE WHEN cal.RF3_USD_COGS_AMT+ cal.RF3_USD_MTL_COST_AMT = 0 OR cal.CURRM_USD_INV_AMT - ( cal.RF1_USD_COGS_AMT + cal.RF1_USD_MTL_COST_AMT+ cal.RF2_USD_COGS_AMT+ cal.RF2_USD_MTL_COST_AMT) = 0 THEN NULL
                                                               ELSE 30.42 * 2 + ( (cal.CURRM_USD_INV_AMT - ( cal.RF1_USD_COGS_AMT + cal.RF1_USD_MTL_COST_AMT+ cal.RF2_USD_COGS_AMT+ cal.RF2_USD_MTL_COST_AMT) ) / (cal.RF3_USD_COGS_AMT+ cal.RF3_USD_MTL_COST_AMT) * 30.42) END
                                                     ELSE CASE WHEN cal.AVG_USD_COGS_AMT+ cal.AVG_USD_MTL_COST_AMT = 0 OR cal.CURRM_USD_INV_AMT = 0 THEN NULL
                                                               ELSE cal.CURRM_USD_INV_AMT / (cal.AVG_USD_COGS_AMT+ cal.AVG_USD_MTL_COST_AMT) * 30.42 END
                                                     END)
                       ELSE 0 END)                              AS CURRM_USD_AMT     /* 당월달러금액             */
                  ,0                                            AS ACCU_KRW_AMT      /* 누적원화금액             */
                  ,0                                            AS ACCU_USD_AMT      /* 누적달러금액             */
                  ,sysdate                                      AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                       AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,sysdate                                      AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                       AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM  (SELECT dtl.BASE_YYYYMM                       AS BASE_YYYYMM
                         ,dtl.DIV_CD                            AS DIV_CD
                         ,dtl.SUBSDR_CD                         AS SUBSDR_CD
                         ,SUM(dtl.CURRM_USD_INV_AMT)            AS CURRM_USD_INV_AMT /* INV Amt (USD)            */
                         ,SUM(dtl.RF1_USD_COGS_AMT)             AS RF1_USD_COGS_AMT  /* COGS(ROFO +1Month) (USD) */
                         ,SUM(dtl.RF2_USD_COGS_AMT)             AS RF2_USD_COGS_AMT  /* COGS(ROFO +2Month) (USD) */
                         ,SUM(dtl.RF3_USD_COGS_AMT)             AS RF3_USD_COGS_AMT  /* COGS(ROFO +3Month) (USD) */
                         ,CASE WHEN (DECODE(SUM(dtl.RF1_USD_COGS_AMT),0,0,1) + DECODE(SUM(dtl.RF2_USD_COGS_AMT),0,0,1) + DECODE(SUM(dtl.RF3_USD_COGS_AMT),0,0,1)) = 0 THEN 0
                               ELSE SUM(dtl.RF1_USD_COGS_AMT + dtl.RF2_USD_COGS_AMT + dtl.RF3_USD_COGS_AMT) / 
                                        (DECODE(SUM(dtl.RF1_USD_COGS_AMT),0,0,1) + DECODE(SUM(dtl.RF2_USD_COGS_AMT),0,0,1) + DECODE(SUM(dtl.RF3_USD_COGS_AMT),0,0,1)) * 
                                        (CASE WHEN SUM(dtl.RF1_USD_COGS_AMT) = 0 AND SUM(dtl.RF2_USD_COGS_AMT) = 0 AND SUM(dtl.RF3_USD_COGS_AMT) = 0 THEN 0 ELSE 1 END) 
                               END                              AS AVG_USD_COGS_AMT  /* COGS(3Month ROFO Avg.) (USD) */
                         ,SUM(dtl.RF1_USD_MTL_COST_AMT)         AS RF1_USD_MTL_COST_AMT  /* MTL Cost (ROFO +1Month) (USD) */
                         ,SUM(dtl.RF2_USD_MTL_COST_AMT)         AS RF2_USD_MTL_COST_AMT  /* MTL Cost (ROFO +2Month) (USD) */
                         ,SUM(dtl.RF3_USD_MTL_COST_AMT)         AS RF3_USD_MTL_COST_AMT  /* MTL Cost (ROFO +3Month) (USD) */
                         ,CASE WHEN (DECODE(SUM(dtl.RF1_USD_MTL_COST_AMT),0,0,1) + DECODE(SUM(dtl.RF2_USD_MTL_COST_AMT),0,0,1) + DECODE(SUM(dtl.RF3_USD_MTL_COST_AMT),0,0,1)) = 0 THEN 0
                               ELSE SUM(dtl.RF1_USD_MTL_COST_AMT + dtl.RF2_USD_MTL_COST_AMT + dtl.RF3_USD_MTL_COST_AMT) / 
                                        (DECODE(SUM(dtl.RF1_USD_MTL_COST_AMT),0,0,1) + DECODE(SUM(dtl.RF2_USD_MTL_COST_AMT),0,0,1) + DECODE(SUM(dtl.RF3_USD_MTL_COST_AMT),0,0,1)) *
                                        (CASE WHEN SUM(dtl.RF1_USD_MTL_COST_AMT) = 0 AND SUM(dtl.RF2_USD_MTL_COST_AMT) = 0 AND SUM(dtl.RF1_USD_MTL_COST_AMT) = 0 THEN 0 ELSE 1 END) 
                               END                              AS AVG_USD_MTL_COST_AMT  /* MTL Cost (3Month ROFO Avg.) (USD) */
                         ,SUM(dtl.ACCUM_AVG_USD_INV_AMT)        AS ACCUM_AVG_USD_INV_AMT
                         ,SUM(ACCUM_AVG_USD_COGS_AMT)           AS ACCUM_AVG_USD_COGS_AMT
                   FROM (SELECT COALESCE(pa11.BASE_YYYYMM, pa12.BASE_YYYYMM) AS BASE_YYYYMM
                             --,COALESCE(pa11.DIV_CD, pa12.DIV_CD)           AS DIV_CD
                               ,CASE WHEN typ.CAL_TYPE = 'GBU' THEN 'GBU'
                                     WHEN typ.CAL_TYPE IN ('B2B','B2C') THEN
                                          CASE WHEN COALESCE(pa11.DIV_CD, pa12.DIV_CD) IN ('GJT','GNT','GOT','PKT','DMT','DTT','PGT','VHT','GCM','CMM','SDT','CLT','SRT') THEN 'B2B'
                                               WHEN COALESCE(pa11.DIV_CD, pa12.DIV_CD) IN ('GLT','GPT','PJT','GRT','GTT','PCT','PST','PNT','PHT','MST','CNT','CWT','DFT','DVT','CVT','DGT') THEN 'B2C'
                                               ELSE 'NONE' END
                                     WHEN typ.CAL_TYPE = 'DIV' THEN COALESCE(pa11.DIV_CD, pa12.DIV_CD)
                                     END                                     AS DIV_CD
                               ,a13.SCRN_DSPL_SEQ                            AS DIV_KOR_NAME
                               ,a13.DIV_SHRT_NAME                            AS DIV_SHRT_NAME
                               ,COALESCE(pa11.SUBSDR_CD, pa12.SUBSDR_CD)     AS SUBSDR_CD
                               ,a16.SUBSDR_SHRT_NAME                         AS NEW_SUBSDR_SHRT_NAME
                               ,a16.SORT_ORDER                               AS SORT1_ORDER
                               ,COALESCE(pa11.MGT_ORG_CD, pa12.MGT_ORG_CD)   AS MGT_ORG_CD
                               ,a14.MGT_ORG_SHRT_NAME                        AS MGT_ORG_SHRT_NAME
                               ,a14.SORT_ORDER                               AS SORT_ORDER
                               ,COALESCE(pa11.SCENARIO_TYPE_CD, pa12.SCENARIO_TYPE_CD) AS SCENARIO_TYPE_CD
                               ,a15.SCENARIO_TYPE_NAME                       AS SCENARIO_TYPE_NAME
                               ,a15.SORT_ORDER                               AS SORT_ORDER0
                               ,pa11.CURRM_USD_INV_AMT                       AS CURRM_USD_INV_AMT
                               ,pa11.RF1_USD_COGS_AMT                        AS RF1_USD_COGS_AMT
                               ,pa11.RF2_USD_COGS_AMT                        AS RF2_USD_COGS_AMT
                               ,pa11.RF3_USD_COGS_AMT                        AS RF3_USD_COGS_AMT
                               ,pa12.RF1_USD_MTL_COST_AMT                    AS RF1_USD_MTL_COST_AMT
                               ,pa12.RF2_USD_MTL_COST_AMT                    AS RF2_USD_MTL_COST_AMT
                               ,pa12.RF3_USD_MTL_COST_AMT                    AS RF3_USD_MTL_COST_AMT
                               ,NVL((pa11.ACCUM_AVG_USD_INV_AMT / (CASE WHEN pa11.MONTH_NUM_ADD_1=0 THEN NULL 
                                                                        ELSE pa11.MONTH_NUM_ADD_1 END)), 0)  
                                                                        	AS ACCUM_AVG_USD_INV_AMT
                               ,NVL((pa11.ACCUM_AVG_USD_COGS_AMT / (CASE WHEN pa11.MONTH_NUM=0 THEN NULL 
                                                                         ELSE pa11.MONTH_NUM END)), 0) 
                                                                         	AS ACCUM_AVG_USD_COGS_AMT
                               ,NVL(((NVL(pa11.ACCUM_AVG_USD_COGS_AMT, 0) + 
                                      NVL(pa12.ACCUM_AVG_USD_MTL_COST_AMT, 0)) / 
                                      (CASE WHEN pa11.MONTH_NUM=0 THEN NULL 
                                            ELSE pa11.MONTH_NUM END)), 0)    AS ACCUM_AVG_USD_TOT_AMT
                         FROM  (SELECT a11.CLOSING_YYYYMM              AS BASE_YYYYMM
                                      ,a11.SUBSDR_CD                   AS SUBSDR_CD
                                      ,a11.SCENARIO_TYPE_CD            AS SCENARIO_TYPE_CD
                                      ,a11.MGT_ORG_CD                  AS MGT_ORG_CD
                                      ,a11.DIV_CD                      AS DIV_CD
                                      ,SUM(a11.CURRM_USD_INV_AMT)      AS CURRM_USD_INV_AMT
                                      ,SUM(a11.RF1_USD_COGS_AMT)       AS RF1_USD_COGS_AMT
                                      ,SUM(a11.RF2_USD_COGS_AMT)       AS RF2_USD_COGS_AMT
                                      ,SUM(a11.RF3_USD_COGS_AMT)       AS RF3_USD_COGS_AMT
                                      ,SUM(a11.ACCUM_AVG_USD_INV_AMT)  AS ACCUM_AVG_USD_INV_AMT
                                      ,(MAX(a11.MONTH_NUM) + 1.0)      AS MONTH_NUM_ADD_1
                                      ,SUM(a11.ACCUM_AVG_USD_COGS_AMT) AS ACCUM_AVG_USD_COGS_AMT
                                      ,MAX(a11.MONTH_NUM)              AS MONTH_NUM
                                FROM   NPT_APP.NV_DWM_DIO_CONSLD_FS_SUMM_H a11
                                WHERE (a11.SCENARIO_TYPE_CD IN ('AC0')
                                AND    a11.CLOSING_YYYYMM   IN (iv_yyyymm)
                                AND    a11.ASSET_CONSLD_STEP_TYPE_CD IN ('GXX')
                                AND    a11.COGS_TYPE_CD     IN ('INTEGRATED')
                                AND    a11.SUBSDR_CD    NOT IN ('ESPZ'))
                                GROUP BY a11.CLOSING_YYYYMM, a11.SUBSDR_CD, a11.SCENARIO_TYPE_CD, a11.MGT_ORG_CD, a11.DIV_CD
                               ) pa11
                                 FULL OUTER JOIN (SELECT a11.CLOSING_YYYYMM                  AS BASE_YYYYMM
                                                        ,a11.SUBSDR_CD                       AS SUBSDR_CD
                                                        ,a11.SCENARIO_TYPE_CD                AS SCENARIO_TYPE_CD
                                                        ,a11.MGT_ORG_CD                      AS MGT_ORG_CD
                                                        ,a11.DIV_CD                          AS DIV_CD
                                                        ,SUM(a11.RF1_USD_MTL_COST_AMT)       AS RF1_USD_MTL_COST_AMT
                                                        ,SUM(a11.RF2_USD_MTL_COST_AMT)       AS RF2_USD_MTL_COST_AMT
                                                        ,SUM(a11.RF3_USD_MTL_COST_AMT)       AS RF3_USD_MTL_COST_AMT
                                                        ,SUM(a11.ACCUM_AVG_USD_MTL_COST_AMT) AS ACCUM_AVG_USD_MTL_COST_AMT
                                                  FROM   NPT_APP.NV_DWM_DIO_MS_SUMM_H a11
                                                  WHERE (a11.SCENARIO_TYPE_CD IN ('AC0')
                                                  AND    a11.CLOSING_YYYYMM   IN (iv_yyyymm)
                                                  AND    a11.ASSET_CONSLD_STEP_TYPE_CD IN ('GXX')
                                                  AND    a11.COGS_TYPE_CD     IN ('INTEGRATED'))
                                                  GROUP BY a11.CLOSING_YYYYMM, a11.SUBSDR_CD, a11.SCENARIO_TYPE_CD, a11.MGT_ORG_CD, a11.DIV_CD
                                                 ) pa12
                                              ON ( pa11.BASE_YYYYMM      = pa12.BASE_YYYYMM 
                                             AND   pa11.DIV_CD           = pa12.DIV_CD 
                                             AND   pa11.MGT_ORG_CD       = pa12.MGT_ORG_CD 
                                             AND   pa11.SCENARIO_TYPE_CD = pa12.SCENARIO_TYPE_CD 
                                             AND   pa11.SUBSDR_CD        = pa12.SUBSDR_CD)
                                 LEFT OUTER JOIN  NPT_APP.NV_DWD_DIV_LEAF_M a13
                                              ON (COALESCE(pa11.DIV_CD, pa12.DIV_CD) = a13.DIV_CD)
                                 LEFT OUTER JOIN  NPT_APP.NV_DWD_MGT_ORG_M a14
                                              ON (COALESCE(pa11.MGT_ORG_CD, pa12.MGT_ORG_CD) = a14.MGT_ORG_CD)
                                 LEFT OUTER JOIN  NPT_APP.NV_DWD_SCENARIO_TYPE_M a15
                                              ON (COALESCE(pa11.SCENARIO_TYPE_CD, pa12.SCENARIO_TYPE_CD) = a15.SCENARIO_TYPE_CD)
                                 LEFT OUTER JOIN NPT_APP.NV_DWD_SUBSDR_M a16
                                              ON (COALESCE(pa11.SUBSDR_CD, pa12.SUBSDR_CD) = a16.SUBSDR_CD)
                               ,(SELECT 'GBU' AS CAL_TYPE FROM DUAL UNION ALL
                                 SELECT 'DIV' AS CAL_TYPE FROM DUAL UNION ALL
                                 SELECT 'B2B' AS CAL_TYPE FROM DUAL UNION ALL
                                 SELECT 'B2C' AS CAL_TYPE FROM DUAL
                                ) typ
                         ) dtl
                   GROUP BY dtl.BASE_YYYYMM, dtl.SUBSDR_CD, dtl.DIV_CD
                  ) cal
                 ,(SELECT 'INV_DIO'     AS KPI_CD, '재고일수'         AS KPI_NAME FROM DUAL UNION ALL
                   SELECT 'INV_DIO_R'   AS KPI_CD, '재고일수(역산법)' AS KPI_NAME FROM DUAL UNION ALL
                   SELECT 'INV_DIO_MTL' AS KPI_CD, '재고일수(자재)'   AS KPI_NAME FROM DUAL
                  ) kpi
            GROUP BY cal.BASE_YYYYMM, cal.SUBSDR_CD, cal.DIV_CD, kpi.KPI_CD
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_subsdr_cntry_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('2)Insert success row : ' || vn_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            dbms_output.put_line('vv_exception ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);
        WHEN OTHERS THEN
            ROLLBACK;
            dbms_output.put_line('OTHER ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);

    END SP_RS_SMR_KPI_INV_SUMMARY;

    PROCEDURE SP_RS_SMR_KPI_SCM_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia
        /* 2.모       듈 : RS (ARES)
        /* 3.프로그램 ID : SP_RS_SMR_KPI_SCM_SUMMARY
        /* 4.기       능 : 법인별로 월별 SCM Trend를 위한 데이터를 집계하여
        /*                 TB_RS_KPI_SCM_H에 데이터를 생성함
        /* 5.입 력 변 수 :
        /*                 [필수] iv_yyyymm( 기준월 )
        /*                 [필수] iv_category( 시산구분 - 'SMART_SCM')
        /*                 [필수] iv_div_yyyymm( Division기준월 )
        /* 6.Source      : NPT_APP.NV_DWW_SCM_WEEKLY_KPI_H
        /* 7.사  용   예 :
        /* 8.파 일 위 치 :
        /* 9. Step      : 1) Delete 기준월의 기존 SMART_INV 데이터 삭제
                          2) Insert
                             - step 1 : 
                             - step 2 : 
                             - step 3 : 
        /* 10.변 경 이 력 :
        /* Version  작성자  소속   일    자   내       용                                             요청자
        /* -------- ------ ------ ---------- -------------------------------------------------------- ------
        /*  1.0     KIM.S.K RS    2016.01.19  최초 작성
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_smr_kpi_scm_summary (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

    BEGIN
        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        /*---------------------------------------------
           기준월의 기존 SMART_INV 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   NPT_RS_MGR.TB_RS_KPI_SCM_H
            WHERE  BASE_YYYYMMDD = iv_yyyymm
            AND    CAT_CD        = iv_category
            AND    MANUAL_ADJ_FLAG = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_scm_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT INTO NPT_RS_MGR.TB_RS_KPI_SCM_H
                 ( PERIOD_TYPE_VALUE                    /* 기간유형값               */
                  ,BASE_YYYYMMDD                        /* 기준년월일               */
                  ,KPI_CD                               /* KPI코드                  */
                  ,CAT_CD                               /* 카테고리코드             */
                  ,MANUAL_ADJ_FLAG                      /* 수작업조정여부           */
                  ,DIV_CD                               /* 사업부코드               */
                  ,SUBSDR_FCTRY_CD                      /* 법인공장코드             */
                  ,KPI_RESULT_VALUE                     /* KPI실적값                */
                  ,KPI_RED_VALUE                        /* KPI적색값                */
                  ,KPI_GREEN_VALUE                      /* KPI녹색값                */
                  ,ATTRIBUTE1_AMT                       /* 예비속성1금액            */
                  ,ATTRIBUTE2_AMT                       /* 예비속성2금액            */
                  ,ATTRIBUTE3_AMT                       /* 예비속성3금액            */
                  ,ATTRIBUTE4_AMT                       /* 예비속성4금액            */
                  ,ATTRIBUTE5_AMT                       /* 예비속성5금액            */
                  ,CREATION_DATE                        /* 생성일자                 */
                  ,CREATION_USR_ID                      /* 생성사용자아이디         */
                  ,LAST_UPD_DATE                        /* 최종갱신일자             */
                  ,LAST_UPD_USR_ID                      /* 최종갱신사용자아이디     */
                 )
            -- GBU
            SELECT a11.PERIOD_TYPE_CD                           AS PERIOD_TYPE_VALUE /* 기간유형값               */
                  ,a11.WEEKLY_YYYYMMDD                          AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,DECODE(a11.KPI_TYPE_CD, 'SA_OL_4W_LESS', 'OL_4W_LESS', 'SA_OL_4W_OVER', 'OL_4W_OVER', a11.KPI_TYPE_CD)
                                                                AS KPI_CD            /* KPI코드                  */
                  ,iv_category                                  AS CAT_CD            /* 카테고리코드             */
                  ,'N'                                          AS MANUAL_ADJ_FLAG   /* 수작업조정여부           */
                  ,'GBU'                                        AS DIV_CD            /* 사업부코드               */
                  ,a11.SUBSDR_CD                                AS SUBSDR_FCTRY_CD   /* 법인공장코드             */
                  ,SUM(KPI_RESULT_VALUE)                        AS KPI_RESULT_VALUE  /* KPI실적값                */
                  ,NULL                                         AS KPI_RED_VALUE     /* KPI적색값                */
                  ,NULL                                         AS KPI_GREEN_VALUE   /* KPI녹색값                */
                  ,0                                            AS ATTRIBUTE1_AMT    /* 예비속성1금액            */
                  ,0                                            AS ATTRIBUTE2_AMT    /* 예비속성2금액            */
                  ,0                                            AS ATTRIBUTE3_AMT    /* 예비속성3금액            */
                  ,0                                            AS ATTRIBUTE4_AMT    /* 예비속성4금액            */
                  ,0                                            AS ATTRIBUTE5_AMT    /* 예비속성5금액            */
                  ,sysdate                                      AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                       AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,sysdate                                      AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                       AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_SCM_WEEKLY_KPI_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SCM_SUBSDR_LVL4_M a12
                                ON (a11.MGT_SUBSDR_CD = a12.MGT_SUBSDR_CD 
                               AND  a11.RHQ_CD        = a12.RHQ_CD 
                               AND  a11.SUBSDR_CD     = a12.SUBSDR_CD 
                               AND  a11.ZONE_CD       = a12.ZONE_CD)
            WHERE  a11.PERIOD_TYPE_CD IN ('M')
            AND    a11.WEEKLY_YYYYMMDD = iv_yyyymm
            AND  ((a11.DIV_LVL_CD     IN ('0')
            AND    a11.SUBSDR_LVL_CD  IN ('0'))
               OR (a11.DIV_LVL_CD     IN ('0')
            AND    a11.SUBSDR_LVL_CD  IN ('3')))
            AND    a11.KPI_TYPE_CD IN ('SA_OL_4W_LESS', 'SA_OL_4W_OVER', 'WOS')
            GROUP BY a11.PERIOD_TYPE_CD, a11.WEEKLY_YYYYMMDD, a11.KPI_TYPE_CD, a11.SUBSDR_CD
            UNION ALL
            -- DIVISION별
            SELECT a11.PERIOD_TYPE_CD                           AS PERIOD_TYPE_VALUE /* 기간유형값               */
                  ,a11.WEEKLY_YYYYMMDD                          AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,DECODE(a11.KPI_TYPE_CD, 'SA_OL_4W_LESS', 'OL_4W_LESS', 'SA_OL_4W_OVER', 'OL_4W_OVER', a11.KPI_TYPE_CD)
                                                                AS KPI_CD            /* KPI코드                  */
                  ,iv_category                                  AS CAT_CD            /* 카테고리코드             */
                  ,'N'                                          AS MANUAL_ADJ_FLAG   /* 수작업조정여부           */
                  ,a11.DIV_CD                                   AS DIV_CD            /* 사업부코드               */
                  ,a11.SUBSDR_CD                                AS SUBSDR_FCTRY_CD   /* 법인공장코드             */
                  ,SUM(KPI_RESULT_VALUE)                        AS KPI_RESULT_VALUE  /* KPI실적값                */
                  ,NULL                                         AS KPI_RED_VALUE     /* KPI적색값                */
                  ,NULL                                         AS KPI_GREEN_VALUE   /* KPI녹색값                */
                  ,0                                            AS ATTRIBUTE1_AMT    /* 예비속성1금액            */
                  ,0                                            AS ATTRIBUTE2_AMT    /* 예비속성2금액            */
                  ,0                                            AS ATTRIBUTE3_AMT    /* 예비속성3금액            */
                  ,0                                            AS ATTRIBUTE4_AMT    /* 예비속성4금액            */
                  ,0                                            AS ATTRIBUTE5_AMT    /* 예비속성5금액            */
                  ,sysdate                                      AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                       AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,sysdate                                      AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                       AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_SCM_WEEKLY_KPI_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SCM_DIV5_M a12
                                ON (a11.DIV1_CD = a12.DIV1_CD 
                               AND  a11.DIV2_CD = a12.DIV2_CD 
                               AND  a11.DIV3_CD = a12.DIV3_CD 
                               AND  a11.DIV4_CD = a12.DIV4_CD 
                               AND  a11.DIV5_CD = a12.DIV5_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SCM_SUBSDR_LVL4_M a13
                                ON (a11.MGT_SUBSDR_CD = a13.MGT_SUBSDR_CD 
                               AND  a11.RHQ_CD        = a13.RHQ_CD 
                               AND  a11.SUBSDR_CD     = a13.SUBSDR_CD 
                               AND  a11.ZONE_CD       = a13.ZONE_CD)

            WHERE  a11.PERIOD_TYPE_CD IN ('M')
            AND    a11.WEEKLY_YYYYMMDD = iv_yyyymm
            AND    ((a11.DIV_LVL_CD IN ('0')
               AND   a11.SUBSDR_LVL_CD IN ('0'))
            OR      (a11.DIV_LVL_CD IN ('5')
               AND   a11.SUBSDR_LVL_CD IN ('3')))
            AND    a11.KPI_TYPE_CD IN ('SA_OL_4W_LESS', 'SA_OL_4W_OVER', 'WOS')
            GROUP BY a11.PERIOD_TYPE_CD, a11.WEEKLY_YYYYMMDD, a11.KPI_TYPE_CD, a11.DIV_CD, a11.SUBSDR_CD
            ;
            
        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_scm_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('2)Insert success row : ' || vn_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            dbms_output.put_line('vv_exception ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);
        WHEN OTHERS THEN
            ROLLBACK;
            dbms_output.put_line('OTHER ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);

    END SP_RS_SMR_KPI_SCM_SUMMARY;

    PROCEDURE SP_RS_SMR_KPI_FFR_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia
        /* 2.모       듈 : RS (ARES)
        /* 3.프로그램 ID : SP_RS_SMR_KPI_FFR_SUMMARY
        /* 4.기       능 : 법인별로 월별 시장불량율(FFR) Trend를 위한 데이터를 집계하여
        /*                 TB_RS_EXCEL_UPLD_DATA_D에 데이터를 생성함
        /* 5.입 력 변 수 :
        /*                 [필수] iv_yyyymm( 기준월 )
        /*                 [필수] iv_category( 시산구분 - 'SMART_FFR')
        /*                 [필수] iv_div_yyyymm( Division기준월 )
        /* 6.Source      : NPT_APP.NV_DWW_MKT_DEFECT_RATE_H
        /* 7.사  용   예 :
        /* 8.파 일 위 치 :
        /* 9. Step      : 1) Delete 기준월의 기존 SMART_FFR 데이터 삭제
                          2) Insert
                             - step 1 : 
                             - step 2 : 
                             - step 3 : 
        /* 10.변 경 이 력 :
        /* Version  작성자  소속   일    자   내       용                                             요청자
        /* -------- ------ ------ ---------- -------------------------------------------------------- ------
        /*  1.0     KIM.S.K RS    2016.01.21  최초 작성
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_smr_kpi_ffr_summary (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

    BEGIN
        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        /*---------------------------------------------
           기준월의 기존 SMART_FFR 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
            WHERE  BASE_YYYYMMDD = iv_yyyymm
            AND    RS_CLSF_ID    = iv_category;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT INTO NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
                 ( PRCS_SEQ                             /* 프로세스일련순서         */
                  ,RS_MODULE_CD                         /* ARES모듈코드             */
                  ,RS_CLSF_ID                           /* ARES분류아이디           */
                  ,RS_TYPE_CD                           /* ARES유형코드             */
                  ,RS_TYPE_NAME                         /* ARES유형명               */
                  ,DIV_CD                               /* 사업부코드               */
                  ,BASE_YYYYMMDD                        /* 기준년월일               */
                  ,CD_DESC                              /* 코드설명                 */
                  ,SORT_SEQ                             /* 정렬일련순서             */
                  ,USE_FLAG                             /* 사용여부                 */
                  ,ATTRIBUTE1_VALUE                     /* 예비속성1값              */
                  ,ATTRIBUTE2_VALUE                     /* 예비속성2값              */
                  ,ATTRIBUTE3_VALUE                     /* 예비속성3값              */
                  ,ATTRIBUTE4_VALUE                     /* 예비속성4값              */
                  ,ATTRIBUTE5_VALUE                     /* 예비속성5값              */
                  ,ATTRIBUTE6_VALUE                     /* 예비속성6값              */
                  ,ATTRIBUTE7_VALUE                     /* 예비속성7값              */
                  ,ATTRIBUTE8_VALUE                     /* 예비속성8값              */
                  ,ATTRIBUTE9_VALUE                     /* 예비속성9값              */
                  ,ATTRIBUTE10_VALUE                    /* 예비속성10값             */
                  ,ATTRIBUTE11_VALUE                    /* 예비속성11값             */
                  ,ATTRIBUTE12_VALUE                    /* 예비속성12값             */
                  ,CREATION_DATE                        /* 생성일자                 */
                  ,CREATION_USR_ID                      /* 생성사용자아이디         */
                  ,CREATION_USR_NAME                    /* 생성사용자명             */
                  ,LAST_UPD_DATE                        /* 최종갱신일자             */
                  ,LAST_UPD_USR_ID                      /* 최종갱신사용자아이디     */
                 )
            SELECT '1550'                                           AS PRCS_SEQ          /* 프로세스일련순서         */
                  ,'ARES'                                           AS RS_MODULE_CD      /* ARES모듈코드             */
                  ,'SMART_FFR'                                      AS RS_CLSF_ID        /* ARES분류아이디           */
                  ,'SMART_FFR'                                      AS RS_TYPE_CD        /* ARES유형코드             */
                  ,'SMART_FFR'                                      AS RS_TYPE_NAME      /* ARES유형명               */
                  ,a11.DIV5_CD                                      AS DIV_CD            /* 사업부코드               */
                  ,a11.WEEKLY_YYYYMMDD                              AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,'SMART_FFR'                                      AS CD_DESC           /* 코드설명                 */
                  ,NULL                                             AS SORT_SEQ          /* 정렬일련순서             */
                  ,'Y'                                              AS USE_FLAG          /* 사용여부                 */
                  ,a12.SUBSDR_CD                                    AS ATTRIBUTE1_VALUE  /* Subsidiary               */
                  ,a14.SUBSDR_SHRT_NAME                             AS ATTRIBUTE2_VALUE  /* Subsidiary Name          */
                  ,a11.DIV5_CD                                      AS ATTRIBUTE3_VALUE  /* Division                 */
                  ,a13.DIV5_NAME                                    AS ATTRIBUTE4_VALUE  /* Division Name            */
                  ,DECODE(SUM(a11.ACCU_SELL_QTY),0,0,SUM(a11.ACCU_SVC_COUNT) / SUM(a11.ACCU_SELL_QTY)) 
                                                                    AS ATTRIBUTE5_VALUE  /* Field Failure Rate       */
                  ,SUM(a11.ACCU_SVC_COUNT)                          AS ATTRIBUTE6_VALUE  /* Service Count            */
                  ,SUM(a11.ACCU_SELL_QTY)                           AS ATTRIBUTE7_VALUE  /* Sell Quantity            */
                  ,NULL                                             AS ATTRIBUTE8_VALUE  /* 예비속성8값              */
                  ,NULL                                             AS ATTRIBUTE9_VALUE  /* 예비속성9값              */
                  ,NULL                                             AS ATTRIBUTE10_VALUE /* 예비속성10값             */
                  ,NULL                                             AS ATTRIBUTE11_VALUE /* 예비속성11값             */
                  ,NULL                                             AS ATTRIBUTE12_VALUE /* 예비속성12값             */
                  ,sysdate                                          AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                           AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,'ares'                                           AS CREATION_USR_NAME /* 생성사용자명             */
                  ,sysdate                                          AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                           AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_MKT_DEFECT_RATE_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_FFR_SUBSDR_MGT_M a12
                                ON (a11.MGT_SUBSDR_CD = a12.MGT_SUBSDR_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SCM_DIV5_M a13
                                ON (a11.DIV1_CD = a13.DIV1_CD 
                               AND  a11.DIV2_CD = a13.DIV2_CD 
                               AND  a11.DIV3_CD = a13.DIV3_CD 
                               AND  a11.DIV4_CD = a13.DIV4_CD 
                               AND  a11.DIV5_CD = a13.DIV5_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_FFR_SUBSDR_M a14
                                ON (a12.SUBSDR_CD = a14.SUBSDR_CD)
            WHERE  a11.PERIOD_TYPE_CD IN ('M')
            AND    a11.WEEKLY_YYYYMMDD = iv_yyyymm
            AND   (a11.DIV_LVL_CD     IN ('0')
               OR  a11.DIV_LVL_CD     IN ('5'))
            AND    a11.MGT_SUBSDR_CD NOT IN ('ALL')
            GROUP BY a12.SUBSDR_CD, a14.SUBSDR_SHRT_NAME, a11.DIV5_CD, a13.DIV5_NAME, a11.WEEKLY_YYYYMMDD
            ;
            
        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('2)Insert success row : ' || vn_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            dbms_output.put_line('vv_exception ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);
        WHEN OTHERS THEN
            ROLLBACK;
            dbms_output.put_line('OTHER ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);

    END SP_RS_SMR_KPI_FFR_SUMMARY;

    PROCEDURE SP_RS_SMR_KPI_SVC_SUMMARY(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia
        /* 2.모       듈 : RS (ARES)
        /* 3.프로그램 ID : SP_RS_SMR_KPI_SVC_SUMMARY
        /* 4.기       능 : 법인별로 월별 SVC Trend를 위한 데이터를 집계하여
        /*                 TB_RS_EXCEL_UPLD_DATA_D에 데이터를 생성함
        /* 5.입 력 변 수 :
        /*                 [필수] iv_yyyymm( 기준월 )
        /*                 [필수] iv_category( 시산구분 - 'SMART_SVC')
        /*                 [필수] iv_div_yyyymm( Division기준월 )
        /* 6.Source      : NPT_APP.NV_DWW_MKT_DEFECT_RATE_H
        /* 7.사  용   예 :
        /* 8.파 일 위 치 :
        /* 9. Step      : 1) Delete 기준월의 기존 SMART_SVC 데이터 삭제
                          2) Insert
                             - step 1 : 
                             - step 2 : 
                             - step 3 : 
        /* 10.변 경 이 력 :
        /* Version  작성자  소속   일    자   내       용                                             요청자
        /* -------- ------ ------ ---------- -------------------------------------------------------- ------
        /*  1.0     KIM.S.K RS    2016.01.25  최초 작성
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_smr_kpi_svc_summary (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

    BEGIN
        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        /*---------------------------------------------
           기준월의 기존 SMART_SVC 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
            WHERE  BASE_YYYYMMDD = iv_yyyymm
            AND    RS_CLSF_ID    = iv_category;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            -- Answer Speed : SVC_CALL
            -- RTAT         : SVC_RTAT
            -- Net Service Expense Rate : SVC_NET_COST
            INSERT INTO NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
                 ( PRCS_SEQ                             /* 프로세스일련순서         */
                  ,RS_MODULE_CD                         /* ARES모듈코드             */
                  ,RS_CLSF_ID                           /* ARES분류아이디           */
                  ,RS_TYPE_CD                           /* ARES유형코드             */
                  ,RS_TYPE_NAME                         /* ARES유형명               */
                  ,DIV_CD                               /* 사업부코드               */
                  ,BASE_YYYYMMDD                        /* 기준년월일               */
                  ,CD_DESC                              /* 코드설명                 */
                  ,SORT_SEQ                             /* 정렬일련순서             */
                  ,USE_FLAG                             /* 사용여부                 */
                  ,ATTRIBUTE1_VALUE                     /* 예비속성1값              */
                  ,ATTRIBUTE2_VALUE                     /* 예비속성2값              */
                  ,ATTRIBUTE3_VALUE                     /* 예비속성3값              */
                  ,ATTRIBUTE4_VALUE                     /* 예비속성4값              */
                  ,ATTRIBUTE5_VALUE                     /* 예비속성5값              */
                  ,ATTRIBUTE6_VALUE                     /* 예비속성6값              */
                  ,ATTRIBUTE7_VALUE                     /* 예비속성7값              */
                  ,ATTRIBUTE8_VALUE                     /* 예비속성8값              */
                  ,ATTRIBUTE9_VALUE                     /* 예비속성9값              */
                  ,ATTRIBUTE10_VALUE                    /* 예비속성10값             */
                  ,ATTRIBUTE11_VALUE                    /* 예비속성11값             */
                  ,ATTRIBUTE12_VALUE                    /* 예비속성12값             */
                  ,CREATION_DATE                        /* 생성일자                 */
                  ,CREATION_USR_ID                      /* 생성사용자아이디         */
                  ,CREATION_USR_NAME                    /* 생성사용자명             */
                  ,LAST_UPD_DATE                        /* 최종갱신일자             */
                  ,LAST_UPD_USR_ID                      /* 최종갱신사용자아이디     */
                 )
            -- Answer Speed : SVC_CALL
            SELECT '1560'                                           AS PRCS_SEQ          /* 프로세스일련순서         */
                  ,'ARES'                                           AS RS_MODULE_CD      /* ARES모듈코드             */
                  ,'SMART_SVC'                                      AS RS_CLSF_ID        /* ARES분류아이디           */
                  ,'SVC_CALL'                                       AS RS_TYPE_CD        /* ARES유형코드             */
                  ,'SVC_CALL'                                       AS RS_TYPE_NAME      /* ARES유형명               */
                  ,'*'                                              AS DIV_CD            /* 사업부코드               */
                  ,a11.ACCTG_YYYYMM                                 AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,'SVC_CALL'                                       AS CD_DESC           /* 코드설명                 */
                  ,NULL                                             AS SORT_SEQ          /* 정렬일련순서             */
                  ,'Y'                                              AS USE_FLAG          /* 사용여부                 */
                  ,a11.SUBSDR_CD                                    AS ATTRIBUTE1_VALUE  /* Subsidiary               */
                  ,a12.SUBSDR_SHRT_NAME                             AS ATTRIBUTE2_VALUE  /* Subsidiary Name          */
                  ,'*'                                              AS ATTRIBUTE3_VALUE  /* Division                 */
                  ,NULL                                             AS ATTRIBUTE4_VALUE  /* Division Name            */
                  ,DECODE(SUM(a11.DENOM_VALUE),0,0,SUM(a11.NUMR_VALUE) / SUM(a11.DENOM_VALUE)) 
                                                                    AS ATTRIBUTE5_VALUE  /* Answer Speed             */
                  ,SUM(a11.NUMR_VALUE)                              AS ATTRIBUTE6_VALUE  /* Waiting Time             */
                  ,SUM(a11.DENOM_VALUE)                             AS ATTRIBUTE7_VALUE  /* Response Call            */
                  ,NULL                                             AS ATTRIBUTE8_VALUE  /* 예비속성8값              */
                  ,NULL                                             AS ATTRIBUTE9_VALUE  /* 예비속성9값              */
                  ,NULL                                             AS ATTRIBUTE10_VALUE /* 예비속성10값             */
                  ,NULL                                             AS ATTRIBUTE11_VALUE /* 예비속성11값             */
                  ,NULL                                             AS ATTRIBUTE12_VALUE /* 예비속성12값             */
                  ,sysdate                                          AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                           AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,'ares'                                           AS CREATION_USR_NAME /* 생성사용자명             */
                  ,sysdate                                          AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                           AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_SVC_LVL_CALL_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SUBSDR_M a12
                                ON (a11.SUBSDR_CD = a12.SUBSDR_CD)
            WHERE  a11.KPI_CD       = 'SVC_CALL'
            AND    a11.ACCTG_YYYYMM = iv_yyyymm
            GROUP BY a11.ACCTG_YYYYMM, a11.SUBSDR_CD, a12.SUBSDR_SHRT_NAME
            UNION ALL
            -- RTAT : SVC_RTAT
            SELECT '1560'                                           AS PRCS_SEQ          /* 프로세스일련순서         */
                  ,'ARES'                                           AS RS_MODULE_CD      /* ARES모듈코드             */
                  ,'SMART_SVC'                                      AS RS_CLSF_ID        /* ARES분류아이디           */
                  ,'SVC_RTAT'                                       AS RS_TYPE_CD        /* ARES유형코드             */
                  ,'SVC_RTAT'                                       AS RS_TYPE_NAME      /* ARES유형명               */
                  ,a11.DIV_CD                                       AS DIV_CD            /* 사업부코드               */
                  ,a11.ACCTG_YYYYMM                                 AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,'SVC_RTAT'                                       AS CD_DESC           /* 코드설명                 */
                  ,NULL                                             AS SORT_SEQ          /* 정렬일련순서             */
                  ,'Y'                                              AS USE_FLAG          /* 사용여부                 */
                  ,a11.SUBSDR_CD                                    AS ATTRIBUTE1_VALUE  /* Subsidiary               */
                  ,a13.SUBSDR_SHRT_NAME                             AS ATTRIBUTE2_VALUE  /* Subsidiary Name          */
                  ,a11.DIV_CD                                       AS ATTRIBUTE3_VALUE  /* Division                 */
                  ,a12.DIV_SHRT_NAME                                AS ATTRIBUTE4_VALUE  /* Division Name            */
                  ,DECODE(SUM(a11.DENOM_VALUE),0,0,SUM(a11.NUMR_VALUE) / SUM(a11.DENOM_VALUE)) 
                                                                    AS ATTRIBUTE5_VALUE  /* RTAT                     */
                  ,SUM(a11.NUMR_VALUE)                              AS ATTRIBUTE6_VALUE  /* Total Repair Lead Time   */
                  ,SUM(a11.DENOM_VALUE)                             AS ATTRIBUTE7_VALUE  /* Total Repair             */
                  ,NULL                                             AS ATTRIBUTE8_VALUE  /* 예비속성8값              */
                  ,NULL                                             AS ATTRIBUTE9_VALUE  /* 예비속성9값              */
                  ,NULL                                             AS ATTRIBUTE10_VALUE /* 예비속성10값             */
                  ,NULL                                             AS ATTRIBUTE11_VALUE /* 예비속성11값             */
                  ,NULL                                             AS ATTRIBUTE12_VALUE /* 예비속성12값             */
                  ,sysdate                                          AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                           AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,'ares'                                           AS CREATION_USR_NAME /* 생성사용자명             */
                  ,sysdate                                          AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                           AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_APP.NV_DWW_SVC_KPI_MONTHLY_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_DIV_LEAF_M a12
                                ON (a11.DIV_CD = a12.DIV_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SUBSDR_M a13
                                ON (a11.SUBSDR_CD = a13.SUBSDR_CD)
            WHERE  a11.KPI_CD       = 'SVC_RTAT'
            AND    a11.ACCTG_YYYYMM = iv_yyyymm
            GROUP BY a11.ACCTG_YYYYMM, a11.SUBSDR_CD, a13.SUBSDR_SHRT_NAME, a11.DIV_CD, a12.DIV_SHRT_NAME
            UNION ALL
            -- Net Service Expense Rate : SVC_NET_COST
            SELECT '1560'                                           AS PRCS_SEQ          /* 프로세스일련순서         */
                  ,'ARES'                                           AS RS_MODULE_CD      /* ARES모듈코드             */
                  ,'SMART_SVC'                                      AS RS_CLSF_ID        /* ARES분류아이디           */
                  ,'SVC_NET_COST'                                   AS RS_TYPE_CD        /* ARES유형코드             */
                  ,'SVC_NET_COST'                                   AS RS_TYPE_NAME      /* ARES유형명               */
                  ,'*' /*a11.DIV_CD*/                               AS DIV_CD            /* 사업부코드               */
                  ,a11.CLOSING_YYYYMM                               AS BASE_YYYYMMDD     /* 기준년월일               */
                  ,'SVC_NET_COST'                                   AS CD_DESC           /* 코드설명                 */
                  ,NULL                                             AS SORT_SEQ          /* 정렬일련순서             */
                  ,'Y'                                              AS USE_FLAG          /* 사용여부                 */
                  ,a11.SUBSDR_CD                                    AS ATTRIBUTE1_VALUE  /* Subsidiary               */
                  ,a15.SUBSDR_SHRT_NAME                             AS ATTRIBUTE2_VALUE  /* Subsidiary Name          */
                  ,'*'  /*a11.DIV_CD*/                              AS ATTRIBUTE3_VALUE  /* Division                 */
                  ,NULL /*a13.DIV_SHRT_NAME*/                       AS ATTRIBUTE4_VALUE  /* Division Name            */
                  ,SUM(a11.CURRM_USD_AMT)                           AS ATTRIBUTE5_VALUE  /* Curr. Amt (USD)          */
                  ,NULL                                             AS ATTRIBUTE6_VALUE  /* 예비속성6값              */
                  ,NULL                                             AS ATTRIBUTE7_VALUE  /* 예비속성7값              */
                  ,a12.NET_SVC_CAT_CD                               AS ATTRIBUTE8_VALUE  /* Net SVC Categoy          */
                  ,a16.NET_SVC_CAT_NAME                             AS ATTRIBUTE9_VALUE  /* Net SVC Categoy Name     */
                  ,a11.NET_SVC_CAT_CD                               AS ATTRIBUTE10_VALUE /* Net SVC Detail           */
                  ,a12.NET_SVC_DETL_NAME                            AS ATTRIBUTE11_VALUE /* Net SVC Detail Name      */
                  ,NULL                                             AS ATTRIBUTE12_VALUE /* 예비속성12값             */
                  ,sysdate                                          AS CREATION_DATE     /* 생성일자                 */
                  ,'ares'                                           AS CREATION_USR_ID   /* 생성사용자아이디         */
                  ,'ares'                                           AS CREATION_USR_NAME /* 생성사용자명             */
                  ,sysdate                                          AS LAST_UPD_DATE     /* 최종갱신일자             */
                  ,'ares'                                           AS LAST_UPD_USR_ID   /* 최종갱신사용자아이디     */
            FROM   NPT_DW_MGR.TB_DWW_NET_SVC_H a11
                   LEFT OUTER JOIN NPT_APP.NV_DWD_NET_SVC_DETL_M a12
                                ON (a11.NET_SVC_CAT_CD = a12.NET_SVC_DETL_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_DIV_LEAF_M a13
                                ON (a11.DIV_CD = a13.DIV_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SCENARIO_TYPE_M a14
                                ON (a11.SCENARIO_TYPE_CD = a14.SCENARIO_TYPE_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_SUBSDR_M a15
                                ON (a11.SUBSDR_CD = a15.SUBSDR_CD)
                   LEFT OUTER JOIN NPT_APP.NV_DWD_NET_SVC_CAT_M a16
                                ON (a12.NET_SVC_CAT_CD = a16.NET_SVC_CAT_CD)
            WHERE  a11.SCENARIO_TYPE_CD in ('AC0')
            AND    a11.CLOSING_YYYYMM = iv_yyyymm
            GROUP BY a11.SUBSDR_CD, a15.SUBSDR_SHRT_NAME/*, a11.DIV_CD, a13.DIV_SHRT_NAME*/, a12.NET_SVC_CAT_CD, a16.NET_SVC_CAT_NAME, 
                     a11.NET_SVC_CAT_CD, a12.NET_SVC_DETL_NAME, a11.CLOSING_YYYYMM
            ;
            
        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('2)Insert success row : ' || vn_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            dbms_output.put_line('vv_exception ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);
        WHEN OTHERS THEN
            ROLLBACK;
            dbms_output.put_line('OTHER ERROR!! : ' || vv_err_desc);
            raise_application_error(-20001, vv_err_desc);

    END SP_RS_SMR_KPI_SVC_SUMMARY;


END pg_rs_smr_kpi_summary;
/
