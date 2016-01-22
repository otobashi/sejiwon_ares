CREATE OR REPLACE PACKAGE BODY NPT_APP.pg_rs_kpi_bep IS

    PROCEDURE SP_RS_KPI_BEP_RANGE(iv_yyyymm     IN VARCHAR2
                                 ,iv_category   IN VARCHAR2
                                 ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_bep_range                                                             */
        /* 4.기       능 :                                                                                 */
        /*                 한계이익율,영업이익율을 계산하여 각 이익율 구간대별로                           */
        /*                 KPI를 SUM하여 tb_rs_kpi_prod_h에 데이터를 생성함                                */
        /*                                                                                                 */
        /*                 ===== 모델 수 COUNT 로직 =====                                                  */
        /*                    PRODUCT_TYPE_CODE(제품유형코드) = 'SET'                                      */
        /*                    DISABLE_TYPE_CODE(단종구분코드) = '0'                                        */
        /*                    SALES_QTY <> 0                                                               */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_RANGE 데이터 삭제                                           */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.11.28 최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /*     1.1  mysik  RS       2015.09.16 C20150911_71349 ARES 구간대별 수익성 (한계이익, 영업이익) 적재 자동화 */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bep_range (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        /* Start -- 2015.09.16 C20150911_71349 ARES 구간대별 수익성 (한계이익, 영업이익) 적재 자동화 */
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0402';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';
        /* End -- 2015.09.16 C20150911_71349 ARES 구간대별 수익성 (한계이익, 영업이익) 적재 자동화 */

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log

        /*----------------------------------------
           기준월의 기존 BEP_RANGE 데이터 삭제
        ----------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN

        INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)
        /*---------------------------------------------------
           한계이익 구간별 매출액,한계이익,마케팅비용 생성
        ----------------------------------------------------*/
            WITH TEMPA( BASIS_YYYYMM, DIV_CD, MGNL_PRF_TYPE_CD,
                        MGNL_PRF_RANGE_CD, OI_RANGE_CD, SCENARIO_TYPE_CD, GRADE_NAME, OLD_NEW_CD, VIRT_MDL_FLAG_CD,MGNL_PRF_MDL_CNT,
                        GROSS_SALES_KRW_AMT, SALES_DEDUCT_KRW_AMT, NSALES_KRW_AMT, MGNL_PRF_KRW_AMT, OI_KRW_AMT,
                        GROSS_SALES_USD_AMT, SALES_DEDUCT_USD_AMT, NSALES_USD_AMT, MGNL_PRF_USD_AMT, OI_USD_AMT ) AS
            (
            --AC0
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
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
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
            AND     A11.DIV_CD NOT IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y'))
            GROUP BY  a11.ACCTG_YYYYMM  ,
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
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y', 'N', '*')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     A11.DIV_CD IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y')
            AND     a11.SCENARIO_TYPE_CD in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4'))
            GROUP BY  a11.ACCTG_YYYYMM  ,
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
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4')
            AND     A11.DIV_CD NOT IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y'))
            GROUP BY  a11.ACCTG_YYYYMM ,
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
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y', 'N', '*')
            AND     a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
            AND     a11.CURRENCY_CD in ('KRW')
            AND     A11.DIV_CD IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_MGN' AND attribute3_value = 'ALL' AND USE_FLAG = 'Y')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4'))
            GROUP BY a11.ACCTG_YYYYMM ,
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
            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N'         AS MANUAL_ADJUST_FLAG,
                     c2.cd_id   AS KPI_TYPE_CODE,
                    iv_category AS CATEGORY_CODE,
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
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            --AND    C2.cd_id in ('SALE','COI','MGN_PROFIT')
            AND    C2.cd_id in ('SALE','MGN_PROFIT','MODEL_COUNT') -- 한게이익 구간대별 매출, 한계이익
            AND    mgnl.VIRT_MDL_FLAG_CD = 'N'
            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
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


            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N'         AS MANUAL_ADJUST_FLAG,
                     c2.cd_id   AS KPI_TYPE_CODE,
                    iv_category AS CATEGORY_CODE,
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
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','COI','MGN_PROFIT','MODEL_COUNT')
            AND    mgnl.VIRT_MDL_FLAG_CD = 'N'
            --AND    mgnl.scenario_type_cd in ('PR2','PR3','PR4')
            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
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
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
            누계  데이터 생성
        ---------------------------------------------*/
            update npt_rs_mgr.tb_rs_kpi_prod_h  a
            set   a.accu_krw_amt = (
                                    select sum(b.currm_krw_amt)
                                    from npt_rs_mgr.tb_rs_kpi_prod_h  b
                                    where b.base_yyyymm between to_char(to_date(substr(a.base_yyyymm,1,4), 'YYYY'), 'YYYY')||'01'  and a.base_yyyymm
                        and   a.scenario_type_cd = b.scenario_type_cd
                        and   a.cat_cd = b.cat_cd
						            and   a.kpi_cd = b.kpi_cd
						            and   a.sub_cat_cd = b.sub_cat_cd
						            and   a.manual_adj_flag = b.manual_adj_flag
						            and   a.div_cd = b.div_cd
						            --and   a.apply_yyyymm = b.apply_yyyymm
                         )
            where a.base_yyyymm = iv_yyyymm
            and   a.cat_cd  = iv_category ;


        /*--------------------------------------------
            상위 사업부 데이터 생성 ('COI%')
        ---------------------------------------------*/
        --SP_RS_ROLLUP_PROD(iv_yyyymm, iv_category, iv_div_yyyymm);

            INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_prod_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'COI%'
                    AND    A.kpi_cd IN ('SALE','COI','MGN_PROFIT','MODEL_COUNT')
                    AND    A.manual_adj_flag = 'N'
                    AND    C.cd_clsf_id = 'RANGE_COI'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            --AND    a.kpi_cd in ('SALE','COI','MGN_PROFIT','MODEL_COUNT')
            --and    a.sub_cat_cd like 'COI%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.apply_yyyymm;

        /*--------------------------------------------
            상위 사업부 데이터 생성 ('MARGINAL_PF%')
        ---------------------------------------------*/
            INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_prod_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'MARGINAL_PF%'
                    AND    A.kpi_cd IN  ('SALE','MGN_PROFIT','MODEL_COUNT')
                    AND    A.manual_adj_flag = 'N'
                    AND    C.cd_clsf_id = 'RANGE_MGN'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    a.kpi_cd in ('SALE','MGN_PROFIT','MODEL_COUNT')
            and    a.sub_cat_cd like 'MARGINAL_PF%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.apply_yyyymm;

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_prod_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_BEP_RANGE;

    PROCEDURE SP_RS_KPI_BEP_GRADE(iv_yyyymm     IN VARCHAR2
                                 ,iv_category   IN VARCHAR2
                                 ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_bep_grade                                                             */
        /* 4.기       능 :                                                                                 */
        /*                 1. 모델 Grade별로 한계이익율을 계산하고 각 이익율 구간대별로 KPI를 SUM          */
        /*                 2. 모델 Grade별로 영업이익율을 계산하고 각 이익율 구간대별로 KPI를 SUM          */
        /*                 3. 모델 Grade별로 신/구모델 매출액 생성                                         */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_GRADE 데이터 삭제                                           */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.04 최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /*     1.1  mysik  RS       2015.09.16 C20150911_71352 ARES 모델 Grade별 수익성 적재 자동화        */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bep_grade (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        /* Start -- C20150911_71352 ARES 모델 Grade별 수익성 적재 자동화 */
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0403';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';
        /* End -- C20150911_71352 ARES 모델 Grade별 수익성 적재 자동화 */

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN



        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);


        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;


        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;


        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log


         /*----------------------------------------
           기준월의 기존 BEP_GRADE 데이터 삭제
        ----------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_grd_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_grd_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN

        INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,mdl_grd_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)

        /*-----------------------------------------
           Grade별 매출액,한계이익 생성
        ------------------------------------------*/
            WITH TEMPA( BASIS_YYYYMM, DIV_CD, MGNL_PRF_TYPE_CD,
                        MGNL_PRF_RANGE_CD, OI_RANGE_CD, SCENARIO_TYPE_CD, GRADE_NAME, OLD_NEW_CD, VIRT_MDL_FLAG_CD,
                        GROSS_SALES_KRW_AMT, SALES_DEDUCT_KRW_AMT, NSALES_KRW_AMT, MGNL_PRF_KRW_AMT, OI_KRW_AMT,
                        GROSS_SALES_USD_AMT, SALES_DEDUCT_USD_AMT, NSALES_USD_AMT, MGNL_PRF_USD_AMT, OI_USD_AMT ) AS
            (
            -- AC0
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end  VIRT_MDL_FLAG_CD,
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
            LEFT OUTER JOIN npt_rs_mgr.tb_rs_clss_cd_m a19
            ON     (a19.cd_clsf_id = 'RANGE_GRD_NEW'
            and     a19.cd_id = a11.div_cd
            and     a19.use_flag = 'Y')
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD = ( case when a19.attribute4_value = 'EXTERNAL' THEN 'EXCL'
                                                       else 'INCL'
                                                  end )
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('AC0')
            )
            GROUP BY  a11.ACCTG_YYYYMM  ,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                      case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end
            UNION ALL
            -- PR1
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end  VIRT_MDL_FLAG_CD,
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
            LEFT OUTER JOIN npt_rs_mgr.tb_rs_clss_cd_m a19
            ON     (a19.cd_clsf_id = 'RANGE_GRD_NEW'
            and     a19.cd_id = a11.div_cd
            and     a19.use_flag = 'Y')
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD = ( case when a19.attribute4_value = 'EXTERNAL' THEN 'EXCL'
                                                       else 'INCL'
                                                  end )
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4'))
            GROUP BY  a11.ACCTG_YYYYMM ,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end
            )
            -- High End
            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N' as MANUAL_ADJUST_FLAG,
                     c2.cd_id as  KPI_TYPE_CODE,
                    iv_category      CATEGORY_CODE, -- BEP_GRADE
                    case when c2.cd_id in ('SALE', 'MGN_PROFIT') then
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
                    end as CATEGORY_DETAIL_CODE,
                    NVL(upper(mgnl.GRADE_NAME), ' ') GRADE_NAME,
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
                         else 0
                    end) as curr_mon_krw_amount,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         else 0
                    end) as curr_mon_usd_amount,
                    null accu_mon_krw_amount,
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','MGN_PROFIT')
            AND    nvl(mgnl.VIRT_MDL_FLAG_CD,'N') = 'N'
            /* 2015.09.03 CSR */
            and    mgnl.GRADE_NAME <> 'Exception'
            AND    mgnl.div_cd not IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_GRD_NEW' AND attribute1_value = 'N' AND USE_FLAG = 'Y')
            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE', 'MGN_PROFIT') then
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
                  end,
                  NVL(upper(mgnl.GRADE_NAME), ' ')

            union all
            -- High End 영업이익
            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N' as MANUAL_ADJUST_FLAG,
                     c2.cd_id as  KPI_TYPE_CODE,
                    iv_category      CATEGORY_CODE, -- BEP_GRADE
                    case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
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
                    NVL(upper(mgnl.GRADE_NAME), ' ') GRADE_NAME,
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
                         else 0
                    end) as curr_mon_krw_amount,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         else 0
                    end) as curr_mon_usd_amount,
                    null accu_mon_krw_amount,
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','COI','MGN_PROFIT')
            AND    nvl(mgnl.VIRT_MDL_FLAG_CD,'N') = 'N'
            /* 2015.09.03 CSR */
            and    mgnl.GRADE_NAME <> 'Exception'
            AND    mgnl.div_cd not IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_GRD_NEW' AND attribute1_value = 'N' AND USE_FLAG = 'Y')

            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
                      case mgnl.OI_RANGE_CD
                         when '10%~'     then 'COI_10'
                         when '0%~10%'   then 'COI_0_10'
                         when '-5%~0%'   then 'COI_-5_0'
                         when '-10%~-5%' then 'COI_-10_-5'
                         when '-15%~-10%'  then 'COI_-15_-10'
                         when '~-15%'      then 'COI_-15'
                         else null
                      end
                  end,
                  NVL(upper(mgnl.GRADE_NAME), ' ')
                  ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2-2) Insert Table tb_rs_kpi_grd_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('2-2)Insert success row : ' || vn_insert_row_cnt);




        /*--------------------------------------------
            누계  데이터 생성
        ---------------------------------------------*/
            update npt_rs_mgr.tb_rs_kpi_grd_h  a
            set   a.accu_krw_amt = (
                                    select sum(b.currm_krw_amt)
                                    from npt_rs_mgr.tb_rs_kpi_grd_h  b
                                    where b.base_yyyymm between to_char(to_date(substr(a.base_yyyymm,1,4), 'YYYY'), 'YYYY')||'01'  and a.base_yyyymm
                        and   a.scenario_type_cd = b.scenario_type_cd
                        and   a.cat_cd = b.cat_cd
						            and   a.kpi_cd = b.kpi_cd
						            and   a.sub_cat_cd = b.sub_cat_cd
                        and   a.mdl_grd_cd = b.mdl_grd_cd
                        and   a.manual_adj_flag = b.manual_adj_flag
						            and   a.div_cd = b.div_cd
						             )
            where a.base_yyyymm = iv_yyyymm
            and   a.cat_cd  = iv_category ;

        /*--------------------------------------------
            상위 사업부 데이터 생성
        ---------------------------------------------*/
        --SP_RS_ROLLUP_GRADE(iv_yyyymm, iv_category, iv_div_yyyymm);
        -- COI%
            INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,mdl_grd_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.mdl_grd_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_grd_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'COI%'
                    AND    A.kpi_cd IN ('COI','SALE')
                    AND    A.manual_adj_flag = 'N'
                    --AND    C.cd_clsf_id = 'RANGE_NEW'
                    AND    C.cd_clsf_id = 'RANGE_GRD_NEW'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    A.sub_cat_cd LIKE 'COI%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.mdl_grd_cd
                     ,a.apply_yyyymm;

        -- MARGINAL_PF%
            INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,mdl_grd_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.mdl_grd_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_grd_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'MARGINAL_PF%'
                    AND    A.kpi_cd IN ('COI','SALE','MGN_PROFIT')
                    AND    A.manual_adj_flag = 'N'
                    --AND    C.cd_clsf_id = 'RANGE_NEW'
                    AND    C.cd_clsf_id = 'RANGE_GRD_NEW'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    A.sub_cat_cd LIKE 'MARGINAL_PF%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.mdl_grd_cd
                     ,a.apply_yyyymm;

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_grd_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);


    END SP_RS_KPI_BEP_GRADE;

    PROCEDURE SP_RS_KPI_BEP_NEWOLD(iv_yyyymm     IN VARCHAR2
                                  ,iv_category   IN VARCHAR2
                                  ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_bep_newold                                                            */
        /* 4.기       능 :                                                                                 */
        /*                 1. 신/구 모델별로 한계이익율을 계산하고 각 이익율 구간대별로 KPI를 SUM          */
        /*                 2. 신/구 모델별로 영업이익율을 계산하고 각 이익율 구간대별로 KPI를 SUM          */
        /*                 3. 출시 1년, 2년 경과 모델에 대한 매출액을 생성함                               */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_NEW_OLD 데이터 삭제                                         */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.10 최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /*     1.1  mysik  RS       2015.09.16 C20150911_71355 ARES 신/구모델별 수익성 적재 자동화         */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bep_newold (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        /* Start -- 2015.09.16 C20150911_71355 ARES 신/구모델별 수익성 적재 자동화 */
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0404';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';
        /* End -- 2015.09.16 C20150911_71355 ARES 신/구모델별 수익성 적재 자동화 */
        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log
        /*----------------------------------------
           기준월의 기존 BEP_NEW_OLD 데이터 삭제
        ----------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_grd_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_grd_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN

        INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,mdl_grd_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)

        /*-----------------------------------------
           신,구 모델별  매출액,한계이익 생성
        ------------------------------------------*/
            WITH TEMPA( BASIS_YYYYMM, DIV_CD, MGNL_PRF_TYPE_CD,
                        MGNL_PRF_RANGE_CD, OI_RANGE_CD, SCENARIO_TYPE_CD, GRADE_NAME, OLD_NEW_CD, VIRT_MDL_FLAG_CD,
                        GROSS_SALES_KRW_AMT, SALES_DEDUCT_KRW_AMT, NSALES_KRW_AMT, MGNL_PRF_KRW_AMT, OI_KRW_AMT,
                        GROSS_SALES_USD_AMT, SALES_DEDUCT_USD_AMT, NSALES_USD_AMT, MGNL_PRF_USD_AMT, OI_USD_AMT ) AS
            (
            -- AC0
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end  VIRT_MDL_FLAG_CD,
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
            LEFT OUTER JOIN npt_rs_mgr.tb_rs_clss_cd_m a19
            ON     (a19.cd_clsf_id = 'RANGE_GRD_NEW'
            and     a19.cd_id = a11.div_cd
            and     a19.use_flag = 'Y')
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD = ( case when a19.attribute4_value = 'EXTERNAL' THEN 'EXCL'
                                                       else 'INCL'
                                                  end )
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('AC0'))
            GROUP BY  a11.ACCTG_YYYYMM  ,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                      case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else a11.VIRT_MDL_FLAG end
            UNION ALL
            -- PR1
            SELECT  a11.ACCTG_YYYYMM  BASIS_YYYYMM,
                    a11.DIV_CD  DIV_CD,
                    a11.ACCU6_LOSS_FLAG  MGNL_PRF_TYPE_CD,
                    a11.MGNL_PRF_RANGE  MGNL_PRF_RANGE_CD,
                    a11.OI_RANGE  OI_RANGE_CD,
                    a11.SCENARIO_TYPE_CD SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  GRD_NAME,
                    a12.OLD_NEW_CD  OLD_NEW_CD,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end  VIRT_MDL_FLAG_CD,
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
            LEFT OUTER JOIN npt_rs_mgr.tb_rs_clss_cd_m a19
            ON     (a19.cd_clsf_id = 'RANGE_GRD_NEW'
            and     a19.cd_id = a11.div_cd
            and     a19.use_flag = 'Y')
            WHERE  (a11.ACCTG_YYYYMM = iv_yyyymm
            AND     a11.CONSLD_SALES_MDL_FLAG in ('Y')
            AND     a11.VRNC_ALC_INCL_EXCL_CD = ( case when a19.attribute4_value = 'EXTERNAL' THEN 'EXCL'
                                                       else 'INCL'
                                                  end )
            AND     a11.CURRENCY_CD in ('KRW')
            AND     a11.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4'))
            GROUP BY  a11.ACCTG_YYYYMM ,
                    a11.DIV_CD  ,
                    a11.ACCU6_LOSS_FLAG  ,
                    a11.MGNL_PRF_RANGE  ,
                    a11.OI_RANGE  ,
                    a11.SCENARIO_TYPE_CD,
                    a18.ATTRIBUTE_NAME  ,
                    a12.OLD_NEW_CD  ,
                    case when a11.MDL_SFFX_CD like 'VM-%.CPS' then 'Y' else DECODE(a11.VIRT_MDL_FLAG,'*','N') end
            )
            -- NEW OLD
            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N' as MANUAL_ADJUST_FLAG,
                     c2.cd_id as  KPI_TYPE_CODE,
                    iv_category AS  CATEGORY_CODE,
                    case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
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
                    end as CATEGORY_DETAIL_CODE,
                    NVL(mgnl.OLD_NEW_CD, ' ') OLD_NEW_CD,
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
                         else 0
                    end) as curr_mon_krw_amount,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         else 0
                    end) as curr_mon_usd_amount,
                    null accu_mon_krw_amount,
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','COI','MGN_PROFIT')
            AND    nvl(mgnl.VIRT_MDL_FLAG_CD,'N') = 'N'

            /* 2015.09.03 CSR */
            and    mgnl.GRADE_NAME <> 'Exception'
            AND    mgnl.div_cd not IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_GRD_NEW' AND attribute1_value = 'N' AND USE_FLAG = 'Y')

            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
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
                  end,
                  NVL(mgnl.OLD_NEW_CD, ' ')



            union all
            -- NEW OLD 영업이익
            SELECT
                    mgnl.BASIS_YYYYMM,
                    mgnl.scenario_type_cd SCENARIO_CODE,
                    mgnl.div_cd DIVISION_CODE,
                    'N' as MANUAL_ADJUST_FLAG,
                     c2.cd_id as  KPI_TYPE_CODE,
                    iv_category AS  CATEGORY_CODE,
                    case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
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
                    NVL(mgnl.OLD_NEW_CD, ' ') OLD_NEW_CD,
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
                         else 0
                    end) as curr_mon_krw_amount,
                    sum(case when c2.cd_id = 'SALE'  then
                              mgnl.NSALES_USD_AMT
                         when c2.cd_id = 'COI' then
                              mgnl.OI_USD_AMT
                         when c2.cd_id = 'MGN_PROFIT' then
                              mgnl.MGNL_PRF_USD_AMT
                         else 0
                    end) as curr_mon_usd_amount,
                    null accu_mon_krw_amount,
                    null accu_mon_usd_amount,
                    sysdate, 'ares',sysdate,'ares'
            from   TEMPA mgnl
                  ,npt_rs_mgr.tb_rs_clss_cd_m C2
            WHERE  C2.cd_clsf_id = 'KPI_TYPE'
            AND    C2.cd_id in ('SALE','COI','MGN_PROFIT')
            AND    nvl(mgnl.VIRT_MDL_FLAG_CD,'N') = 'N'

            /* 2015.09.03 CSR */
            and    mgnl.GRADE_NAME <> 'Exception'
            AND    mgnl.div_cd not IN (SELECT CD_ID FROM npt_rs_mgr.tb_rs_clss_cd_m WHERE cd_clsf_id = 'RANGE_GRD_NEW' AND attribute1_value = 'N' AND USE_FLAG = 'Y')

            GROUP BY
                  mgnl.BASIS_YYYYMM,
                  mgnl.scenario_type_cd,
                  mgnl.div_cd,
                  c2.cd_id,
                  case when c2.cd_id in ('SALE','COI', 'MGN_PROFIT') then
                      case mgnl.OI_RANGE_CD
                         when '10%~'     then 'COI_10'
                         when '0%~10%'   then 'COI_0_10'
                         when '-5%~0%'   then 'COI_-5_0'
                         when '-10%~-5%' then 'COI_-10_-5'
                         when '-15%~-10%'  then 'COI_-15_-10'
                         when '~-15%'      then 'COI_-15'
                         else null
                      end
                  end,
                  NVL(mgnl.OLD_NEW_CD, ' ')
                  ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_grd_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
            누계  데이터 생성
        ---------------------------------------------*/
            update npt_rs_mgr.tb_rs_kpi_grd_h  a
            set   a.accu_krw_amt = (
                                    select sum(b.currm_krw_amt)
                                    from npt_rs_mgr.tb_rs_kpi_grd_h  b
                                    where b.base_yyyymm between to_char(to_date(substr(a.base_yyyymm,1,4), 'YYYY'), 'YYYY')||'01'  and a.base_yyyymm
                        and   a.scenario_type_cd = b.scenario_type_cd
                        and   a.cat_cd = b.cat_cd
						            and   a.kpi_cd = b.kpi_cd
						            and   a.sub_cat_cd = b.sub_cat_cd
                        and   a.mdl_grd_cd = b.mdl_grd_cd
                        and   a.manual_adj_flag = b.manual_adj_flag
						            and   a.div_cd = b.div_cd
						            --and   a.yyyymm = b.yyyymm
                         )
            where a.base_yyyymm = iv_yyyymm
            and   a.cat_cd  = iv_category ;



        /*--------------------------------------------
            상위 사업부 데이터 생성
        ---------------------------------------------*/
        --SP_RS_ROLLUP_GRADE(iv_yyyymm, iv_category, iv_div_yyyymm);
        -- COI%
            INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,mdl_grd_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.mdl_grd_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_grd_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'COI%'
                    AND    A.kpi_cd IN ('COI','SALE')
                    AND    A.manual_adj_flag = 'N'
                    --AND    C.cd_clsf_id = 'RANGE_NEW'
                    AND    C.cd_clsf_id = 'RANGE_GRD_NEW'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    A.sub_cat_cd LIKE 'COI%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.mdl_grd_cd
                     ,a.apply_yyyymm;

        -- MARGINAL_PF%
            INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,mdl_grd_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.mdl_grd_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   (
                    SELECT A.*
                    FROM   npt_rs_mgr.tb_rs_kpi_grd_h a,
                           npt_rs_mgr.tb_rs_clss_cd_m C
                    WHERE  A.base_yyyymm  = iv_yyyymm
                    AND    A.cat_cd IN (iv_category)
                    AND    A.sub_cat_cd LIKE 'MARGINAL_PF%'
                    AND    A.kpi_cd IN ('COI','SALE','MGN_PROFIT')
                    AND    A.manual_adj_flag = 'N'
                    --AND    C.cd_clsf_id = 'RANGE_NEW'
                    AND    C.cd_clsf_id = 'RANGE_GRD_NEW'
                    AND    C.ATTRIBUTE1_VALUE = 'Y'
                    AND    C.CD_ID = A.div_cd
                    ) A
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    A.sub_cat_cd LIKE 'MARGINAL_PF%'
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.mdl_grd_cd
                     ,a.apply_yyyymm;

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_grd_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_BEP_NEWOLD;

    PROCEDURE SP_RS_KPI_BEP_PROD(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2
                                ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_bep_prod                                                              */
        /* 4.기       능 :                                                                                 */
        /*                 Product별로 BEP 매출액, 한계이익, 영업이익을 집계하여                           */
        /*                 tb_rs_kpi_prod_h에 데이터를 생성함                                              */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_PROD 데이터 삭제                                            */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.05 최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /*     1.1  mysik  RS       2015.09.22 C20150918_76956_ARES 제품별 수익성 적재 자동화              */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bep_prod (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        /* Start -- 2015.09.22 C20150918_76956_ARES 제품별 수익성 적재 자동화  */
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0405';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';
        /* End -- 2015.09.22 C20150918_76956_ARES 제품별 수익성 적재 자동화  */

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;




dbms_output.put_line('0)step0 : ' || vv_pgm_cd);

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

dbms_output.put_line('1)step1 : ' || vn_job_log_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log
        /*----------------------------------------
           기준월의 기존 BEP_PROD 데이터 삭제
        ----------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN

        INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)
        WITH temp1 (base_yyyymm, scenario_type_cd, div_cd, kpi_cd, sub_cat_cd, currm_krw_amt, currm_usd_amt, accu_krw_amt, accu_usd_amt) AS
        (
        /*------------------------------------------------------
              사업부, PRODUCT(Level_Key_Code)별 데이터 생성
        -------------------------------------------------------*/
         SELECT M.BASIS_YYYYMM as base_yyyymm,
                M.SCENARIO_TYPE_CODE as scenario_type_cd,
                M.GBU_CODE as div_cd,
                C.CD_ID as kpi_cd,
                M.CATEGORY_DETAIL_CODE as sub_cat_cd,
                sum(CASE C.CD_ID
                     WHEN 'SALE' THEN
                          v_net_sales_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                          v_mgn_profit_krw_amt
                     WHEN 'COI' THEN
                          v_op_inc_krw_amt
                     WHEN 'SALE_QTY' THEN
                          v_sales_qty_krw
                     WHEN 'GROSS_SALE' THEN
                          v_gross_sales_krw_amt
                     ELSE 0 END) as currm_krw_amt,
                sum(CASE C.CD_ID
                     WHEN 'SALE' THEN
                          v_net_sales_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                          v_mgn_profit_usd_amt
                     WHEN 'COI' THEN
                          v_op_inc_usd_amt
                     WHEN 'SALE_QTY' THEN
                          v_sales_qty_usd
                     WHEN 'GROSS_SALE' THEN
                          v_gross_sales_usd_amt
                     ELSE 0 END) as currm_usd_amt,
                sum(CASE C.CD_ID
                     WHEN 'SALE' THEN
                          v_net_sales_accu_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                          v_mgn_profit_accu_krw_amt
                     WHEN 'COI' THEN
                          v_op_inc_accu_krw_amt
                     WHEN 'SALE_QTY' THEN
                          v_sales_qty_accu_krw
                     WHEN 'GROSS_SALE' THEN
                          v_gross_sales_accu_krw_amt
                     ELSE 0 END) as accu_krw_amt,
                sum(CASE C.CD_ID
                     WHEN 'SALE' THEN
                          v_net_sales_accu_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                          v_mgn_profit_accu_usd_amt
                     WHEN 'COI' THEN
                          v_op_inc_accu_usd_amt
                     WHEN 'SALE_QTY' THEN
                          v_sales_qty_accu_usd
                     WHEN 'GROSS_SALE' THEN
                          v_gross_sales_accu_usd_amt
                     ELSE 0 END) as accu_usd_amt
         FROM(
          SELECT A.acctg_yyyymm as BASIS_YYYYMM,
                 A.scenario_type_cd as SCENARIO_TYPE_CODE,
                 A.div_cd as GBU_CODE,
                 B.prod_lvl_key_cd as CATEGORY_DETAIL_CODE,
                 SUM(v_net_sales_krw_amt * minus_plus_sign_value) as v_net_sales_krw_amt,
                 SUM(v_net_sales_usd_amt * minus_plus_sign_value) as v_net_sales_usd_amt,
                 SUM(v_mgn_profit_krw_amt * minus_plus_sign_value) as v_mgn_profit_krw_amt,
                 SUM(v_mgn_profit_usd_amt * minus_plus_sign_value) as v_mgn_profit_usd_amt,
                 SUM(v_op_inc_krw_amt * minus_plus_sign_value) as v_op_inc_krw_amt,
                 SUM(v_op_inc_usd_amt * minus_plus_sign_value) as v_op_inc_usd_amt,
                 SUM(v_sales_qty_krw * minus_plus_sign_value) as v_sales_qty_krw,
                 SUM(v_sales_qty_usd * minus_plus_sign_value) as v_sales_qty_usd,
                 SUM(v_gross_sales_krw_amt * minus_plus_sign_value) as v_gross_sales_krw_amt,
                 SUM(v_gross_sales_usd_amt * minus_plus_sign_value) as v_gross_sales_usd_amt,
                 SUM(v_net_sales_accu_krw_amt * minus_plus_sign_value) as v_net_sales_accu_krw_amt,
                 SUM(v_net_sales_accu_usd_amt * minus_plus_sign_value) as v_net_sales_accu_usd_amt,
                 SUM(v_mgn_profit_accu_krw_amt * minus_plus_sign_value) as v_mgn_profit_accu_krw_amt,
                 SUM(v_mgn_profit_accu_usd_amt * minus_plus_sign_value) as v_mgn_profit_accu_usd_amt,
                 SUM(v_op_inc_accu_krw_amt * minus_plus_sign_value) as v_op_inc_accu_krw_amt,
                 SUM(v_op_inc_accu_usd_amt * minus_plus_sign_value) as v_op_inc_accu_usd_amt,
                 SUM(v_sales_qty_accu_krw * minus_plus_sign_value) as v_sales_qty_accu_krw,
                 SUM(v_sales_qty_accu_usd * minus_plus_sign_value) as v_sales_qty_accu_usd,
                 SUM(v_gross_sales_accu_krw_amt * minus_plus_sign_value) as v_gross_sales_accu_krw_amt,
                 SUM(v_gross_sales_accu_usd_amt * minus_plus_sign_value) as v_gross_sales_accu_usd_amt
        FROM  (/*----- 실적 집계 -----*/
               --SELECT acctg_yyyymm
               SELECT /*+ parallel(a 32) parallel(b 32) */ acctg_yyyymm
                     ,scenario_type_cd
                     ,div_cd
                     ,prod_lvl4_cd
                     ,mdl_sffx_cd
                     ,SUM(currm_krw_nsales_amt) as v_net_sales_krw_amt
                     ,SUM(currm_usd_nsales_amt) as v_net_sales_usd_amt
                     ,SUM(currm_krw_mgnl_prf_amt) as v_mgn_profit_krw_amt
                     ,SUM(currm_usd_mgnl_prf_amt) as v_mgn_profit_usd_amt
                     ,SUM(currm_krw_oi_amt) as v_op_inc_krw_amt
                     ,SUM(currm_usd_oi_amt) as v_op_inc_usd_amt
                     ,SUM(currm_sales_qty) as v_sales_qty_krw
                     ,SUM(currm_sales_qty) as v_sales_qty_usd
                     ,SUM(currm_krw_gross_sales_amt) as v_gross_sales_krw_amt
                     ,SUM(currm_usd_gross_sales_amt) as v_gross_sales_usd_amt
                     ,SUM(accum_krw_nsales_amt) as v_net_sales_accu_krw_amt
                     ,SUM(accum_usd_nsales_amt) as v_net_sales_accu_usd_amt
                     ,SUM(accum_krw_mgnl_prf_amt) as v_mgn_profit_accu_krw_amt
                     ,SUM(accum_usd_mgnl_prf_amt) as v_mgn_profit_accu_usd_amt
                     ,SUM(accum_krw_oi_amt) as v_op_inc_accu_krw_amt
                     ,SUM(accum_usd_oi_amt) as v_op_inc_accu_usd_amt
                     ,SUM(accum_sales_qty) as v_sales_qty_accu_krw
                     ,SUM(accum_sales_qty) as v_sales_qty_accu_usd
                     ,SUM(accum_krw_gross_sales_amt) as v_gross_sales_accu_krw_amt
                     ,SUM(accum_usd_gross_sales_amt) as v_gross_sales_accu_usd_amt
               FROM  TB_APO_BEP_MDL_CUST_PRFT_D   -- 실적 table from CPS
               WHERE acctg_yyyymm = iv_yyyymm
               and   scenario_type_cd = 'AC0'
               and   oth_sales_incl_excl_cd = 'N' -- 기타매출포함제외코드(N: 기타매출포함, Y: 기타매출제외)
               and   vrnc_alc_incl_excl_cd = 'Y'  -- 차액배부포함제외코드(N: 차액배부전, Y: 차액배부후)
               and   consld_sales_mdl_flag = 'Y'  -- 연결매출모델여부
               and   mdl_sffx_cd not like 'VM-%.CPS'
               GROUP BY acctg_yyyymm
                       ,scenario_type_cd
                       ,div_cd
                       ,prod_lvl4_cd
                       ,mdl_sffx_cd
               UNION ALL
               /*----- 이동계획 집계 -----*/
               --SELECT iv_yyyymm as acctg_yyyymm
               SELECT /*+ leading(a) use_hash(a b) parallel(a 32) parallel(b 32) */  iv_yyyymm as acctg_yyyymm
                     ,'PR'||months_between(add_months(to_date(A.pln_yyyymm, 'YYYYMM'),1), to_date(A.pln_period_yyyymm, 'YYYYMM')) as scenario_type_cd
                     ,A.div_cd
                     ,B.prod_lvl4_cd
                     ,A.mdl_sffx_cd
                     ,sum(decode(A.bep_idx_cd, 'BEP20000000', A.krw_amt, 0)) as v_net_sales_krw_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP20000000', A.usd_amt, 0)) as v_net_sales_usd_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP50000000', A.krw_amt, 0)) as v_mgn_profit_krw_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP50000000', A.usd_amt, 0)) as v_mgn_profit_usd_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP60000000', A.krw_amt, 0)) as v_op_inc_krw_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP60000000', A.usd_amt, 0)) as v_op_inc_usd_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP10000000', A.krw_amt, 0)) as v_sales_qty_krw
                     ,sum(decode(A.bep_idx_cd, 'BEP10000000', A.usd_amt, 0)) as v_sales_qty_usd
                     ,sum(decode(A.bep_idx_cd, 'BEP20060000', A.krw_amt, 0)) as v_gross_sales_krw_amt
                     ,sum(decode(A.bep_idx_cd, 'BEP20060000', A.usd_amt, 0)) as v_gross_sales_usd_amt
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                     ,null
                from (select pln_period_yyyymm
                            ,pln_yyyymm
                            ,div_cd
                            ,subsdr_cd
                            ,mdl_sffx_cd
                            ,bep_idx_cd
                            ,SUM(krw_var_amt            +
                                 krw_var_mtrx_adj_amt   +
                                 krw_var_mdl_adj_amt    +
                                 krw_var_usr_dimpos_amt +
                                 krw_var_comn_alc_amt   +
                                 krw_fix_amt            +
                                 krw_fix_mtrx_adj_amt   +
                                 krw_fix_mdl_adj_amt    +
                                 krw_fix_usr_dimpos_amt +
                                 krw_fix_comn_alc_amt   ) krw_amt
                            ,SUM(usd_var_amt            +
                                 usd_var_mtrx_adj_amt   +
                                 usd_var_mdl_adj_amt    +
                                 usd_var_usr_dimpos_amt +
                                 usd_var_comn_alc_amt   +
                                 usd_fix_amt            +
                                 usd_fix_mtrx_adj_amt   +
                                 usd_fix_mdl_adj_amt    +
                                 usd_fix_usr_dimpos_amt +
                                 usd_fix_comn_alc_amt   ) usd_amt
                      from  TB_RFC_MDL_CUST_BEP_S     -- 이동계획 table from RF
                      where pln_period_yyyymm = to_char(add_months(to_date(iv_yyyymm, 'YYYYMM'), 1), 'YYYYMM')
                      and   intrnl_sales_flag = 'N'   -- 내부매출여부(Y: Internal, N: External)
                      and   condl_sales_cd = 'N'      -- 검수도매출코드(Y: 검수도, N: 인수도)
                      and   bep_idx_cd in ('BEP20000000', 'BEP50000000', 'BEP60000000', 'BEP10000000', 'BEP20060000')
                      and   mdl_sffx_cd not like 'VM-%.CPS'
                      group by pln_period_yyyymm
                              ,pln_yyyymm
                              ,div_cd
                              ,subsdr_cd
                              ,mdl_sffx_cd
                              ,bep_idx_cd
                     ) A
                INNER JOIN TB_CM_SUBSDR_MDL_PERIOD_H  B -- 법인모델마스터
                on  B.mgt_type_cd = 'CM'
                and B.acctg_yyyymm = '*'
                and B.acctg_week = '*'
                and B.temp_flag = 'N'
                and B.subsdr_cd = A.subsdr_cd
                and B.div_cd = A.div_cd
                and B.mdl_sffx_cd = A.mdl_sffx_cd
                group by A.pln_period_yyyymm
                        ,A.pln_yyyymm
                        ,A.div_cd
                        ,B.prod_lvl4_cd
                        ,A.mdl_sffx_cd
          )  A
          INNER JOIN npt_rs_mgr.TB_RS_DIV_PROD_M B
          ON  B.div_cd = A.div_cd
          AND B.prod_lvl4_cd = A.prod_lvl4_cd
          AND ( B.mapp_type_cd = 'P'
          OR    B.mapp_type_cd = 'M' AND B.mdl_sffx_cd = A.mdl_sffx_cd )
          WHERE A.scenario_type_cd in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
          GROUP BY A.acctg_yyyymm,
                   A.scenario_type_cd,
                   A.div_cd,
                   B.prod_lvl_key_cd
       ) M  -- end of 실적 & 이동계획
       /*
       LEFT OUTER JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C
       ON   C.CD_CLSF_ID = 'KPI_TYPE'
       AND  C.CD_ID in ('SALE','MGN_PROFIT','COI','SALE_QTY','GROSS_SALE')
       */
        ,npt_rs_mgr.tb_rs_clss_cd_m C
        where  C.cd_clsf_id = 'KPI_TYPE'
        and C.cd_id in ('SALE','MGN_PROFIT','COI','SALE_QTY','GROSS_SALE')
       GROUP BY M.BASIS_YYYYMM,
                M.SCENARIO_TYPE_CODE,
                M.GBU_CODE,
                C.CD_ID,
                M.CATEGORY_DETAIL_CODE
        )
        SELECT temp1.base_yyyymm,
               temp1.scenario_type_cd,
               temp1.div_cd,
               'N',
               temp1.kpi_cd,
               iv_category,
               temp1.sub_cat_cd,
               CASE temp1.scenario_type_cd
                    WHEN 'AC0' THEN temp1.base_yyyymm
                    WHEN 'PR1' THEN to_char(add_months(to_date(temp1.base_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                    WHEN 'PR2' THEN to_char(add_months(to_date(temp1.base_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                    WHEN 'PR3' THEN to_char(add_months(to_date(temp1.base_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                    WHEN 'PR4' THEN to_char(add_months(to_date(temp1.base_yyyymm,'YYYYMM'), 4), 'YYYYMM')
               END,
               temp1.currm_krw_amt,
               temp1.currm_usd_amt,
               temp1.accu_krw_amt,
               temp1.accu_usd_amt,
               SYSDATE,
               'ares',
               SYSDATE,
               'ares'
        FROM temp1;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('2)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
            상위 사업부 데이터 생성
        ---------------------------------------------*/
        SP_RS_ROLLUP_PROD(iv_yyyymm, iv_category, iv_div_yyyymm);

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_prod_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);


    END SP_RS_KPI_BEP_PROD;

PROCEDURE SP_RS_KPI_BEP_ENHANCE_BIZ(iv_yyyymm     IN VARCHAR2
                                       ,iv_category   IN VARCHAR2
                                       ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_bep_enhance_biz                                                       */
        /* 4.기       능 :                                                                                 */
        /*                 육성사업별로 BEP 매출액, 한계이익, 영업이익, SALES_DEDUCTION을 집계하여         */
        /*                 tb_rs_kpi_prod_h에 데이터를 생성함                                              */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_ENHANCE_BIZ 데이터 삭제                                     */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS     2014.12.05 최초작성                                                      */
        /*                                   실적 및 이동계획 소스테이블이 바뀔 수 있음                    */
        /*     1.1  mysik  RS     2015.09.22 C20150918_76961_ARES 육성사업구분별 수익성 적재 자동화        */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bep_enhance_biz (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt          NUMBER;
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Log Variable 추가
        /* Start -- 2015.09.22 C20150918_76961_ARES 육성사업구분별 수익성 적재 자동화 */
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0401';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';
        /* End -- 2015.09.22 C20150918_76961_ARES 육성사업구분별 수익성 적재 자동화 */

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log

        /*---------------------------------------------
           기준월의 기존 BEP_ENHANCE_BIZ 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

                WITH bep_temp AS
                 ( /*----- 실적 집계 -----*/

                  SELECT /*+ materialize */
                   a11.acctg_yyyymm
                  ,a11.scenario_type_cd
                  ,a11.div_cd
                  ,
                    --a12.PROD_LVL4_CD  prod_lvl4_cd,
                    a11.mdl_sffx_cd mdl_sffx_cd
                  ,a11.au_cd
                  ,a11.subsdr_cd
                  ,a11.sales_subsdr_rnr_cd
                  ,a11.production_subsdr_cd
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'KRW' THEN
                             a11.nsales_amt
                            ELSE
                             0
                        END) nsales_krw_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'USD' THEN
                             a11.nsales_amt
                            ELSE
                             0
                        END) nsales_usd_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'KRW' THEN
                             a11.oi_amt
                            ELSE
                             0
                        END) oi_krw_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'USD' THEN
                             a11.oi_amt
                            ELSE
                             0
                        END) oi_usd_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'KRW' THEN
                             a11.mgnl_prf_amt
                            ELSE
                             0
                        END) mgnl_prf_krw_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'USD' THEN
                             a11.mgnl_prf_amt
                            ELSE
                             0
                        END) mgnl_prf_usd_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'KRW' THEN
                             a11.sales_deduct_amt
                            ELSE
                             0
                        END) sales_deduct_krw_amt
                  ,SUM(CASE
                            WHEN a11.currency_cd = 'USD' THEN
                             a11.sales_deduct_amt
                            ELSE
                             0
                        END) sales_deduct_usd_amt
                  FROM   npt_app.nv_dww_con_bep_summ_dw_s a11
                  LEFT   OUTER JOIN npt_app.nv_dwd_prft_confm_scenario_h a13
                  ON     (a11.acctg_yyyymm = a13.acctg_yyyymm AND a11.div_cd = a13.div_cd AND a11.scenario_type_cd = a13.scenario_type_cd)
                  WHERE  a11.scenario_type_cd IN ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
                  AND    a11.acctg_yyyymm IN (iv_yyyymm)
                        --and a11.DIV_CD NOT in ('GNT','PDT')
                  AND    a11.consld_sales_mdl_flag IN ('Y')
                  AND    a11.currm_accum_type_cd IN ('CURRM') -- ,'ACCUM')
                  AND    a11.vrnc_alc_incl_excl_cd IN ('INCL')
                  AND    a11.currency_cd IN ('KRW', 'USD') -- ,'USD')
                  AND    a13.confirm_flag = 'Y'
                  GROUP  BY a11.acctg_yyyymm
                            ,a11.scenario_type_cd
                            ,a11.div_cd
                            ,
                             --a12.PROD_LVL4_CD,
                             a11.mdl_sffx_cd
                            ,a11.au_cd
                            ,a11.subsdr_cd
                            ,a11.sales_subsdr_rnr_cd
                            ,a11.production_subsdr_cd)

                SELECT ebiz.basis_yyyymm
                      ,ebiz.scenario_type_cd
                      ,ebiz.div_cd
                      ,'N'
                      ,mapp.cd_id AS kpi_cd
                      ,'BEP_ENHANCE_BIZ' cat_cd
                      ,nvl(substr(ebiz.prod_kor_name, 1, 30), '*') category_detail_code
                      ,

                       MIN(CASE scenario_type_cd
                               WHEN 'AC0' THEN
                                basis_yyyymm
                               WHEN 'PR1' THEN
                                to_char(add_months(to_date(basis_yyyymm, 'YYYYMM'), 1), 'YYYYMM')
                               WHEN 'PR2' THEN
                                to_char(add_months(to_date(basis_yyyymm, 'YYYYMM'), 2), 'YYYYMM')
                               WHEN 'PR3' THEN
                                to_char(add_months(to_date(basis_yyyymm, 'YYYYMM'), 3), 'YYYYMM')
                               WHEN 'PR4' THEN
                                to_char(add_months(to_date(basis_yyyymm, 'YYYYMM'), 4), 'YYYYMM')
                           END)
                      ,SUM(CASE
                               WHEN mapp.cd_id = 'SALE' THEN
                                ebiz.nsales_krw_amt
                               WHEN mapp.cd_id = 'COI' THEN
                                ebiz.oi_krw_amt
                               WHEN mapp.cd_id = 'MGN_PROFIT' THEN
                                ebiz.mgnl_prf_krw_amt
                               WHEN mapp.cd_id = 'SALES_DEDUCTION' THEN
                                ebiz.sales_deduct_krw_amt
                               ELSE
                                0
                           END) curr_mon_krw_amount
                      ,SUM(CASE
                               WHEN mapp.cd_id = 'SALE' THEN
                                ebiz.nsales_usd_amt
                               WHEN mapp.cd_id = 'COI' THEN
                                ebiz.oi_usd_amt
                               WHEN mapp.cd_id = 'MGN_PROFIT' THEN
                                ebiz.mgnl_prf_usd_amt
                               WHEN mapp.cd_id = 'SALES_DEDUCTION' THEN
                                ebiz.sales_deduct_usd_amt
                               ELSE
                                0
                           END) curr_mon_usd_amount
                      ,0 accu_krw_amount
                      ,0 accu_usd_amount
                      ,SYSDATE
                      ,'ares'
                      ,SYSDATE
                      ,'ares'
                FROM   (

                        SELECT /*+ parallel(32) */
                         a11.acctg_yyyymm   basis_yyyymm
                        ,a11.div_cd         div_cd
                        ,a112.scrn_dspl_seq div_kor_name
                        ,a112.div_shrt_name div_shrt_name
                        ,
                          /*
                          a11.SALES_SUBSDR_RNR_CD  SUBSDR_CD,
                          a117.MGT_ORG_SHRT_NAME  MGT_ORG_SHRT_NAME,
                          a117.SORT_ORDER  SORT_ORDER,

                          a11.SUBSDR_CD  SUBSDR_CD0,
                          a116.SUBSDR_SHRT_NAME  NEW_SUBSDR_SHRT_NAME,
                          a116.SORT_ORDER  SORT1_ORDER,
                          a11.PRODUCTION_SUBSDR_CD  SUBSDR_CD1,
                          a114.SUBSDR_SHRT_NAME  SUBSDR_NAME,
                          a114.SORT_ORDER  SORT_ORDER0,
                          */a11.scenario_type_cd    scenario_type_cd
                        ,a115.scenario_type_name scenario_type_name
                        ,a115.sort_order         sort_order1
                        ,
                          /*
                          a14.OLD_NEW_CD  OLD_NEW_CD,
                          a14.GRD_CD  GRD_CD,
                          a113.ATTRIBUTE_NAME  ATTRIBUTE_NAME,
                          a110.UP_PROD_CD  PROD_CD,
                          a119.PROD_ENG_NAME  PROD_ENG_NAME,
                          a119.SORT_ORDER  SORT_ORDER2,
                          a19.UP_PROD_CD  PROD_CD0,
                          a110.PROD_ENG_NAME  PROD_ENG_NAME0,
                          a110.SORT_ORDER  SORT_ORDER3,
                          a18.UP_PROD_CD  PROD_CD1,
                          a19.PROD_ENG_NAME  PROD_ENG_NAME1,
                          a19.SORT_ORDER  SORT_ORDER4,
                          a13.USR_PROD1_LAST_CD  PROD_CD2,
                          a18.PROD_ENG_NAME  PROD_ENG_NAME2,
                          a18.SORT_ORDER  SORT_ORDER5,
                          */a17.up_prod_cd     prod_cd3
                        ,a118.prod_eng_name prod_eng_name3
                        ,a118.sort_order    sort_order6
                        ,a16.up_prod_cd     prod_cd4
                        ,a17.prod_eng_name  prod_eng_name4
                        ,a17.sort_order     sort_order7
                        ,a15.up_prod_cd     prod_cd5
                        ,a16.prod_eng_name  prod_eng_name5
                        ,a16.sort_order     sort_order8
                        ,a12.prod_cd        prod_cd6
                        ,a15.prod_eng_name  prod_eng_name6
                        ,a15.prod_kor_name
                        ,a15.sort_order     sort_order9
                        ,
                          --sum(a11.SALES_QTY) SALES_QTY,
                          SUM(nsales_krw_amt) nsales_krw_amt
                        ,SUM(nsales_usd_amt) nsales_usd_amt
                        ,SUM(oi_krw_amt) oi_krw_amt
                        ,SUM(oi_usd_amt) oi_usd_amt
                        ,SUM(mgnl_prf_krw_amt) mgnl_prf_krw_amt
                        ,SUM(mgnl_prf_usd_amt) mgnl_prf_usd_amt
                        ,SUM(sales_deduct_krw_amt) sales_deduct_krw_amt
                        ,SUM(sales_deduct_usd_amt) sales_deduct_usd_amt
                        FROM   bep_temp a11
                        LEFT   OUTER JOIN npt_app.nv_dwd_biz_type_prod_m a12
                        ON     (a11.mdl_sffx_cd = a12.mdl_sffx_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_subsdr_mdl_period_h a13
                        ON     (a11.mdl_sffx_cd = a13.mdl_sffx_cd AND a11.subsdr_cd = a13.subsdr_cd)
                        LEFT   OUTER JOIN npt_dw_mgr.tb_dwd_subsdr_mdl_period_h a14
                        ON     (a11.acctg_yyyymm = a14.acctg_yyyymm AND a11.mdl_sffx_cd = a14.mdl_sffx_cd AND a11.subsdr_cd = a14.subsdr_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_biz_type_prod4_m a15
                        ON     (a12.prod_cd = a15.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_biz_type_prod3_m a16
                        ON     (a15.up_prod_cd = a16.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_biz_type_prod2_m a17
                        ON     (a16.up_prod_cd = a17.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_rpt_prod4_m a18
                        ON     (a13.usr_prod1_last_cd = a18.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_rpt_prod3_m a19
                        ON     (a18.up_prod_cd = a19.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_rpt_prod2_m a110
                        ON     (a19.up_prod_cd = a110.prod_cd)

                        LEFT   OUTER JOIN npt_app.nv_dwd_div_leaf_m a112
                        ON     (a11.div_cd = a112.div_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_02_grd_cd a113
                        ON     (a14.grd_cd = a113.attribute_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_subsdr_m a114
                        ON     (a11.production_subsdr_cd = a114.subsdr_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_scenario_type_m a115
                        ON     (a11.scenario_type_cd = a115.scenario_type_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_subsdr_m a116
                        ON     (a11.subsdr_cd = a116.subsdr_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_mgt_org_rnr_m a117
                        ON     (a11.sales_subsdr_rnr_cd = a117.mgt_org_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_biz_type_prod1_m a118
                        ON     (a17.up_prod_cd = a118.prod_cd)
                        LEFT   OUTER JOIN npt_app.nv_dwd_rpt_prod1_m a119
                        ON     (a110.up_prod_cd = a119.prod_cd)
                        WHERE  a11.scenario_type_cd IN ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
                        GROUP  BY a11.acctg_yyyymm
                                  ,a11.div_cd
                                  ,a112.scrn_dspl_seq
                                  ,a112.div_shrt_name
                                  ,
                                   /*
                                   a11.SALES_SUBSDR_RNR_CD,
                                   a117.MGT_ORG_SHRT_NAME,
                                   a117.SORT_ORDER,
                                   a11.SUBSDR_CD,
                                   a116.SUBSDR_SHRT_NAME,
                                   a116.SORT_ORDER,
                                   a11.PRODUCTION_SUBSDR_CD,
                                   a114.SUBSDR_SHRT_NAME,
                                   a114.SORT_ORDER,
                                   */a11.scenario_type_cd
                                  ,a115.scenario_type_name
                                  ,a115.sort_order
                                  ,
                                   /*
                                   a14.OLD_NEW_CD,
                                   a14.GRD_CD,
                                   a113.ATTRIBUTE_NAME,
                                   a110.UP_PROD_CD,
                                   a119.PROD_ENG_NAME,
                                   a119.SORT_ORDER,
                                   a19.UP_PROD_CD,
                                   a110.PROD_ENG_NAME,
                                   a110.SORT_ORDER,
                                   a18.UP_PROD_CD,
                                   a19.PROD_ENG_NAME,
                                   a19.SORT_ORDER,
                                   a13.USR_PROD1_LAST_CD,
                                   a18.PROD_ENG_NAME,
                                   a18.SORT_ORDER,
                                   */a17.up_prod_cd
                                  ,a118.prod_eng_name
                                  ,a118.sort_order
                                  ,a16.up_prod_cd
                                  ,a17.prod_eng_name
                                  ,a17.sort_order
                                  ,a15.up_prod_cd
                                  ,a16.prod_eng_name
                                  ,a16.sort_order
                                  ,a12.prod_cd
                                  ,a15.prod_eng_name
                                  ,a15.prod_kor_name
                                  ,a15.sort_order

                        ) ebiz
                LEFT   OUTER JOIN npt_rs_mgr.tb_rs_clss_cd_m mapp
                ON     mapp.cd_clsf_id = 'KPI_TYPE'
                AND    mapp.cd_id IN ('SALE', 'COI', 'MGN_PROFIT', 'SALES_DEDUCTION')

                GROUP  BY ebiz.basis_yyyymm
                         ,ebiz.scenario_type_cd
                         ,ebiz.div_cd
                         ,mapp.cd_id
                         ,ebiz.prod_kor_name

                ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2.1) Insert Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;


        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_row_cnt;
        dbms_output.put_line('2.1)Insert success row : ' || vn_insert_row_cnt);


        /*---------------------------------------------
         2.  상위 사업군 생성
        ----------------------------------------------*/
       BEGIN

        INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.div_cd
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,b.mapp_tg_cd AS sub_cat_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ARES'
                  ,SYSDATE
                  ,'ARES'
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h a
            INNER  JOIN npt_rs_mgr.tb_rs_clss_cd_r b
            ON     b.cd_mapp_clsf_cd = 'BEP_ENHANCE_BIZ_ROLL_UP'
            AND    b.mapp_src_cd = a.sub_cat_cd
            AND    b.use_flag = 'Y'
            WHERE  a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = 'BEP_ENHANCE_BIZ'
                  --AND SCENARIO_CODE in 'AC0'
            AND    a.manual_adj_flag = 'N'
            AND    a.kpi_cd IN ('SALE', 'COI', 'MGN_PROFIT', 'SALES_DEDUCTION')

            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,a.div_cd
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,b.mapp_tg_cd
                     ,a.apply_yyyymm;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2.2) Insert Table tb_rs_kpi_prod_h(upper) Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_row_cnt;
        dbms_output.put_line('2.2)Insert success row : ' || vn_insert_row_cnt);



        /*---------------------------------------------
         3.  육성 사업군 생성 (Division별)
        ----------------------------------------------*/

            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h a
            WHERE  a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    a.manual_adj_flag = 'N'
            AND    EXISTS (
                          select *
                          from npt_rs_mgr.tb_rs_clss_cd_m m
                          where m.cd_clsf_id like '%_BIZ'
                          and  m.attribute3_value = 'DIVISION'
                          and  m.cd_id = a.sub_cat_cd )
            ;

          Begin

            INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

                SELECT A.BASE_YYYYMM,
                       A.SCENARIO_TYPE_CD,
                       A.DIV_CD,
                       a.manual_adj_flag,
                       SUBSTR(A.KPI_CD, 4)  AS KPI_TYPE_CODE,
                       'BEP_ENHANCE_BIZ',
                       m.Cd_Id AS CATEGORY_DETAIL_CODE,
                       a.apply_yyyymm,
                       SUM(A.CURRM_KRW_AMT),
                       SUM(A.CURRM_USD_AMT),
                       SUM(A.ACCU_KRW_AMT),
                       SUM(A.ACCU_USD_AMT),
                       sysdate ,
                       'ARES',
                       sysdate ,
                       'ARES'
                FROM   npt_rs_mgr.TB_RS_KPI_DIV_H  A
                INNER JOIN npt_rs_mgr.tb_rs_clss_cd_m   m
                ON     m.cd_clsf_id like '%_BIZ'
                and  m.attribute3_value = 'DIVISION'
                and  m.attribute4_value = a.div_cd
                WHERE  A.BASE_YYYYMM  = iv_yyyymm
                AND    A.SCENARIO_TYPE_CD in ('AC0','PR1','PR2','PR3','PR4')
                AND    A.CAT_CD = 'OH'
                AND    A.KPI_CD in ('OH_SALE','OH_MGN_PROFIT','OH_COI','OH_SALES_DEDUCTION')
                GROUP BY A.BASE_YYYYMM,
                         A.SCENARIO_TYPE_CD,
                         A.DIV_CD,
                         a.manual_adj_flag,
                         A.KPI_CD,
                         m.Cd_Id,
                         a.apply_yyyymm  ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2.3) Insert Table tb_rs_kpi_prod_h(Division) Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_row_cnt;
        dbms_output.put_line('2.3)Insert success row : ' || vn_insert_row_cnt);


        /*---------------------------------------------
         4.  GNT:Commercial TV = TV 사이니지 + Hotel LTV로 분리
        ----------------------------------------------*/

/* -- GNT:Commercial TV = TV 사이니지 + Hotel LTV로 분리  */
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h a
            WHERE  a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    a.manual_adj_flag = 'N'
            and    a.sub_cat_cd in ('TV 사이니지', 'Hotel TV')
            and    a.base_yyyymm >= '201401'
            ;
/* -- GNT:Commercial TV = TV 사이니지 + Hotel LTV로 분리  */
        BEGIN
        INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)

        WITH TEMPA(ACCTG_YYYYMM, SCENARIO_TYPE_CD, DIV_CD, AU_CD, SUBSDR_RNR_CD, PROD_LVL4_CD, MDL_SFFX_CD,
                   v_net_sales_krw_amt, v_net_sales_usd_amt, v_op_inc_krw_amt, v_op_inc_usd_amt,
                   v_sales_deduct_krw_amt, v_sales_deduct_usd_amt, v_mgn_profit_krw_amt, v_mgn_profit_usd_amt,
                   v_net_sales_accum_krw_amt, v_net_sales_accum_usd_amt, v_op_inc_accum_krw_amt, v_op_inc_accum_usd_amt,
                   v_sales_deduct_accum_krw_amt, v_sales_deduct_accum_usd_amt, v_mgn_profit_accum_krw_amt, v_mgn_profit_accum_usd_amt) AS
(        -- Actual
         SELECT /*+ parallel(a 32) parallel(mh 32) use_hash(mh) */ A.acctg_yyyymm
               ,A.scenario_type_cd
               ,A.div_cd, A.AU_CD, A.SALES_SUBSDR_RNR_CD  SUBSDR_RNR_CD
               ,C.prod_lvl4_cd
               ,A.mdl_sffx_cd
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_usd_amt

               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_accum_usd_amt
         FROM  NPT_APP.NV_DWW_CON_BEP_SUMM_DW_S  A   -- BEP
               left outer join  NPT_APP.NV_DWD_PRFT_CONFM_SCENARIO_H  B

               on A.ACCTG_YYYYMM = B.ACCTG_YYYYMM and
               A.DIV_CD = B.DIV_CD and
               A.SCENARIO_TYPE_CD = B.SCENARIO_TYPE_CD
              left outer join  NPT_APP.NV_DWD_SUBSDR_MDL_PERIOD_H  C
                on A.MDL_SFFX_CD = C.MDL_SFFX_CD and
                   A.SUBSDR_CD = C.SUBSDR_CD
         WHERE A.SCENARIO_TYPE_CD in ('AC0')
         AND   A.ACCTG_YYYYMM = iv_yyyymm -- BETWEEN iv_yyyymm AND iv_yyyymm_TO
         AND   A.DIV_CD in ('GNT')
         --AND   A.CONSLD_SALES_MDL_FLAG in ('Y')
         AND   A.CURRM_ACCUM_TYPE_CD in ('CURRM','ACCUM')
         AND   A.VRNC_ALC_INCL_EXCL_CD in ('INCL')
         AND   A.CURRENCY_CD in ('KRW', 'USD')
         AND   B.CONFIRM_FLAG = 'Y'
         GROUP BY A.acctg_yyyymm
               ,A.scenario_type_cd
               ,A.div_cd, A.AU_CD, A.SALES_SUBSDR_RNR_CD
               ,C.prod_lvl4_cd
               ,A.mdl_sffx_cd
        ),
        TEMPP(ACCTG_YYYYMM, SCENARIO_TYPE_CD, DIV_CD, AU_CD, SUBSDR_RNR_CD, PROD_LVL4_CD, MDL_SFFX_CD,
                   v_net_sales_krw_amt, v_net_sales_usd_amt, v_op_inc_krw_amt, v_op_inc_usd_amt,
                   v_sales_deduct_krw_amt, v_sales_deduct_usd_amt, v_mgn_profit_krw_amt, v_mgn_profit_usd_amt,
                   v_net_sales_accum_krw_amt, v_net_sales_accum_usd_amt, v_op_inc_accum_krw_amt, v_op_inc_accum_usd_amt,
                   v_sales_deduct_accum_krw_amt, v_sales_deduct_accum_usd_amt, v_mgn_profit_accum_krw_amt, v_mgn_profit_accum_usd_amt) AS
(        -- 이동계획
         SELECT /*+ parallel(a 32) parallel(mh 32) use_hash(mh) */ TO_CHAR(ADD_MONTHS(TO_DATE(A.acctg_yyyymm||'01','YYYYMMDD'),+1),'YYYYMM') acctg_yyyymm
               ,A.scenario_type_cd
               ,A.div_cd, A.AU_CD, A.SALES_SUBSDR_RNR_CD  SUBSDR_RNR_CD
               ,C.prod_lvl4_cd
               ,A.mdl_sffx_cd
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'KRW' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'CURRM' AND A.CURRENCY_CD = 'USD' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_usd_amt

               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.NSALES_AMT       ELSE 0 END)       as v_net_sales_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.OI_AMT           ELSE 0 END)       as v_op_inc_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.SALES_DEDUCT_AMT ELSE 0 END)       as v_sales_deduct_accum_usd_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'KRW' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_accum_krw_amt
               ,SUM(CASE WHEN A.CURRM_ACCUM_TYPE_CD = 'ACCUM' AND A.CURRENCY_CD = 'USD' THEN A.MGNL_PRF_AMT     ELSE 0 END)       as v_mgn_profit_accum_usd_amt
         FROM  NPT_APP.NV_DWW_CON_BEP_SUMM_DW_S  A
               left outer join  NPT_APP.NV_DWD_PRFT_CONFM_SCENARIO_H  B
               on A.ACCTG_YYYYMM = B.ACCTG_YYYYMM and
               A.DIV_CD = B.DIV_CD and
               A.SCENARIO_TYPE_CD = B.SCENARIO_TYPE_CD
              left outer join  NPT_APP.NV_DWD_SUBSDR_MDL_PERIOD_H  C
                on A.MDL_SFFX_CD = C.MDL_SFFX_CD and
                   A.SUBSDR_CD = C.SUBSDR_CD
         WHERE A.SCENARIO_TYPE_CD in ('PR1','PR2','PR3','PR4')
         AND   A.ACCTG_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(iv_yyyymm||'01','YYYYMMDD'),-1),'YYYYMM')
         AND   A.DIV_CD in ('GNT')
         --AND   A.CONSLD_SALES_MDL_FLAG in ('Y')
         AND   A.CURRM_ACCUM_TYPE_CD in ('CURRM','ACCUM')
         AND   A.VRNC_ALC_INCL_EXCL_CD in ('INCL')
         AND   A.CURRENCY_CD in ('KRW', 'USD')
         AND   B.CONFIRM_FLAG = 'Y'
         and   iv_yyyymm >= '201401'
         GROUP BY TO_CHAR(ADD_MONTHS(TO_DATE(A.acctg_yyyymm||'01','YYYYMMDD'),+1),'YYYYMM')
               ,A.scenario_type_cd
               ,A.div_cd, A.AU_CD, A.SALES_SUBSDR_RNR_CD
               ,C.prod_lvl4_cd
               ,A.mdl_sffx_cd
        ) ,
        -- 사업군 분류에서 Commercial TV(GNT)만 가져옴. 그리고, 분리 비율 PL4 추가
        TEMPC (DIV_CD, BIZ_TYPE, BIZ_NAME, RULE_TYPE_CD, PROD_LVL3_CD, PROD_LVL4_CD, MDL_CD, SALES_MDL_SFFX_CD, MDL_SFFX_CD, ENABLE_FLAG, APPLY_FLAG) AS
        (
          SELECT A.DIV_CD, A.BIZ_TYPE, B.prod_kor_name AS BIZ_NAME, A.RULE_TYPE_CD, A.PROD_LVL3_CD, A.PROD_LVL4_CD, A.MDL_CD,
                 A.SALES_MDL_SFFX_CD, A.MDL_SFFX_CD, A.ENABLE_FLAG, A.APPLY_FLAG
          FROM   (
                  SELECT DIV_CD, ATTRIBUTE_VALUE AS BIZ_TYPE, RULE_TYPE_CD, PROD_LVL3_CD, PROD_LVL4_CD, MDL_CD, SALES_MDL_SFFX_CD, MDL_SFFX_CD, ENABLE_FLAG, APPLY_FLAG
                  FROM   NV_CM_FORML_RULE_MDL_NEW_H
                  WHERE  ATTRIBUTE_NAME = 'BIZ_TYPE_LEVEL'
                  AND    DIV_CD = 'GNT'
                  -- Commercail TV 분리 PL4
                  UNION ALL
                  SELECT 'GNT' DIV_CD, 'BIZ_B3_L4_GNT_2' AS BIZ_TYPE, 'PROD_LVL4' RULE_TYPE_CD, 'CSXXXX' PROD_LVL3_CD, 'CSXXXXXX' PROD_LVL4_CD, '*' MDL_CD,
                         '*' SALES_MDL_SFFX_CD, '*' MDL_SFFX_CD, 'Y' ENABLE_FLAG, 'Y' APPLY_FLAG
                  FROM DUAL
                  UNION ALL
                  SELECT 'GNT' DIV_CD, 'BIZ_B3_L4_GNT_3' AS BIZ_TYPE, 'PROD_LVL4' RULE_TYPE_CD, 'HTXXXX' PROD_LVL3_CD, 'HTXXXXXX' PROD_LVL4_CD, '*' MDL_CD,
                         '*' SALES_MDL_SFFX_CD, '*' MDL_SFFX_CD, 'Y' ENABLE_FLAG, 'Y' APPLY_FLAG
                  FROM DUAL
                 ) A
                left outer join  NPT_APP.NV_DWD_BIZ_TYPE_PROD4_M  B
                on    A.BIZ_TYPE = B.PROD_CD
        )


        SELECT basis_yyyymm,
               scenario_type_code,
               gbu_code,
               --SUBSDR_RNR_CD,
               'N',
               kpi_type_code,
               iv_category,
               category_detail_code,
               MIN(
               CASE scenario_type_code
                    WHEN 'AC0' THEN basis_yyyymm
                    WHEN 'PR1' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), +1), 'YYYYMM')
                    WHEN 'PR2' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), +2), 'YYYYMM')
                    WHEN 'PR3' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), +3), 'YYYYMM')
                    WHEN 'PR4' THEN to_char(add_months(to_date(basis_yyyymm,'YYYYMM'), +4), 'YYYYMM')
               END),
               SUM(curr_mon_krw_amount),
               SUM(curr_mon_usd_amount),
               SUM(accu_krw_amount),
               SUM(accu_usd_amount),
               SYSDATE,
               'ares',
               SYSDATE,
               'ares'
        FROM (
                SELECT M.acctg_yyyymm as BASIS_YYYYMM,
                       M.scenario_type_cd as SCENARIO_TYPE_CODE,
                       M.div_cd as GBU_CODE,
                       M.SUBSDR_RNR_CD,
                       C.cd_id as KPI_TYPE_CODE,
                       M.devlm_biz_kor_name as CATEGORY_DETAIL_CODE,
                       sum(CASE C.CD_ID
                           WHEN 'SALE' THEN
                                 v_net_sales_krw_amt
                           WHEN 'MGN_PROFIT' THEN
                                 v_mgn_profit_krw_amt
                           WHEN 'COI' THEN
                                 v_op_inc_krw_amt
                           WHEN 'SALES_DEDUCTION' THEN
                                 v_sales_deduct_krw_amt
                           ELSE 0 END) as CURR_MON_KRW_AMOUNT,
                       sum(CASE C.CD_ID
                           WHEN 'SALE' THEN
                                 v_net_sales_usd_amt
                           WHEN 'MGN_PROFIT' THEN
                                 v_mgn_profit_usd_amt
                           WHEN 'COI' THEN
                                 v_op_inc_usd_amt
                           WHEN 'SALES_DEDUCTION' THEN
                                 v_sales_deduct_usd_amt
                           ELSE 0 END) as CURR_MON_USD_AMOUNT,
                       sum(CASE C.CD_ID
                           WHEN 'SALE' THEN
                                 v_net_sales_accu_krw_amt
                           WHEN 'MGN_PROFIT' THEN
                                 v_mgn_profit_accu_krw_amt
                           WHEN 'COI' THEN
                                 v_op_inc_accu_krw_amt
                           WHEN 'SALES_DEDUCTION' THEN
                                 v_sales_deduct_accu_krw_amt
                           ELSE 0 END) as ACCU_KRW_AMOUNT,
                       sum(CASE C.CD_ID
                           WHEN 'SALE' THEN
                                 v_net_sales_accu_usd_amt
                           WHEN 'MGN_PROFIT' THEN
                                 v_mgn_profit_accu_usd_amt
                           WHEN 'COI' THEN
                                 v_op_inc_accu_usd_amt
                           WHEN 'SALES_DEDUCTION' THEN
                                 v_sales_deduct_accu_usd_amt
                           ELSE 0 END) as ACCU_USD_AMOUNT
                FROM(
                       --SELECT A.acctg_yyyymm,
                       SELECT /*+ no_merge(a) no_expand */ A.acctg_yyyymm,
                              A.scenario_type_cd,
                              A.div_cd,
                              A.SUBSDR_RNR_CD,
                              B.BIZ_NAME devlm_biz_kor_name,
                              SUM(v_net_sales_krw_amt ) as v_net_sales_krw_amt,
                              SUM(v_net_sales_usd_amt ) as v_net_sales_usd_amt,
                              SUM(v_mgn_profit_krw_amt ) as v_mgn_profit_krw_amt,
                              SUM(v_mgn_profit_usd_amt ) as v_mgn_profit_usd_amt,
                              SUM(v_op_inc_krw_amt ) as v_op_inc_krw_amt,
                              SUM(v_op_inc_usd_amt ) as v_op_inc_usd_amt,
                              SUM(v_sales_deduct_krw_amt ) as v_sales_deduct_krw_amt,
                              SUM(v_sales_deduct_usd_amt ) as v_sales_deduct_usd_amt,
                              SUM(v_net_sales_accu_krw_amt ) as v_net_sales_accu_krw_amt,
                              SUM(v_net_sales_accu_usd_amt ) as v_net_sales_accu_usd_amt,
                              SUM(v_mgn_profit_accu_krw_amt ) as v_mgn_profit_accu_krw_amt,
                              SUM(v_mgn_profit_accu_usd_amt ) as v_mgn_profit_accu_usd_amt,
                              SUM(v_op_inc_accu_krw_amt ) as v_op_inc_accu_krw_amt,
                              SUM(v_op_inc_accu_usd_amt ) as v_op_inc_accu_usd_amt,
                              SUM(v_sales_deduct_accu_krw_amt ) as v_sales_deduct_accu_krw_amt,
                              SUM(v_sales_deduct_accu_usd_amt ) as v_sales_deduct_accu_usd_amt
                        FROM (/*----- 실적 집계 -----*/
                                SELECT /*+ parallel(a 32) parallel(mh 32) use_hash(mh) */ A.acctg_yyyymm
                                       ,A.scenario_type_cd
                                       ,A.div_cd
                                       ,A.SUBSDR_RNR_CD
                                       ,NVL(E.PROD_LVl4_CD, A.prod_lvl4_cd) AS PROD_LVL4_CD
                                       ,A.mdl_sffx_cd
                                       ,SUM(v_net_sales_krw_amt    * NVL(E.splt_rate, 1)) as v_net_sales_krw_amt
                                       ,SUM(v_net_sales_usd_amt    * NVL(E.splt_rate, 1)) as v_net_sales_usd_amt
                                       ,SUM(v_op_inc_krw_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_krw_amt
                                       ,SUM(v_op_inc_usd_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_usd_amt
                                       ,SUM(v_sales_deduct_krw_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_krw_amt
                                       ,SUM(v_sales_deduct_usd_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_usd_amt
                                       ,SUM(v_mgn_profit_krw_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_krw_amt
                                       ,SUM(v_mgn_profit_usd_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_usd_amt

                                       ,SUM(v_net_sales_accum_krw_amt    * NVL(E.splt_rate, 1)) as v_net_sales_accu_krw_amt
                                       ,SUM(v_net_sales_accum_usd_amt    * NVL(E.splt_rate, 1)) as v_net_sales_accu_usd_amt
                                       ,SUM(v_op_inc_accum_krw_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_accu_krw_amt
                                       ,SUM(v_op_inc_accum_usd_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_accu_usd_amt
                                       ,SUM(v_sales_deduct_accum_krw_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_accu_krw_amt
                                       ,SUM(v_sales_deduct_accum_usd_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_accu_usd_amt
                                       ,SUM(v_mgn_profit_accum_krw_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_accu_krw_amt
                                       ,SUM(v_mgn_profit_accum_usd_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_accu_usd_amt
                                 FROM  TEMPA  A
                                       --  2015.06.18 Commercial TV(TV 사이니지, Hotel LTV) 분리
                                       LEFT OUTER JOIN (SELECT distinct base_yyyymm
                                                              ,div_cd
                                                              ,CASE WHEN AU_CD = 'EYK' THEN SUBSDR_CD || '_IYK'
                                                                    WHEN AU_CD = 'ESB' THEN SUBSDR_CD || '_ISB'
                                                                    ELSE SUBSDR_CD
                                                               END as subsdr_cd
                                                              ,au_cd
                                                        FROM   (
                                                                SELECT div_cd,
                                                                       base_yyyymmdd base_yyyymm ,
                                                                       attribute1_value SUBSDR_CD,
                                                                       attribute2_value AU_CD,
                                                                       attribute3_value MAPP_DIV_CD,
                                                                       attribute4_value PROD_LVL4_CD,
                                                                       TO_NUMBER(attribute5_value) SPLT_RATE,
                                                                       TO_NUMBER(attribute6_value) SPLT_QTY,
                                                                       attribute7_value USE_FLAG
                                                                FROM   npt_rs_mgr.tb_rs_excel_upld_data_d
                                                                WHERE  prcs_seq = 1200
                                                                AND    rs_module_cd = 'ARES'
                                                                AND    base_yyyymmdd = iv_yyyymm
                                                                AND    attribute7_value = 'Y'
                                                                ) h
                                                        WHERE  splt_rate <> 0
                                                        AND    (mapp_div_cd <> 'GNTTS' OR (mapp_div_cd = 'GNTTS' AND splt_rate <> 1))
                                       ) D
                                       ON  D.base_yyyymm = A.acctg_yyyymm
                                       AND D.div_cd = A.div_cd

                                       AND (CASE WHEN A.SUBSDR_RNR_CD = 'EMAF_IEF' THEN 'EMEF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMGF_IGF' THEN 'EMGF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMLF_ILF' THEN 'EMLF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_IIR' THEN 'EMIR'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_ITU' THEN 'EMTU'
                                                 ELSE A.SUBSDR_RNR_CD END ) = D.SUBSDR_CD




                                       AND D.au_cd = CASE WHEN D.au_cd = '*' THEN '*' ELSE a.au_cd END
                                       LEFT OUTER JOIN (
                                                        SELECT div_cd,
                                                               base_yyyymmdd base_yyyymm ,
                                                              -- attribute1_value SUBSDR_CD,

                                                               CASE WHEN attribute2_value = 'EYK' THEN attribute1_value || '_IYK'
                                                                    WHEN attribute2_value = 'ESB' THEN attribute1_value || '_ISB'
                                                                    ELSE attribute1_value
                                                               END as subsdr_cd,

                                                               attribute2_value AU_CD,
                                                               attribute3_value MAPP_DIV_CD,
                                                               attribute4_value PROD_LVL4_CD,
                                                               TO_NUMBER(attribute5_value) SPLT_RATE,
                                                               TO_NUMBER(attribute6_value) SPLT_QTY,
                                                               attribute7_value USE_FLAG
                                                        FROM   npt_rs_mgr.tb_rs_excel_upld_data_d
                                                        WHERE  prcs_seq = 1200
                                                        AND    rs_module_cd = 'ARES'
                                                        AND    base_yyyymmdd = iv_yyyymm
                                                        AND    attribute7_value = 'Y'
                                                       ) E
                                       on   E.base_yyyymm = A.acctg_yyyymm
                                       and  E.div_cd = A.div_cd
                                       /*
                                       AND (CASE WHEN E.AU_CD = 'EYK' THEN E.SUBSDR_CD || '_IYK'
                                                 WHEN E.AU_CD = 'ESB' THEN E.SUBSDR_CD || '_ISB'
                                                 ELSE E.SUBSDR_CD END) = A.SUBSDR_RNR_CD
                                                   */

                                       AND (CASE WHEN A.SUBSDR_RNR_CD = 'EMAF_IEF' THEN 'EMEF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMGF_IGF' THEN 'EMGF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMLF_ILF' THEN 'EMLF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_IIR' THEN 'EMIR'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_ITU' THEN 'EMTU'
                                                 ELSE A.SUBSDR_RNR_CD END ) = E.SUBSDR_CD

                                       AND  E.au_cd = NVL(D.au_cd, '*')
                                       AND  E.splt_rate <> 0
                                       AND  (E.mapp_div_cd <> 'GNTTS' OR (E.mapp_div_cd = 'GNTTS' AND E.splt_rate <> 1))
                                       AND  A.prod_lvl4_cd LIKE 'CS%'
                                       --  2015.06.18 Commercial TV(TV 사이니지, Hotel LTV) 분리
                                 WHERE 1=1
                                 GROUP BY A.acctg_yyyymm
                                       ,A.scenario_type_cd
                                       ,A.div_cd
                                       ,A.SUBSDR_RNR_CD
                                       ,NVL(E.PROD_LVl4_CD, A.prod_lvl4_cd)
                                       ,A.mdl_sffx_cd
                                UNION ALL
                                SELECT /*+ parallel(a 32) parallel(mh 32) use_hash(mh) */ A.acctg_yyyymm
                                       ,A.scenario_type_cd
                                       ,A.div_cd
                                       ,A.SUBSDR_RNR_CD
                                       ,NVL(E.PROD_LVl4_CD, A.prod_lvl4_cd) AS PROD_LVL4_CD
                                       ,A.mdl_sffx_cd
                                       ,SUM(v_net_sales_krw_amt    * NVL(E.splt_rate, 1)) as v_net_sales_krw_amt
                                       ,SUM(v_net_sales_usd_amt    * NVL(E.splt_rate, 1)) as v_net_sales_usd_amt
                                       ,SUM(v_op_inc_krw_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_krw_amt
                                       ,SUM(v_op_inc_usd_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_usd_amt
                                       ,SUM(v_sales_deduct_krw_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_krw_amt
                                       ,SUM(v_sales_deduct_usd_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_usd_amt
                                       ,SUM(v_mgn_profit_krw_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_krw_amt
                                       ,SUM(v_mgn_profit_usd_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_usd_amt

                                       ,SUM(v_net_sales_accum_krw_amt    * NVL(E.splt_rate, 1)) as v_net_sales_accu_krw_amt
                                       ,SUM(v_net_sales_accum_usd_amt    * NVL(E.splt_rate, 1)) as v_net_sales_accu_usd_amt
                                       ,SUM(v_op_inc_accum_krw_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_accu_krw_amt
                                       ,SUM(v_op_inc_accum_usd_amt       * NVL(E.splt_rate, 1)) AS v_op_inc_accu_usd_amt
                                       ,SUM(v_sales_deduct_accum_krw_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_accu_krw_amt
                                       ,SUM(v_sales_deduct_accum_usd_amt * NVL(E.splt_rate, 1)) as v_sales_deduct_accu_usd_amt
                                       ,SUM(v_mgn_profit_accum_krw_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_accu_krw_amt
                                       ,SUM(v_mgn_profit_accum_usd_amt   * NVL(E.splt_rate, 1)) AS v_mgn_profit_accu_usd_amt
                                 FROM  TEMPP  A
                                       --  2015.06.18 Commercial TV(TV 사이니지, Hotel LTV) 분리
                                       LEFT OUTER JOIN (SELECT distinct base_yyyymm
                                                              ,div_cd
                                                              --,subsdr_cd
                                                              ,CASE WHEN au_cd = 'EYK' THEN subsdr_cd || '_IYK'
                                                                    WHEN au_cd = 'ESB' THEN subsdr_cd || '_ISB'
                                                                    ELSE subsdr_cd
                                                               END as subsdr_cd
                                                              ,au_cd
                                                        FROM   (
                                                                SELECT div_cd,
                                                                       base_yyyymmdd base_yyyymm ,
                                                                       attribute1_value SUBSDR_CD,
                                                                       attribute2_value AU_CD,
                                                                       attribute3_value MAPP_DIV_CD,
                                                                       attribute4_value PROD_LVL4_CD,
                                                                       TO_NUMBER(attribute5_value) SPLT_RATE,
                                                                       TO_NUMBER(attribute6_value) SPLT_QTY,
                                                                       attribute7_value USE_FLAG
                                                                FROM   npt_rs_mgr.tb_rs_excel_upld_data_d
                                                                WHERE  prcs_seq = 1200
                                                                AND    rs_module_cd = 'ARES'
                                                                AND    base_yyyymmdd = TO_CHAR(ADD_MONTHS(TO_DATE(iv_yyyymm||'01','YYYYMMDD'),-1),'YYYYMM')
                                                                AND    attribute7_value = 'Y'
                                                                ) h
                                                        WHERE  splt_rate <> 0
                                                        AND    (mapp_div_cd <> 'GNTTS' OR (mapp_div_cd = 'GNTTS' AND splt_rate <> 1))
                                       ) D
                                       ON  D.base_yyyymm = A.acctg_yyyymm
                                       AND D.div_cd = A.div_cd
                                       /*
                                       AND (CASE WHEN D.AU_CD = 'EYK' THEN D.SUBSDR_CD || '_IYK'
                                                 WHEN D.AU_CD = 'ESB' THEN D.SUBSDR_CD || '_ISB'
                                                 ELSE D.SUBSDR_CD END) = A.SUBSDR_RNR_CD
                                                   */
                                       AND (CASE WHEN A.SUBSDR_RNR_CD = 'EMAF_IEF' THEN 'EMEF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMGF_IGF' THEN 'EMGF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMLF_ILF' THEN 'EMLF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_IIR' THEN 'EMIR'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_ITU' THEN 'EMTU'
                                                 ELSE A.SUBSDR_RNR_CD END ) = D.SUBSDR_CD
                                       AND D.au_cd = CASE WHEN D.au_cd = '*' THEN '*' ELSE a.au_cd END
                                       LEFT OUTER JOIN (
                                                        SELECT div_cd,
                                                               base_yyyymmdd base_yyyymm ,
                                                               --attribute1_value SUBSDR_CD,
                                                               CASE WHEN attribute2_value = 'EYK' THEN attribute1_value || '_IYK'
                                                                    WHEN attribute2_value = 'ESB' THEN attribute1_value || '_ISB'
                                                                    ELSE attribute1_value
                                                               END as subsdr_cd,
                                                               attribute2_value AU_CD,
                                                               attribute3_value MAPP_DIV_CD,
                                                               attribute4_value PROD_LVL4_CD,
                                                               TO_NUMBER(attribute5_value) SPLT_RATE,
                                                               TO_NUMBER(attribute6_value) SPLT_QTY,
                                                               attribute7_value USE_FLAG
                                                        FROM   npt_rs_mgr.tb_rs_excel_upld_data_d
                                                        WHERE  prcs_seq = 1200
                                                        AND    rs_module_cd = 'ARES'
                                                        AND    base_yyyymmdd = TO_CHAR(ADD_MONTHS(TO_DATE(iv_yyyymm||'01','YYYYMMDD'),-1),'YYYYMM') --= iv_yyyymm
                                                        AND    attribute7_value = 'Y'
                                                       ) E
                                       on   E.base_yyyymm = A.acctg_yyyymm
                                       and  E.div_cd = A.div_cd
                                       /*
                                       AND (CASE WHEN E.AU_CD = 'EYK' THEN E.SUBSDR_CD || '_IYK'
                                                 WHEN E.AU_CD = 'ESB' THEN E.SUBSDR_CD || '_ISB'
                                                 ELSE E.SUBSDR_CD END) = A.SUBSDR_RNR_CD
                                                   */

                                       AND (CASE WHEN A.SUBSDR_RNR_CD = 'EMAF_IEF' THEN 'EMEF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMGF_IGF' THEN 'EMGF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMLF_ILF' THEN 'EMLF'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_IIR' THEN 'EMIR'
                                                 WHEN A.SUBSDR_RNR_CD = 'EMDF_ITU' THEN 'EMTU'
                                                 ELSE A.SUBSDR_RNR_CD END ) = E.SUBSDR_CD


                                       AND  E.au_cd = NVL(D.au_cd, '*')
                                       AND  E.splt_rate <> 0
                                       AND  (E.mapp_div_cd <> 'GNTTS' OR (E.mapp_div_cd = 'GNTTS' AND E.splt_rate <> 1))
                                       AND  A.prod_lvl4_cd LIKE 'CS%'
                                       --  2015.06.18 Commercial TV(TV 사이니지, Hotel LTV) 분리
                                 WHERE 1=1
                                 GROUP BY A.acctg_yyyymm
                                       ,A.scenario_type_cd
                                       ,A.div_cd
                                       ,A.SUBSDR_RNR_CD
                                       ,NVL(E.PROD_LVl4_CD, A.prod_lvl4_cd)
                                       ,A.mdl_sffx_cd

                             ) A
                        INNER JOIN TEMPC  B -- npt_rs_mgr.TB_RS_BIZ_M
                        on   B.RULE_TYPE_CD = 'PROD_LVL4'
                        and  B.div_cd = A.div_cd
                        and  B.prod_lvl4_cd = A.prod_lvl4_cd
                        -- DIV / MDL / MDL_SFFX_ETC / MDL_SFFX_SALE / PROD_LVL3 / PROD_LVL4
                        and   B.div_cd in (select DIV_CD
                                           from   npt_rs_mgr.TB_RS_DIV_H
                                           where  BASE_YYYYMM = iv_div_yyyymm
                                           and    ORG_LEAF_FLAG = 'Y')  -- 2014.10.23 Leaf 사업부만
                        and  B.ENABLE_FLAG = 'Y'
                        WHERE A.scenario_type_cd in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
                        GROUP BY A.acctg_yyyymm,
                                 A.scenario_type_cd,
                                 A.div_cd,
                                 A.SUBSDR_RNR_CD,
                                 B.BIZ_NAME
                    ) M
                LEFT OUTER JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C
                on  C.CD_CLSF_ID = 'KPI_TYPE'
                and C.CD_ID in ('SALE','MGN_PROFIT','COI','SALES_DEDUCTION')
                GROUP BY M.acctg_yyyymm,
                         M.scenario_type_cd,
                         M.div_cd,
                         M.SUBSDR_RNR_CD,
                         C.cd_id,
                         M.devlm_biz_kor_name
             )
        GROUP BY basis_yyyymm,
                 scenario_type_code,
                 gbu_code,
                 --SUBSDR_RNR_CD,
                 kpi_type_code,
                 category_detail_code;
------------- GNT(TS, HT 분리 끝.------------------
        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('3) Insert Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
           5.본부, GBU Rollup
        ---------------------------------------------*/
        BEGIN
        INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
            (base_yyyymm
            ,scenario_type_cd
            ,div_cd
            ,manual_adj_flag
            ,kpi_cd
            ,cat_cd
            ,sub_cat_cd
            ,apply_yyyymm
            ,currm_krw_amt
            ,currm_usd_amt
            ,accu_krw_amt
            ,accu_usd_amt
            ,creation_date
            ,creation_usr_id
            ,last_upd_date
            ,last_upd_usr_id)
        SELECT  A.BASE_YYYYMM
               ,A.SCENARIO_TYPE_CD
               ,B.CMPNY_CD
               ,A.MANUAL_ADJ_FLAG
               ,A.KPI_CD
               ,A.CAT_CD
               ,A.SUB_CAT_CD
               ,MIN(A.APPLY_YYYYMM)
               ,SUM(A.CURRM_KRW_AMT)
               ,SUM(A.CURRM_USD_AMT)
               ,SUM(A.ACCU_KRW_AMT)
               ,SUM(A.ACCU_USD_AMT)
               ,SYSDATE
               ,'ares'
               ,SYSDATE
               ,'ares'
        FROM  npt_rs_mgr.TB_RS_KPI_PROD_H A
        INNER JOIN npt_rs_mgr.TB_RS_DIV_H B
        ON    B.DIV_CD = A.DIV_CD
        AND   B.BASE_YYYYMM = iv_div_yyyymm
        WHERE A.BASE_YYYYMM = iv_yyyymm
        AND   A.CAT_CD = iv_category
        AND   A.MANUAL_ADJ_FLAG = 'N'
        AND   A.DIV_CD NOT IN ( 'CMM' )
        GROUP BY A.BASE_YYYYMM
                ,A.SCENARIO_TYPE_CD
                ,B.CMPNY_CD
                ,A.MANUAL_ADJ_FLAG
                ,A.KPI_CD
                ,A.CAT_CD
                ,A.SUB_CAT_CD
        UNION ALL
        SELECT  A.BASE_YYYYMM
               ,A.SCENARIO_TYPE_CD
               ,'GBU'
               ,A.MANUAL_ADJ_FLAG
               ,A.KPI_CD
               ,A.CAT_CD
               ,A.SUB_CAT_CD
               ,MIN(A.APPLY_YYYYMM)
               ,SUM(A.CURRM_KRW_AMT)
               ,SUM(A.CURRM_USD_AMT)
               ,SUM(A.ACCU_KRW_AMT)
               ,SUM(A.ACCU_USD_AMT)
               ,SYSDATE
               ,'ares'
               ,SYSDATE
               ,'ares'
        FROM  npt_rs_mgr.TB_RS_KPI_PROD_H A
        WHERE A.BASE_YYYYMM = iv_yyyymm
        AND   A.CAT_CD = iv_category
        AND   A.MANUAL_ADJ_FLAG = 'N'
        GROUP BY A.BASE_YYYYMM
                ,A.SCENARIO_TYPE_CD
                ,A.MANUAL_ADJ_FLAG
                ,A.KPI_CD
                ,A.CAT_CD
                ,A.SUB_CAT_CD;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('4) Insert Hierarchy Table tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_div_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);
        -- Job Log

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_BEP_ENHANCE_BIZ;


    PROCEDURE SP_RS_KPI_CPS(iv_yyyymm     IN VARCHAR2
                           ,iv_category   IN VARCHAR2
                           ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_cps                                                                   */
        /* 4.기       능 :                                                                                 */
        /*                 BEP 매출액, 한계이익, 영업이익, 재료비를 집계하여                               */
        /*                 tb_rs_kpi_div_h에 데이터를 생성함                                               */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 CPS 데이터 삭제                                                 */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.11 최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /*     1.1  mysik  RS      2015.10.06  C20150924_81598_ARES 연결수익성 지표 적재 자동화            */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_cps (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0406';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        /* Start -- 2015.10.06  C20150924_81598_ARES 연결수익성 지표 적재 자동화            */
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log
        /* End -- 2015.10.06  C20150924_81598_ARES 연결수익성 지표 적재 자동화            */

        /*----------------------------------------
           기준월의 기존 CPS 데이터 삭제
        ----------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_div_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_div_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT INTO npt_rs_mgr.tb_rs_kpi_div_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)
            /*----- 실적 및 이동계획 집계 -----*/
            SELECT
                 ABC.acctg_yyyymm as base_yyyymm
                ,ABC.scenario_type_cd
                ,ABC.div_cd
                ,'N' as manual_adj_flag
                ,C.cd_id as kpi_cd
                ,iv_category as cat_cd
                ,MIN(CASE ABC.scenario_type_cd
                          WHEN 'AC0' THEN ABC.acctg_yyyymm
                          WHEN 'PR1' THEN to_char(add_months(to_date(ABC.acctg_yyyymm,'YYYYMM'), 1), 'YYYYMM')
                          WHEN 'PR2' THEN to_char(add_months(to_date(ABC.acctg_yyyymm,'YYYYMM'), 2), 'YYYYMM')
                          WHEN 'PR3' THEN to_char(add_months(to_date(ABC.acctg_yyyymm,'YYYYMM'), 3), 'YYYYMM')
                          WHEN 'PR4' THEN to_char(add_months(to_date(ABC.acctg_yyyymm,'YYYYMM'), 4), 'YYYYMM')
                     END) as apply_yyyymm
                ,SUM(CASE C.cd_id
                     WHEN 'CPS_SALE' THEN
                           v_net_sales_krw_amt
                     WHEN 'CPS_MGN_PROFIT' THEN
                           v_mgn_profit_krw_amt
                     WHEN 'CPS_COI' THEN
                           v_op_inc_krw_amt
                     WHEN 'CPS_V_MTL' THEN
                           v_mtl_krw_amt
                     ELSE 0 END) as currm_krw_amt
                ,SUM(CASE C.cd_id
                     WHEN 'CPS_SALE' THEN
                           v_net_sales_usd_amt
                     WHEN 'CPS_MGN_PROFIT' THEN
                           v_mgn_profit_usd_amt
                     WHEN 'CPS_COI' THEN
                           v_op_inc_usd_amt
                     WHEN 'CPS_V_MTL' THEN
                           v_mtl_usd_amt
                     ELSE 0 END) as currm_usd_amt
                ,SUM(CASE C.cd_id
                     WHEN 'CPS_SALE' THEN
                           v_net_sales_accu_krw_amt
                     WHEN 'CPS_MGN_PROFIT' THEN
                           v_mgn_profit_accu_krw_amt
                     WHEN 'CPS_COI' THEN
                           v_op_inc_accu_krw_amt
                     WHEN 'CPS_V_MTL' THEN
                           v_mtl_accu_krw_amt
                     ELSE 0 END) as accu_krw_amt
                ,SUM(CASE C.cd_id
                     WHEN 'CPS_SALE' THEN
                           v_net_sales_accu_usd_amt
                     WHEN 'CPS_MGN_PROFIT' THEN
                           v_mgn_profit_accu_usd_amt
                     WHEN 'CPS_COI' THEN
                           v_op_inc_accu_usd_amt
                     WHEN 'CPS_V_MTL' THEN
                           v_mtl_accu_usd_amt
                     ELSE 0 END) as accu_usd_amt
                ,SYSDATE
                ,'ares'
                ,SYSDATE
                ,'ares'
            FROM
            (/*----- 실적 -----*/
              select acctg_yyyymm
                    ,scenario_type_cd
                    ,div_cd
                    ,mdl_sffx_cd
                    ,sum(currm_krw_nsales_amt) as v_net_sales_krw_amt
                    ,sum(currm_usd_nsales_amt) as v_net_sales_usd_amt
                    ,sum(currm_krw_mgnl_prf_amt) as v_mgn_profit_krw_amt
                    ,sum(currm_usd_mgnl_prf_amt) as v_mgn_profit_usd_amt
                    ,sum(currm_krw_oi_amt) as v_op_inc_krw_amt
                    ,sum(currm_usd_oi_amt) as v_op_inc_usd_amt
                    ,sum(currm_krw_var_mtl_cost_amt) as v_mtl_krw_amt
                    ,sum(currm_usd_var_mtl_cost_amt) as v_mtl_usd_amt
                    ,sum(accum_krw_nsales_amt) as v_net_sales_accu_krw_amt
                    ,sum(accum_usd_nsales_amt) as v_net_sales_accu_usd_amt
                    ,sum(accum_krw_mgnl_prf_amt) as v_mgn_profit_accu_krw_amt
                    ,sum(accum_usd_mgnl_prf_amt) as v_mgn_profit_accu_usd_amt
                    ,sum(accum_krw_oi_amt) as v_op_inc_accu_krw_amt
                    ,sum(accum_usd_oi_amt) as v_op_inc_accu_usd_amt
                    ,sum(accum_krw_var_mtl_cost_amt) as v_mtl_accu_krw_amt
                    ,sum(accum_usd_var_mtl_cost_amt) as v_mtl_accu_usd_amt
              FROM  TB_APO_BEP_MDL_CUST_PRFT_D   -- 실적 table from CPS
              WHERE acctg_yyyymm = iv_yyyymm
              and   scenario_type_cd = 'AC0'
              --and   oth_sales_incl_excl_cd = 'N' -- 기타매출포함제외코드(N: 기타매출포함, Y: 기타매출제외)
              and   vrnc_alc_incl_excl_cd = 'Y'  -- 차액배부포함제외코드(N: 차액배부전, Y: 차액배부후)
              and   consld_sales_mdl_flag = 'Y'  -- 연결매출모델여부
              --and   mdl_sffx_cd not like 'VM-%.CPS'
              group by acctg_yyyymm
                      ,scenario_type_cd
                      ,div_cd
                      ,mdl_sffx_cd
              UNION ALL
              /*----- 이동계획 -----*/
              SELECT iv_yyyymm as acctg_yyyymm
                    ,'PR'||months_between(add_months(to_date(pln_yyyymm, 'YYYYMM'),1), to_date(pln_period_yyyymm, 'YYYYMM')) as scenario_type_cd
                    ,div_cd
                    ,mdl_sffx_cd
                    ,sum(decode(bep_idx_cd, 'BEP20000000', krw_amt, 0)) as v_net_sales_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP20000000', usd_amt, 0)) as v_net_sales_usd_amt
                    ,sum(decode(bep_idx_cd, 'BEP50000000', krw_amt, 0)) as v_mgn_profit_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP50000000', usd_amt, 0)) as v_mgn_profit_usd_amt
                    ,sum(decode(bep_idx_cd, 'BEP60000000', krw_amt, 0)) as v_op_inc_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP60000000', usd_amt, 0)) as v_op_inc_usd_amt
                    ,sum(decode(bep_idx_cd, 'BEP30010000', krw_amt, 0)) as v_mtl_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP30010000', usd_amt, 0)) as v_mtl_usd_amt
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
              FROM(
                  select pln_period_yyyymm
                        ,pln_yyyymm
                        ,div_cd
                        ,mdl_sffx_cd
                        ,bep_idx_cd
                        ,SUM(krw_var_amt            +
                             krw_var_mtrx_adj_amt   +
                             krw_var_mdl_adj_amt    +
                             krw_var_usr_dimpos_amt +
                             krw_var_comn_alc_amt   +
                             krw_fix_amt            +
                             krw_fix_mtrx_adj_amt   +
                             krw_fix_mdl_adj_amt    +
                             krw_fix_usr_dimpos_amt +
                             krw_fix_comn_alc_amt   ) krw_amt
                        ,SUM(usd_var_amt            +
                             usd_var_mtrx_adj_amt   +
                             usd_var_mdl_adj_amt    +
                             usd_var_usr_dimpos_amt +
                             usd_var_comn_alc_amt   +
                             usd_fix_amt            +
                             usd_fix_mtrx_adj_amt   +
                             usd_fix_mdl_adj_amt    +
                             usd_fix_usr_dimpos_amt +
                             usd_fix_comn_alc_amt   ) usd_amt
                  from  TB_RFC_MDL_CUST_BEP_S     -- 이동계획 table from RF
                  where pln_period_yyyymm = to_char(add_months(to_date(iv_yyyymm, 'YYYYMM'), 1), 'YYYYMM')
                  and   intrnl_sales_flag = 'N'   -- 내부매출여부(Y: Internal, N: External)
                  and   condl_sales_cd = 'N'      -- 검수도매출코드(Y: 검수도, N: 인수도)
                  and   bep_idx_cd in ('BEP20000000', 'BEP50000000', 'BEP60000000', 'BEP30010000')
                  --and   mdl_sffx_cd not like 'VM-%.CPS'
                  group by pln_period_yyyymm
                          ,pln_yyyymm
                          ,div_cd
                          ,mdl_sffx_cd
                          ,bep_idx_cd
                  )
             group by  pln_period_yyyymm
                      ,pln_yyyymm
                      ,div_cd
                      ,mdl_sffx_cd
            ) ABC -- end of 실적 & 이동계획
            LEFT OUTER JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C
            on  C.CD_CLSF_ID = 'KPI_TYPE'
            and C.CD_ID in ('CPS_MGN_PROFIT', 'CPS_V_MTL', 'CPS_SALE', 'CPS_COI')
            WHERE ABC.scenario_type_cd in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
            AND   NOT EXISTS (SELECT *
                              FROM   npt_rs_mgr.tb_rs_clss_cd_m DC
                              WHERE  DC.cd_clsf_id = 'EXCEPT_MODEL'
                              AND    DC.cd_id = ABC.mdl_sffx_cd
                              AND    DC.attribute1_value = ABC.div_cd)
            GROUP BY ABC.acctg_yyyymm
                    ,ABC.scenario_type_cd
                    ,ABC.div_cd
                    ,C.cd_id
            ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_kpi_div_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
            상위 사업부 데이터 생성
        ---------------------------------------------*/
        PG_RS_KPI_HR.SP_RS_ROLLUP_DIV(iv_yyyymm, iv_category, iv_div_yyyymm);

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_div_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_CPS;

    PROCEDURE SP_RS_KPI_CORP_COUNTRY(iv_yyyymm     IN VARCHAR2
                                    ,iv_category   IN VARCHAR2
                                    ,iv_div_yyyymm IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_corp_country                                                          */
        /* 4.기       능 :                                                                                 */
        /*                 법인, RNR별로 BEP 매출액, 한계이익, 영업이익을 집계하여                         */
        /*                 tb_rs_kpi_subsdr_cntry_h에 데이터를 생성함                                      */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( 시산구분 )                                                  */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                 이동계획 - TB_RFC_MDL_CUST_BEP_S                                                */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_REGION 데이터 삭제                                          */
        /*                2) Insert from source table                                                      */
        /*                3) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.09  최초작성                                                    */
        /*                                     실적 및 이동계획 소스테이블이 바뀔 수 있음                  */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_corp_country (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0407';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log
        /*---------------------------------------------
           기준월의 기존 BEP_REGION 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = iv_category
            AND    kpi_cd in ('SALE', 'COI', 'MGN_PROFIT')
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_subsdr_cntry_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
            INSERT
            INTO   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
             (base_yyyymm
             ,scenario_type_cd
             ,kpi_cd
             ,cat_cd
             ,div_cd
             ,subsdr_cd
             ,subsdr_rnr_cd
             ,au_rnr_cd
             ,regn_rnr_cd
             ,zone_rnr_cd
             ,cntry_cd
             ,regn_cd
             ,manual_adj_flag
             ,currm_krw_amt
             ,currm_usd_amt
             ,accu_krw_amt
             ,accu_usd_amt
             ,creation_date
             ,creation_usr_id
             ,last_upd_date
             ,last_upd_usr_id)
            /*----- 실적 및 이동계획 집계 -----*/
            SELECT
                 ABC.acctg_yyyymm as base_yyyymm
                ,ABC.scenario_type_cd
                ,C.cd_id as kpi_cd
                ,iv_category as cat_cd
                ,ABC.div_cd
                ,ABC.subsdr_cd
                ,substr(ABC.subsdr_rnr_cd,1,4) as subsdr_rnr_cd
                ,nvl(case when S.subsdr_cd in ( 'EMLF', 'EMAF', 'EMDF', 'EMGF' )
                          then substr(ABC.subsdr_rnr_cd,6,3)
                          else au_rnr_cd
                     end, '*') as au_rnr_cd
                ,nvl(S.rhq_cd, '*') as regn_rnr_cd
                ,ABC.zone_rnr_cd
                ,ABC.cntry_cd
                ,ABC.regn_cd
                ,'N' as manual_adj_flag
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_krw_amt
                     WHEN 'COI' THEN
                           v_op_inc_krw_amt
                     ELSE 0 END) as currm_krw_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_usd_amt
                     WHEN 'COI' THEN
                           v_op_inc_usd_amt
                     ELSE 0 END) as currm_usd_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_accu_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_accu_krw_amt
                     WHEN 'COI' THEN
                           v_op_inc_accu_krw_amt
                     ELSE 0 END) as accu_krw_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_accu_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_accu_usd_amt
                     WHEN 'COI' THEN
                           v_op_inc_accu_usd_amt
                     ELSE 0 END) as accu_usd_amt
                ,SYSDATE
                ,'ares'
                ,SYSDATE
                ,'ares'
            FROM
            (/*----- 실적 -----*/
              --select acctg_yyyymm
              select /*+ parallel(a 32) */ acctg_yyyymm
                    ,scenario_type_cd
                    ,div_cd
                    ,subsdr_cd
                    ,nvl(subsdr_rnr_cd, '*') as subsdr_rnr_cd
                    ,nvl(au_rnr_cd, '*') as au_rnr_cd
                    ,zone_rnr_cd
                    ,cntry_cd
                    ,regn_cd
                    ,mdl_sffx_cd
                    ,sum(currm_krw_nsales_amt) as v_net_sales_krw_amt
                    ,sum(currm_usd_nsales_amt) as v_net_sales_usd_amt
                    ,sum(currm_krw_mgnl_prf_amt) as v_mgn_profit_krw_amt
                    ,sum(currm_usd_mgnl_prf_amt) as v_mgn_profit_usd_amt
                    ,sum(currm_krw_oi_amt) as v_op_inc_krw_amt
                    ,sum(currm_usd_oi_amt) as v_op_inc_usd_amt
                    ,sum(accum_krw_nsales_amt) as v_net_sales_accu_krw_amt
                    ,sum(accum_usd_nsales_amt) as v_net_sales_accu_usd_amt
                    ,sum(accum_krw_mgnl_prf_amt) as v_mgn_profit_accu_krw_amt
                    ,sum(accum_usd_mgnl_prf_amt) as v_mgn_profit_accu_usd_amt
                    ,sum(accum_krw_oi_amt) as v_op_inc_accu_krw_amt
                    ,sum(accum_usd_oi_amt) as v_op_inc_accu_usd_amt
              FROM  TB_APO_BEP_MDL_CUST_PRFT_D   -- 실적 table from CPS
              WHERE acctg_yyyymm = iv_yyyymm
              and   scenario_type_cd = 'AC0'
              and   oth_sales_incl_excl_cd = 'N' -- 기타매출포함제외코드(N: 기타매출포함, Y: 기타매출제외)
              and   vrnc_alc_incl_excl_cd = 'Y'  -- 차액배부포함제외코드(N: 차액배부전, Y: 차액배부후)
              and   consld_sales_mdl_flag = 'Y'  -- 연결매출모델여부
              group by acctg_yyyymm
                      ,scenario_type_cd
                      ,div_cd
                      ,subsdr_cd
                      ,subsdr_rnr_cd
                      ,au_rnr_cd
                      ,zone_rnr_cd
                      ,cntry_cd
                      ,regn_cd
                      ,mdl_sffx_cd
              UNION ALL
              /*----- 이동계획 -----*/
              SELECT iv_yyyymm as acctg_yyyymm
                    ,'PR'||months_between(add_months(to_date(pln_yyyymm, 'YYYYMM'),1), to_date(pln_period_yyyymm, 'YYYYMM')) as scenario_type_cd
                    ,div_cd
                    ,subsdr_cd
                    ,nvl(subsdr_rnr_cd, '*') as subsdr_rnr_cd
                    ,nvl(au_rnr_cd, '*') as au_rnr_cd
                    ,zone_rnr_cd
                    ,cntry_cd
                    ,regn_cd
                    ,mdl_sffx_cd
                    ,sum(decode(bep_idx_cd, 'BEP20000000', krw_amt, 0)) as v_net_sales_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP20000000', usd_amt, 0)) as v_net_sales_usd_amt
                    ,sum(decode(bep_idx_cd, 'BEP50000000', krw_amt, 0)) as v_mgn_profit_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP50000000', usd_amt, 0)) as v_mgn_profit_usd_amt
                    ,sum(decode(bep_idx_cd, 'BEP60000000', krw_amt, 0)) as v_op_inc_krw_amt
                    ,sum(decode(bep_idx_cd, 'BEP60000000', usd_amt, 0)) as v_op_inc_usd_amt
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
                    ,null
              FROM(
                  --select pln_period_yyyymm
                  select /*+ parallel(TB_RFC_MDL_CUST_BEP_S 32) */ pln_period_yyyymm
                        ,pln_yyyymm
                        ,div_cd
                        ,subsdr_cd
                        ,zone_rnr_cd
                        ,subsdr_rnr_cd
                        ,'*'  as au_rnr_cd
                        ,cntry_cd
                        ,regn_cd
                        ,mdl_sffx_cd
                        ,bep_idx_cd
                        ,SUM(krw_var_amt            +
                             krw_var_mtrx_adj_amt   +
                             krw_var_mdl_adj_amt    +
                             krw_var_usr_dimpos_amt +
                             krw_var_comn_alc_amt   +
                             krw_fix_amt            +
                             krw_fix_mtrx_adj_amt   +
                             krw_fix_mdl_adj_amt    +
                             krw_fix_usr_dimpos_amt +
                             krw_fix_comn_alc_amt   ) krw_amt
                        ,SUM(usd_var_amt            +
                             usd_var_mtrx_adj_amt   +
                             usd_var_mdl_adj_amt    +
                             usd_var_usr_dimpos_amt +
                             usd_var_comn_alc_amt   +
                             usd_fix_amt            +
                             usd_fix_mtrx_adj_amt   +
                             usd_fix_mdl_adj_amt    +
                             usd_fix_usr_dimpos_amt +
                             usd_fix_comn_alc_amt   ) usd_amt
                  from  TB_RFC_MDL_CUST_BEP_S     -- 이동계획 table from RF
                  where pln_period_yyyymm = to_char(add_months(to_date(iv_yyyymm, 'YYYYMM'), 1), 'YYYYMM')
                  and   intrnl_sales_flag = 'N'   -- 내부매출여부(Y: Internal, N: External)
                  and   condl_sales_cd = 'N'      -- 검수도매출코드(Y: 검수도, N: 인수도)
                  and   bep_idx_cd in ('BEP20000000', 'BEP50000000', 'BEP60000000')
                   group by pln_period_yyyymm
                          ,pln_yyyymm
                          ,div_cd
                          ,subsdr_cd
                          ,zone_rnr_cd
                          ,subsdr_rnr_cd
                          ,cntry_cd
                          ,regn_cd
                          ,mdl_sffx_cd
                          ,bep_idx_cd
                  )
             group by  pln_period_yyyymm
                      ,pln_yyyymm
                      ,div_cd
                      ,subsdr_cd
                      ,subsdr_rnr_cd
                      ,au_rnr_cd
                      ,zone_rnr_cd
                      ,cntry_cd
                      ,regn_cd
                      ,mdl_sffx_cd
            ) ABC -- end of 실적 & 이동계획
            LEFT OUTER JOIN NPT_APP.NV_DWD_MGT_ORG_RNR_M S
            on  ABC.subsdr_rnr_cd = S.mgt_org_cd
            --LEFT OUTER JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C
            JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C              ---<< LEFT OUTER 삭제
            on  C.CD_CLSF_ID = 'KPI_TYPE'
            and C.CD_ID in ('SALE','MGN_PROFIT','COI')
            WHERE ABC.scenario_type_cd in ('AC0', 'PR1', 'PR2', 'PR3', 'PR4')
            AND   NOT EXISTS (SELECT *
                              FROM   npt_rs_mgr.tb_rs_clss_cd_m DC
                              WHERE  DC.cd_clsf_id = 'EXCEPT_MODEL'
                              AND    DC.cd_id = ABC.mdl_sffx_cd
                              AND    DC.attribute1_value = ABC.div_cd
                              AND    DC.cd_desc = '계획광고비 직과 모델')
            GROUP BY ABC.acctg_yyyymm
                    ,ABC.scenario_type_cd
                    ,C.cd_id
                    ,ABC.div_cd
                    ,ABC.subsdr_cd
                    ,substr(ABC.subsdr_rnr_cd,1,4)
                    ,nvl(case when S.subsdr_cd in ( 'EMLF', 'EMAF', 'EMDF', 'EMGF' )
                              then substr(ABC.subsdr_rnr_cd,6,3)
                              else au_rnr_cd
                         end, '*')
                    ,nvl(S.rhq_cd, '*')
                    ,ABC.zone_rnr_cd
                    ,ABC.cntry_cd
                    ,ABC.regn_cd
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
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('2)Insert success row : ' || vn_insert_row_cnt);

        /*--------------------------------------------
            상위 사업부 데이터 생성
        ---------------------------------------------*/
        SP_RS_ROLLUP_SUBSDR_CNTRY(iv_yyyymm, iv_category, iv_div_yyyymm);

        /*---------------------------------------------------
           MC 본부의 영업이익을 본사에서의 MANUAL로 저장
        ----------------------------------------------------*/
        DELETE
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    scenario_type_cd = 'AC0'
        AND    kpi_cd = 'COI'
        AND    cat_cd = 'BEP_REGION_MANUAL'
        AND    div_cd = 'GBU'
        AND    subsdr_cd = '*'
        AND    subsdr_rnr_cd = '*'
        AND    au_rnr_cd = '*'
        AND    regn_rnr_cd = '*'
        AND    zone_rnr_cd = 'ZKR'
        AND    cntry_cd = '*'
        AND    regn_cd = 'K'
        AND    manual_adj_flag = 'Y';

        vn_row_cnt := SQL%ROWCOUNT;
        vn_delete_row_cnt := vn_delete_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_delete_row_cnt);

        INSERT INTO npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        (
           base_yyyymm
          ,scenario_type_cd
          ,kpi_cd
          ,cat_cd
          ,div_cd
          ,subsdr_cd
          ,subsdr_rnr_cd
          ,au_rnr_cd
          ,regn_rnr_cd
          ,zone_rnr_cd
          ,cntry_cd
          ,regn_cd
          ,manual_adj_flag
          ,currm_krw_amt
          ,currm_usd_amt
          ,accu_krw_amt
          ,accu_usd_amt
          ,creation_date
          ,creation_usr_id
          ,last_upd_date
          ,last_upd_usr_id
        )
        SELECT iv_yyyymm
              ,T.scenario_type_cd
              ,T.kpi_cd
              ,'BEP_REGION_MANUAL'
              ,'GBU'
              ,'*'
              ,'*'
              ,'*'
              ,'*'
              ,'ZKR'
              ,'*'
              ,'K'
              ,'Y'
              ,SUM(T.currm_krw_amt)
              ,SUM(T.currm_usd_amt)
              ,SUM(T.accu_krw_amt)
              ,SUM(T.accu_usd_amt)
              ,SYSDATE
              ,'ares'
              ,SYSDATE
              ,'ares'
        FROM  npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h T
        WHERE T.cat_cd LIKE 'BEP_REGION%'
        AND   T.base_yyyymm = iv_yyyymm
        AND   T.scenario_type_cd = 'AC0'
        AND   T.div_cd = 'MST'
        AND   T.kpi_cd = 'COI'
        AND   T.subsdr_rnr_cd = 'EKHQ'
        AND   T.cntry_cd = 'KR'
        GROUP BY T.scenario_type_cd
                ,T.kpi_cd;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('4)Insert success row : ' || vn_insert_row_cnt);

        /*----------------------------------------------------------
           CAV 사업부는 PNT + PHT 데이타로만 집계 요청(2014.2.10)
        ----------------------------------------------------------*/
        DELETE
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    cat_cd = 'BEP_REGION'
        AND    kpi_cd in ('SALE', 'COI', 'MGN_PROFIT')
        AND    manual_adj_flag = 'N'
        AND    div_cd = 'CMS';

        vn_row_cnt := SQL%ROWCOUNT;
        vn_delete_row_cnt := vn_delete_row_cnt + vn_row_cnt;
        dbms_output.put_line('5)Insert success row : ' || vn_delete_row_cnt);

        INSERT
        INTO   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        (
           base_yyyymm
          ,scenario_type_cd
          ,kpi_cd
          ,cat_cd
          ,div_cd
          ,subsdr_cd
          ,subsdr_rnr_cd
          ,au_rnr_cd
          ,regn_rnr_cd
          ,zone_rnr_cd
          ,cntry_cd
          ,regn_cd
          ,manual_adj_flag
          ,currm_krw_amt
          ,currm_usd_amt
          ,accu_krw_amt
          ,accu_usd_amt
          ,creation_date
          ,creation_usr_id
          ,last_upd_date
          ,last_upd_usr_id
        )
        SELECT
           base_yyyymm
          ,scenario_type_cd
          ,kpi_cd
          ,cat_cd
          ,'CMS' AS div_cd
          ,subsdr_cd
          ,subsdr_rnr_cd
          ,au_rnr_cd
          ,regn_rnr_cd
          ,zone_rnr_cd
          ,cntry_cd
          ,regn_cd
          ,manual_adj_flag
          ,SUM(currm_krw_amt)
          ,SUM(currm_usd_amt)
          ,SUM(accu_krw_amt)
          ,SUM(accu_usd_amt)
          ,SYSDATE
          ,'ares'
          ,SYSDATE
          ,'ares'
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    cat_cd = 'BEP_REGION'
        AND    kpi_cd in ('SALE', 'COI', 'MGN_PROFIT')
        AND    manual_adj_flag = 'N'
        AND    div_cd IN ( 'PNT','PHT')
        GROUP BY base_yyyymm
                ,scenario_type_cd
                ,kpi_cd
                ,cat_cd
                ,subsdr_cd
                ,subsdr_rnr_cd
                ,au_rnr_cd
                ,regn_rnr_cd
                ,zone_rnr_cd
                ,cntry_cd
                ,regn_cd
                ,manual_adj_flag ;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('6)Insert success row : ' || vn_insert_row_cnt);


        DELETE
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    cat_cd = 'BEP_REGION'
        AND    kpi_cd in ('SALE', 'COI', 'MGN_PROFIT')
        AND    manual_adj_flag = 'N'
        AND    div_cd in ( 'PNT','PHT');

        vn_row_cnt := SQL%ROWCOUNT;
        vn_delete_row_cnt := vn_delete_row_cnt + vn_row_cnt;
        dbms_output.put_line('7)Insert success row : ' || vn_delete_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_subsdr_cntry_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_CORP_COUNTRY;

    PROCEDURE SP_RS_KPI_MKTG_CHNLL(iv_yyyymm  IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : sp_rs_kpi_mktg_chnll                                                            */
        /* 4.기       능 :                                                                                 */
        /*                 Marketing Report를 위해 tb_rs_kpi_subsdr_cntry_h 테이블에 데이터 적재함         */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                                                                                                 */
        /* 6.Source      : 실적 - TB_APO_BEP_MDL_CUST_PRFT_D                                               */
        /*                                                                                                 */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 기준월의 기존 BEP_CHANNEL 데이터 삭제                                         */
        /*                2) Insert from source table                                                      */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.12  최초작성                                                   */
        /*                                     실적 소스테이블이 바뀔 수 있음                              */
        /*     1.0  mysik  RS       2015.10.05  C20150924_81594 ARES 유통채널별 KPI 적재 자동화            */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_mktg_chnll (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0408';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);


       /* Start -- 2015.10.05  C20150924_81594 ARES 유통채널별 KPI 적재 자동화            */
        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log
        /* End -- 2015.10.05  C20150924_81594 ARES 유통채널별 KPI 적재 자동화            */

        /*---------------------------------------------
           기준월의 기존 BEP_CHANNEL 데이터 삭제
        ----------------------------------------------*/
        BEGIN
            DELETE
            FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
            WHERE  base_yyyymm = iv_yyyymm
            AND    cat_cd = 'BEP_CHANNEL'
            AND    manual_adj_flag = 'N';

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_kpi_subsdr_cntry_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('1)Delete success row : ' || vn_delete_row_cnt);

        /*--------------------------------
             Insert from source table
        --------------------------------*/
        BEGIN
          INSERT
          INTO npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
             (base_yyyymm
             ,scenario_type_cd
             ,kpi_cd
             ,cat_cd
             ,div_cd
             ,subsdr_cd
             ,subsdr_rnr_cd
             ,au_rnr_cd
             ,regn_rnr_cd
             ,zone_rnr_cd
             ,cntry_cd
             ,regn_cd
             ,manual_adj_flag
             ,currm_krw_amt
             ,currm_usd_amt
             ,accu_krw_amt
             ,accu_usd_amt
             ,creation_date
             ,creation_usr_id
             ,last_upd_date
             ,last_upd_usr_id)

          WITH BEP_MODEL_BUYER_V
           ( base_yyyymm,
             scenario_type_cd,
             kpi_cd,
             cat_cd,
             div_cd ,
             subsdr_cd ,
             subsdr_rnr_cd,
             au_rnr_cd,
             regn_rnr_cd,
             zone_rnr_cd,
             cntry_cd,
             regn_cd,
             billto_cust_cd,
             shipto_cust_cd,
             manual_adj_flag,
             currm_krw_amt,
             currm_usd_amt,
             accu_krw_amt,
             accu_usd_amt
           )
          AS (
            SELECT
                 ABC.acctg_yyyymm as base_yyyymm
                ,ABC.scenario_type_cd
                ,C.cd_id as kpi_cd
                ,'BEP_CHANNEL' as cat_cd
                ,ABC.div_cd
                ,ABC.subsdr_cd
                ,substr(ABC.subsdr_rnr_cd,1,4) as subsdr_rnr_cd
                ,nvl(case when S.subsdr_cd in ( 'EMLF', 'EMAF', 'EMDF', 'EMGF' )
                          then substr(ABC.subsdr_rnr_cd,6,3)
                          else au_rnr_cd
                     end, '*') as au_rnr_cd
                ,nvl(S.rhq_cd, '*') as regn_rnr_cd
                ,ABC.zone_rnr_cd
                ,ABC.cntry_cd
                ,ABC.regn_cd
                ,ABC.billto_cust_cd
                ,ABC.shipto_cust_cd
                ,'N' as manual_adj_flag
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_krw_amt
                     WHEN 'COI' THEN
                           v_op_inc_krw_amt
                     ELSE 0 END) as currm_krw_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_usd_amt
                     WHEN 'COI' THEN
                           v_op_inc_usd_amt
                     ELSE 0 END) as currm_usd_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_accu_krw_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_accu_krw_amt
                     WHEN 'COI' THEN
                           v_op_inc_accu_krw_amt
                     ELSE 0 END) as accu_krw_amt
                ,sum(CASE C.cd_id
                     WHEN 'SALE' THEN
                           v_net_sales_accu_usd_amt
                     WHEN 'MGN_PROFIT' THEN
                           v_mgn_profit_accu_usd_amt
                     WHEN 'COI' THEN
                           v_op_inc_accu_usd_amt
                     ELSE 0 END) as accu_usd_amt
            FROM (
              --select acctg_yyyymm
              select /*+ parallel(a 32) */ acctg_yyyymm
                    ,scenario_type_cd
                    ,div_cd
                    ,subsdr_cd
                    ,nvl(subsdr_rnr_cd, '*') as subsdr_rnr_cd
                    ,nvl(au_rnr_cd, '*') as au_rnr_cd
                    ,zone_rnr_cd
                    ,cntry_cd
                    ,regn_cd
                    ,billto_cust_cd
                    ,shipto_cust_cd
                    ,mdl_sffx_cd
                    ,sum(currm_krw_nsales_amt) as v_net_sales_krw_amt
                    ,sum(currm_usd_nsales_amt) as v_net_sales_usd_amt
                    ,sum(currm_krw_mgnl_prf_amt) as v_mgn_profit_krw_amt
                    ,sum(currm_usd_mgnl_prf_amt) as v_mgn_profit_usd_amt
                    ,sum(currm_krw_oi_amt) as v_op_inc_krw_amt
                    ,sum(currm_usd_oi_amt) as v_op_inc_usd_amt
                    ,sum(accum_krw_nsales_amt) as v_net_sales_accu_krw_amt
                    ,sum(accum_usd_nsales_amt) as v_net_sales_accu_usd_amt
                    ,sum(accum_krw_mgnl_prf_amt) as v_mgn_profit_accu_krw_amt
                    ,sum(accum_usd_mgnl_prf_amt) as v_mgn_profit_accu_usd_amt
                    ,sum(accum_krw_oi_amt) as v_op_inc_accu_krw_amt
                    ,sum(accum_usd_oi_amt) as v_op_inc_accu_usd_amt
              FROM  TB_APO_BEP_MDL_CUST_PRFT_D   -- 실적 table from CPS
              WHERE acctg_yyyymm = iv_yyyymm
              and   scenario_type_cd = 'AC0'
              and   oth_sales_incl_excl_cd = 'N' -- 기타매출포함제외코드(N: 기타매출포함, Y: 기타매출제외)
              and   vrnc_alc_incl_excl_cd = 'Y'  -- 차액배부포함제외코드(N: 차액배부전, Y: 차액배부후)
              and   consld_sales_mdl_flag = 'Y'  -- 연결매출모델여부
              group by acctg_yyyymm
                      ,scenario_type_cd
                      ,div_cd
                      ,subsdr_cd
                      ,subsdr_rnr_cd
                      ,au_rnr_cd
                      ,zone_rnr_cd
                      ,cntry_cd
                      ,regn_cd
                      ,billto_cust_cd
                      ,shipto_cust_cd
                      ,mdl_sffx_cd
            ) ABC -- end of 실적
            LEFT OUTER JOIN NPT_APP.NV_DWD_MGT_ORG_RNR_M S
            on  ABC.subsdr_rnr_cd = S.mgt_org_cd
            --LEFT OUTER JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C
            JOIN npt_rs_mgr.TB_RS_CLSS_CD_M C               ---<< LEFT OUTER 제거
            on  C.CD_CLSF_ID = 'KPI_TYPE'
            and C.CD_ID in ('SALE','MGN_PROFIT','COI')
            WHERE ABC.div_cd in ('GLT', 'PNT', 'PHT', 'CNT', 'DFT', 'CVT', 'DGT', 'DVT')  -- 6개 사업부 only
            AND   NOT EXISTS (SELECT *
                              FROM   npt_rs_mgr.tb_rs_clss_cd_m DC
                              WHERE  DC.cd_clsf_id = 'EXCEPT_MODEL'
                              AND    DC.cd_id = ABC.mdl_sffx_cd
                              AND    DC.attribute1_value = ABC.div_cd
                              AND    DC.cd_desc = '계획광고비 직과 모델')
            GROUP BY ABC.acctg_yyyymm
                    ,ABC.scenario_type_cd
                    ,C.cd_id
                    ,ABC.div_cd
                    ,ABC.subsdr_cd
                    ,substr(ABC.subsdr_rnr_cd,1,4)
                    ,nvl(case when S.subsdr_cd in ( 'EMLF', 'EMAF', 'EMDF', 'EMGF' )
                              then substr(ABC.subsdr_rnr_cd,6,3)
                              else au_rnr_cd
                         end, '*')
                    ,nvl(S.rhq_cd, '*')
                    ,ABC.zone_rnr_cd
                    ,ABC.cntry_cd
                    ,ABC.regn_cd
                    ,ABC.billto_cust_cd
                    ,ABC.shipto_cust_cd
          )  -- end of WITH


        /*--------------------------------------------
           1.전체 매출, 영업이익, 한계이익 생성
        ---------------------------------------------*/
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.TB_RS_MKTG_SUBSDR_M b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'             -- USE_FLAG = 'Y' 법인만 대상
          where a.base_yyyymm = iv_yyyymm
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
          UNION ALL
        /*----------------------------------------------------------------------------------------------
           2-1.전국유통 매출, 영업이익, 한계이익 생성 - EASL, ECHK 법인(거래선 전체를 전국유통으로 함)
        -----------------------------------------------------------------------------------------------*/
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,'NATION_'||a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.TB_RS_MKTG_SUBSDR_M b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'
          and   b.subsdr_cd in ('EASL', 'ECHK') -- EASL, ECHK 법인만 대상
          where a.base_yyyymm = iv_yyyymm
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
          UNION ALL
        /*-------------------------------------------------------------------------------------
           2-2. 전국유통 매출, 영업이익, 한계이익 생성 - EKHQ법인 (KR은 Online제외 모두 전국)
        -------------------------------------------------------------------------------------*/
        -- EKHQ법인 전국유통 로직 재검토 필요 (온라인 제외하는 로직이 없음_2014.12.12)
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,'NATION_'||a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.TB_RS_MKTG_SUBSDR_M b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'
          and   b.subsdr_cd = 'EKHQ'  -- KR법인 : 온라인 이외 모두 전국유통
          where a.base_yyyymm = iv_yyyymm
          and   exists (
                        select *
                        from  TB_CM_SUBSDR_CUST_PERIOD_H c
                        where c.mgt_type_cd = 'CM'
                        and   c.acctg_yyyymm = '*'
                        and   c.acctg_week = '*'
                        and   c.temp_flag = 'N'
                        and   a.subsdr_cd = c.subsdr_cd
                        and  (a.billto_cust_cd = c.cust_cd or
                              a.shipto_cust_cd = c.cust_cd )
                        and   c.use_flag = 'Y'
                      )
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
          UNION ALL
        /*-------------------------------------------------------------------
           2-3.전국유통 매출, 영업이익, 한계이익 생성 - EASL, ECHK, EKHQ 이외
        ---------------------------------------------------------------------*/
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,'NATION_'||a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.TB_RS_MKTG_SUBSDR_M b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'
          and   b.subsdr_cd not in ('EASL', 'ECHK', 'EKHQ') -- EASL, ECHK, EKHQ 이외 법인
          where a.base_yyyymm = iv_yyyymm
          and   exists (
                        select *
                        from  TB_CM_SUBSDR_CUST_PERIOD_H c
                        where c.mgt_type_cd = 'CM'
                        and   c.acctg_yyyymm = '*'
                        and   c.acctg_week = '*'
                        and   c.temp_flag = 'N'
                        and   a.subsdr_cd = c.subsdr_cd
                        and  (a.billto_cust_cd = c.cust_cd or
                              a.shipto_cust_cd = c.cust_cd )
                        --and   c.use_flag = 'Y'
                        and   c.chnl_type_cd = '10' -- 전국 유통
                      )
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
          UNION ALL
        /*----------------------------------------------
           3. ONLINE유통 매출, 영업이익, 한계이익 생성
        -----------------------------------------------*/
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,'ONLINE_'||a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.tb_rs_mktg_subsdr_m b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'
          and   b.subsdr_cd not in ('EASL', 'ECHK') -- EASL, ECHK 이외 법인
          where a.base_yyyymm = iv_yyyymm
          and   exists (
                        select *
                        from  TB_CM_SUBSDR_CUST_PERIOD_H c
                        where c.mgt_type_cd = 'CM'
                        and   c.acctg_yyyymm = '*'
                        and   c.acctg_week = '*'
                        and   c.temp_flag = 'N'
                        and   a.subsdr_cd = c.subsdr_cd
                        and  (a.billto_cust_cd = c.cust_cd or
                              a.shipto_cust_cd = c.cust_cd )
                        --and   c.use_flag = 'Y'
                        and   c.chnl_type_cd = '30'  -- ONLINE 유통
                      )
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
          UNION ALL
        /*--------------------------------------------
           4. 지방유통 매출, 영업이익, 한계이익 생성
        ---------------------------------------------*/
          SELECT a.base_yyyymm
                ,a.scenario_type_cd
                ,'LOCAL_'||a.kpi_cd
                ,a.cat_cd
                ,a.div_cd
                ,a.subsdr_cd
                ,a.subsdr_rnr_cd
                ,a.au_rnr_cd
                ,a.regn_rnr_cd
                ,a.zone_rnr_cd
                ,a.cntry_cd
                ,a.regn_cd
                ,a.manual_adj_flag
                ,SUM(a.currm_krw_amt)
                ,SUM(a.currm_usd_amt)
                ,SUM(a.accu_krw_amt)
                ,SUM(a.accu_usd_amt)
                ,sysdate
                ,'ares'
                ,sysdate
                ,'ares'
          FROM  BEP_MODEL_BUYER_V a
          inner join npt_rs_mgr.tb_rs_mktg_subsdr_m b
          on    nvl(a.subsdr_rnr_cd, '*') = b.subsdr_cd
          and   b.use_flag = 'Y'
          and   b.subsdr_cd not in ('EASL', 'ECHK', 'EKHQ')
          where a.base_yyyymm = iv_yyyymm
          and   exists (
                        select *
                        from  TB_CM_SUBSDR_CUST_PERIOD_H c
                        where c.mgt_type_cd = 'CM'
                        and   c.acctg_yyyymm = '*'
                        and   c.acctg_week = '*'
                        and   c.temp_flag = 'N'
                        and   a.subsdr_cd = c.subsdr_cd
                        and  (a.billto_cust_cd = c.cust_cd or
                              a.shipto_cust_cd = c.cust_cd )
                        --and   c.use_flag = 'Y'
                        and   c.chnl_type_cd = '20'  -- 지방 유통
                      )
          group by a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.div_cd
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
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
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        /*----------------------------------------------------------
           CAV 사업부는 PNT + PHT 데이타로만 집계 요청(2014.2.10)
        -----------------------------------------------------------*/
        DELETE
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    cat_cd = 'BEP_CHANNEL'
        AND    manual_adj_flag = 'N'
        AND    div_cd = 'CMS';

        vn_row_cnt := SQL%ROWCOUNT;
        vn_delete_row_cnt := vn_delete_row_cnt + vn_row_cnt;
        dbms_output.put_line('4)Insert success row : ' || vn_delete_row_cnt);

        INSERT
        INTO  npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
           (base_yyyymm
           ,scenario_type_cd
           ,kpi_cd
           ,cat_cd
           ,div_cd
           ,subsdr_cd
           ,subsdr_rnr_cd
           ,au_rnr_cd
           ,regn_rnr_cd
           ,zone_rnr_cd
           ,cntry_cd
           ,regn_cd
           ,manual_adj_flag
           ,currm_krw_amt
           ,currm_usd_amt
           ,accu_krw_amt
           ,accu_usd_amt
           ,creation_date
           ,creation_usr_id
           ,last_upd_date
           ,last_upd_usr_id)
        SELECT
            base_yyyymm
           ,scenario_type_cd
           ,kpi_cd
           ,cat_cd
           ,'CMS' as div_cd
           ,subsdr_cd
           ,subsdr_rnr_cd
           ,au_rnr_cd
           ,regn_rnr_cd
           ,zone_rnr_cd
           ,cntry_cd
           ,regn_cd
           ,manual_adj_flag
           ,SUM(currm_krw_amt)
           ,SUM(currm_usd_amt)
           ,SUM(accu_krw_amt)
           ,SUM(accu_usd_amt)
           ,sysdate
           ,'ares'
           ,sysdate
           ,'ares'
        FROM  npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE base_yyyymm = iv_yyyymm
        AND   cat_cd = 'BEP_CHANNEL'
        AND   manual_adj_flag = 'N'
        AND   div_cd IN ('PNT','PHT')
        GROUP BY base_yyyymm
                ,scenario_type_cd
                ,kpi_cd
                ,cat_cd
                ,subsdr_cd
                ,subsdr_rnr_cd
                ,au_rnr_cd
                ,regn_rnr_cd
                ,zone_rnr_cd
                ,cntry_cd
                ,regn_cd
                ,manual_adj_flag;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('5)Insert success row : ' || vn_insert_row_cnt);

        DELETE
        FROM   npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
        WHERE  base_yyyymm = iv_yyyymm
        AND    cat_cd = 'BEP_CHANNEL'
        AND    manual_adj_flag = 'N'
        AND    div_cd IN ('PNT','PHT');

        vn_row_cnt := SQL%ROWCOUNT;
        vn_delete_row_cnt := vn_delete_row_cnt + vn_row_cnt;
        dbms_output.put_line('6)Insert success row : ' || vn_delete_row_cnt);

        COMMIT;

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_subsdr_cntry_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_KPI_MKTG_CHNLL;

    PROCEDURE SP_RS_ROLLUP_PROD(iv_yyyymm      IN VARCHAR2
                               ,iv_category    IN VARCHAR2
                               ,iv_div_yyyymm  IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : SP_RS_ROLLUP_PROD                                                               */
        /* 4.기       능 :                                                                                 */
        /*                 TB_RS_KPI_PROD_H 테이블에 상위 사업부로 데이터 ROLLUP                           */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( category구분 )                                              */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : TB_RS_KPI_PROD_H                                                                */
        /*                                                                                                 */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.04 최초작성                                                    */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_rollup_prod (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0409';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log

        BEGIN

            INSERT INTO npt_rs_mgr.tb_rs_kpi_prod_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   npt_rs_mgr.tb_rs_kpi_prod_h a
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.apply_yyyymm;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('3) Insert Table Hierarchy tb_rs_kpi_prod_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_prod_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);


    END SP_RS_ROLLUP_PROD;

    PROCEDURE SP_RS_ROLLUP_GRADE(iv_yyyymm      IN VARCHAR2
                                ,iv_category    IN VARCHAR2
                                ,iv_div_yyyymm  IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : SP_RS_ROLLUP_GRADE                                                              */
        /* 4.기       능 :                                                                                 */
        /*                 TB_RS_KPI_GRD_H 테이블에 상위 사업부로 데이터 ROLLUP                            */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( category구분 )                                              */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : TB_RS_KPI_GRD_H                                                                 */
        /*                                                                                                 */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.04 최초작성                                                    */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_rollup_grade (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0410';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log

        BEGIN

            INSERT INTO npt_rs_mgr.tb_rs_kpi_grd_h
                (base_yyyymm
                ,scenario_type_cd
                ,div_cd
                ,manual_adj_flag
                ,kpi_cd
                ,cat_cd
                ,sub_cat_cd
                ,mdl_grd_cd
                ,apply_yyyymm
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)

            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,b.ancestor
                  ,a.manual_adj_flag
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,a.sub_cat_cd
                  ,a.mdl_grd_cd
                  ,a.apply_yyyymm
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM   npt_rs_mgr.tb_rs_kpi_grd_h a
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,b.ancestor
                     ,a.manual_adj_flag
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,a.sub_cat_cd
                     ,a.mdl_grd_cd
                     ,a.apply_yyyymm;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('3) Insert Table Hierarchy tb_rs_kpi_grd_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'TB_RS_KPI_DIV_HEADCNT_H SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_ROLLUP_GRADE;

    PROCEDURE SP_RS_ROLLUP_SUBSDR_CNTRY(iv_yyyymm      IN VARCHAR2
                                       ,iv_category    IN VARCHAR2
                                       ,iv_div_yyyymm  IN VARCHAR2)
        /***************************************************************************************************/
        /* 1.프 로 젝 트 : New Plantopia                                                                   */
        /* 2.모       듈 : RS (ARES)                                                                       */
        /* 3.프로그램 ID : SP_RS_ROLLUP_SUBSDR_CNTRY                                                       */
        /* 4.기       능 :                                                                                 */
        /*                 TB_RS_KPI_SUBSDR_CNTRY_H 테이블에 상위 사업부로 데이터 ROLLUP                   */
        /*                                                                                                 */
        /* 5.입 력 변 수 :                                                                                 */
        /*                 [필수] iv_yyyymm( 기준월 )                                                      */
        /*                 [필수] iv_category( category구분 )                                              */
        /*                 [필수] iv_div_yyyymm( Division기준월 )                                          */
        /*                                                                                                 */
        /* 6.Source      : TB_RS_KPI_SUBSDR_CNTRY_H                                                        */
        /*                                                                                                 */
        /* 7.사  용   예 :                                                                                 */
        /* 8.파 일 위 치 :                                                                                 */
        /* 9. Step      : 1) 상위사업부 데이터 생성                                                        */
        /* 10.변 경 이 력 :                                                                                */
        /* Version  작성자  소속   일    자   내       용                                           요청자 */
        /* -------- ------ ------ ---------- -------------------------------------------------------- -----*/
        /*     1.0  syyim  RS       2014.12.09 최초작성                                                    */
        /***************************************************************************************************/
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_rollup_subsdr_cntry (' || iv_yyyymm || ')'; -- set action name
        vn_row_cnt   NUMBER;

        vv_exception             EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPI0411';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';

        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG 시작
        -- Procedure 등록 : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        npt_app.pg_cm_job_log.sp_cm_start_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                 ,ov_err_cd          => vv_param_err_cd
                                                 ,ov_job_log_id      => vn_job_log_id
                                                 ,iv_module_cd       => vv_module_cd
                                                 ,iv_pgm_cd          => vv_pgm_cd
                                                 ,iv_job_desc        => vv_act_name
                                                 ,iv_usr_id          => vv_usr_id);

        IF vn_job_log_id IS NULL
           OR vn_job_log_id < 1
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC 에러 [' || SQLERRM || ']';
            RAISE vv_exception;
        END IF;

        IF vv_param_err_msg_content IS NOT NULL
        THEN
            RAISE vv_exception;
        END IF;

        IF iv_yyyymm IS NULL
        THEN
            vv_param_err_cd          := '-9999';
            vv_param_err_msg_content := 'YYYYMM is requied parameter!';
            RAISE vv_exception;
        END IF;
        vn_insert_row_cnt   :=0;
        vn_delete_row_cnt   :=0;
        -- Job Log

        BEGIN

            INSERT INTO npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h
                (base_yyyymm
                ,scenario_type_cd
                ,kpi_cd
                ,cat_cd
                ,div_cd
                ,subsdr_cd
                ,subsdr_rnr_cd
                ,au_rnr_cd
                ,regn_rnr_cd
                ,zone_rnr_cd
                ,cntry_cd
                ,regn_cd
                ,manual_adj_flag
                ,currm_krw_amt
                ,currm_usd_amt
                ,accu_krw_amt
                ,accu_usd_amt
                ,creation_date
                ,creation_usr_id
                ,last_upd_date
                ,last_upd_usr_id)
            SELECT a.base_yyyymm
                  ,a.scenario_type_cd
                  ,a.kpi_cd
                  ,a.cat_cd
                  ,b.ancestor
                  ,a.subsdr_cd
                  ,a.subsdr_rnr_cd
                  ,a.au_rnr_cd
                  ,a.regn_rnr_cd
                  ,a.zone_rnr_cd
                  ,a.cntry_cd
                  ,a.regn_cd
                  ,a.manual_adj_flag
                  ,SUM(a.currm_krw_amt)
                  ,SUM(a.currm_usd_amt)
                  ,SUM(a.accu_krw_amt)
                  ,SUM(a.accu_usd_amt)
                  ,SYSDATE
                  ,'ares'
                  ,SYSDATE
                  ,'ares'
            FROM  npt_rs_mgr.tb_rs_kpi_subsdr_cntry_h a
                  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                  ,(SELECT tree.grand_parent AS ancestor ,
                          CASE COALESCE(tree.grand_grand_grand_child, '***')
                              WHEN '***'
                              THEN
                                  CASE COALESCE(tree.grand_grand_child, '***')
                                      WHEN '***'
                                      THEN
                                          CASE COALESCE(tree.grand_child, '***')
                                              WHEN '***'
                                              THEN
                                                  CASE COALESCE(tree.child, '***')
                                                      WHEN '***'
                                                      THEN tree.parent
                                                      ELSE tree.child
                                                  END
                                              ELSE tree.grand_child
                                          END
                                      ELSE tree.grand_grand_child
                                  END
                              ELSE tree.grand_grand_grand_child
                          END AS leaf_child
                     FROM
                          (SELECT hier1.parent_div_cd AS grand_parent ,
                                 hier1.div_cd AS PARENT ,
                                 hier2.div_cd AS CHILD ,
                                 hier3.div_cd AS grand_child ,
                                 hier4.div_cd AS grand_grand_child ,
                                 hier5.div_cd AS grand_grand_grand_child  --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                            FROM
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier1 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier2 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier3 ,
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier4 ,
                                 /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                                 (SELECT c.div_cd ,
                                        c.parent_div_cd
                                   FROM npt_rs_mgr.tb_rs_div_h c ,
                                        npt_rs_mgr.tb_rs_div_h p
                                  WHERE c.base_yyyymm = iv_div_yyyymm
                                        AND c.base_yyyymm = p.base_yyyymm
                                        AND c.parent_div_cd = p.div_cd
                                        AND COALESCE(c.parent_div_cd, '***') <> '***'
                                        AND nvl(c.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                        AND nvl(p.attribute1_value, 'Y') <> 'N' -- 가상 division 제외. 20130908
                                 ) hier5
                           WHERE hier1.div_cd = hier2.parent_div_cd(+)
                                 AND hier2.div_cd = hier3.parent_div_cd(+)
                                 AND hier3.div_cd = hier4.parent_div_cd(+)
                                 AND hier4.div_cd = hier5.parent_div_cd(+) --  /* 조직 Level 추가시 수정할 부분임 2015년 추가함 (LEVEL 5 추가) */
                          ) tree
                   ) b
            WHERE  a.div_cd = b.leaf_child
            AND    a.base_yyyymm = iv_yyyymm
            AND    a.cat_cd = iv_category
            and    a.kpi_cd in ('SALE', 'COI', 'MGN_PROFIT')
            AND    a.manual_adj_flag = 'N'
            GROUP  BY a.base_yyyymm
                     ,a.scenario_type_cd
                     ,a.kpi_cd
                     ,a.cat_cd
                     ,b.ancestor
                     ,a.subsdr_cd
                     ,a.subsdr_rnr_cd
                     ,a.au_rnr_cd
                     ,a.regn_rnr_cd
                     ,a.zone_rnr_cd
                     ,a.cntry_cd
                     ,a.regn_cd
                     ,a.manual_adj_flag;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('3) Insert Table Hierarchy tb_rs_kpi_subsdr_cntry_h Error:' || SQLERRM, 1, 256);
                dbms_output.put_line(vv_param_err_cd||','||vv_param_err_msg_content);
                ROLLBACK;
                RAISE vv_exception;
        END;

        vn_row_cnt := SQL%ROWCOUNT;
        vn_insert_row_cnt := vn_insert_row_cnt + vn_row_cnt;
        dbms_output.put_line('3)Insert success row : ' || vn_insert_row_cnt);

        --JOB 로그 종료처리
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_kpi_subsdr_cntry_h SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB 로그 종료처리
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB 로그 에러 설정
        WHEN vv_exception THEN
            vv_job_log_txt := vv_param_err_msg_content;
            vv_err_desc    := vv_param_err_msg_content;
            -- Error Log
            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);
            -- Error Log
        WHEN OTHERS THEN
            vv_param_err_cd          := SQLCODE;
            vv_param_err_msg_content := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_job_log_txt           := substr('Unknown Error:' || SQLERRM, 1, 256);
            vv_err_desc              := substr('Unknown Error:' || SQLERRM, 1, 256);

            ROLLBACK;

            npt_app.pg_cm_job_log.sp_cm_error_job_log(ov_err_msg_content => vv_param_err_msg_content
                                                     ,ov_err_cd          => vv_param_err_cd
                                                     ,iv_job_log_id      => vn_job_log_id
                                                     ,iv_job_log_txt     => vv_job_log_txt
                                                     ,iv_usr_id          => vv_usr_id
                                                     ,iv_err_desc        => vv_err_desc);
            dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    END SP_RS_ROLLUP_SUBSDR_CNTRY;

END pg_rs_kpi_bep;
