
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
