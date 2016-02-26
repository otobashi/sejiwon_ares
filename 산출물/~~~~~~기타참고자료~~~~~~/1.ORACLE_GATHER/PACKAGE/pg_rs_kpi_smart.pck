CREATE OR REPLACE PACKAGE NPT_APP.pg_rs_kpi_smart
/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_bb_ratio_th - BB RATIO(TV사이니지/HOTEL TV)적재
/* 5.입 력 변 수 :
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/


 IS

    cv_module_name CONSTANT VARCHAR2(40) := 'pg_rs_kpi_smart'; --set package name

    -- iv_category : BEP_SMART_BB 만 입력하면 됨.(BEP_SMART_BB / BEP_SMART_BBW5 / BEP_SMART_BBW13 / BEP_SMART_BBW52)
    PROCEDURE sp_rs_kpi_bb_ratio_th(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2);

    PROCEDURE sp_rs_kpi_bb_ratio(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_GNT ( SALE / COI / MGN_PROFIT /SALES_DEDUCTION 등)
    PROCEDURE sp_rs_kpi_gnt_sale(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_ML
    PROCEDURE sp_rs_kpi_most_likely(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_PIPE
    PROCEDURE sp_rs_kpi_pipeline(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_HR
    PROCEDURE sp_rs_kpi_month_hr(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_PROD
    PROCEDURE sp_rs_kpi_prod(iv_yyyymm     IN VARCHAR2
                            ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_SUBSDR
    PROCEDURE sp_rs_kpi_mgn_profit(iv_yyyymm     IN VARCHAR2
                                  ,iv_category   IN VARCHAR2);
                
    -- iv_category : BEP_SMART_PROD_MMGN
    PROCEDURE sp_rs_kpi_prod_mmgn(iv_yyyymm     IN VARCHAR2
                                 ,iv_category   IN VARCHAR2);
                                  
END pg_rs_kpi_smart;
/
CREATE OR REPLACE PACKAGE BODY NPT_APP.pg_rs_kpi_smart IS
/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_bb_ratio_th - BB RATIO(TV사이니지/HOTEL TV)적재 ['BEP_SMART_BB_TH']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_bb_ratio_th(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bb_ratio_th (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1540'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    LIKE iv_category||'%'
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
           )
           -- BB RATIO
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category                                                     AS rs_type_cd                               
                 ,iv_category                                                     AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,a.basis_yyyymm                                                  AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a.basis_yyyymm
                 ,a.basis_yyyyww
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT') AS division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm  = iv_yyyymm
                   AND    a.division_code = 'GNT'
                   AND    SUBSTR(a.product_level3_code,1,2) IN ('CS','HT')
                   AND    SUBSTR(a.product_level4_code,1,2) IN ('CS','HT')
                   AND    a.model_suffix_code = '*'                
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT')
                           ,a.currency_code
                  ) a
                 ,tb_cm_week_m b
           WHERE  a.basis_yyyyww = REPLACE(b.base_yyyyweek,'W','')
           GROUP BY a.basis_yyyymm
                   ,a.basis_yyyyww
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- BB RATIO W5
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W5'                                               AS rs_type_cd                               
                 ,iv_category||'W5'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT') AS division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm  = iv_yyyymm
                   AND    a.division_code = 'GNT'
                   AND    SUBSTR(a.product_level3_code,1,2) IN ('CS','HT')
                   AND    SUBSTR(a.product_level4_code,1,2) IN ('CS','HT')
                   AND    a.model_suffix_code = '*'                
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT')
                           ,a.currency_code
                  ) A
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE  w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 5주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    w2.base_yyyy < '2017') b
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- BB RATIO W13
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W13'                                              AS rs_type_cd                               
                 ,iv_category||'W13'                                              AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT') AS division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm  = iv_yyyymm
                   AND    a.division_code = 'GNT'
                   AND    SUBSTR(a.product_level3_code,1,2) IN ('CS','HT')
                   AND    SUBSTR(a.product_level4_code,1,2) IN ('CS','HT')
                   AND    a.model_suffix_code = '*'                
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT')
                           ,a.currency_code
                  ) A
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 13주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    w2.base_yyyy < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- BB RATIO W52
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W52'                                              AS rs_type_cd                               
                 ,iv_category||'W52'                                              AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT') AS division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm  = iv_yyyymm
                   AND    a.division_code = 'GNT'
                   AND    SUBSTR(a.product_level3_code,1,2) IN ('CS','HT')
                   AND    SUBSTR(a.product_level4_code,1,2) IN ('CS','HT')
                   AND    a.model_suffix_code = '*'                
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,DECODE(SUBSTR(a.product_level3_code,1,2),'CS','GNTCS','HT','GNTHT')
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 52주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    w2.base_yyyy < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
 
                ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_bb_ratio_th;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_bb_ratio - BB RATIO적재 ['BEP_SMART_BB']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_bb_ratio(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_bb_ratio (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1540'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    LIKE iv_category||'%'
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
           )
           -- BB RATIO
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category                                                     AS rs_type_cd                               
                 ,iv_category                                                     AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,a.basis_yyyymm                                                  AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a.basis_yyyymm
                 ,a.basis_yyyyww
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            = iv_yyyymm                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,tb_cm_week_m b
           WHERE  a.basis_yyyyww = REPLACE(b.base_yyyyweek,'W','')
           GROUP BY a.basis_yyyymm
                   ,a.basis_yyyyww
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- BB RATIO W5
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W5'                                               AS rs_type_cd                               
                 ,iv_category||'W5'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm                             
                 ,b.w1_week                            
                 ,a.subsdr_cd                          
                 ,a.division_code                      
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (                                    
                   SELECT a.basis_yyyymm               
                         ,a.basis_yyyyww               
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            = iv_yyyymm                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 5주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    w2.base_yyyy < '2017') b
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- 63961
           -- BB RATIO W13
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W13'                                               AS rs_type_cd                               
                 ,iv_category||'W13'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            = iv_yyyymm                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 13주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    W2.BASE_YYYY < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           
           -- 94996
           -- BB RATIO W52
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category||'W52'                                               AS rs_type_cd                               
                 ,iv_category||'W52'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            = iv_yyyymm                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE  w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 52주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    W2.BASE_YYYY < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
            
                ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_bb_ratio;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_gnt_sale - GNT 제품별적재 ['BEP_SMART_GNT']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_gnt_sale(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_gnt_sale (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1600'
            AND     rs_module_cd  = 'ARES'
            AND     rs_clsf_id    = 'BEP_SMART'
            AND     rs_type_cd    = iv_category
            AND     base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

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
                    ,iv_category                                                      --rs_type_cd
                    ,iv_category                                                      --rs_type_name
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
              WHERE (a11.acctg_yyyymm          = iv_yyyymm
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

                ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_gnt_sale;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_most_likely - Most Likely ['BEP_SMART_ML']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_most_likely(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_most_likely (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1520'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
                 ,attribute9_value 
                 ,attribute10_value
                 ,attribute11_value
                 ,attribute12_value
                 ,attribute13_value
                 ,attribute14_value
                 ,attribute15_value
           )
           SELECT '1520'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category                                                     AS rs_type_cd                               
                 ,iv_category                                                     AS rs_type_name                             
                 ,a.div_cd                                                        AS div_cd                                   
                 ,b.pln_yyyymm                                                    AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a.div_cd AS cmpny_cd
                 ,a.div_cd AS rs_div_cd
                 ,DECODE(a.ml_acct_cat_cd, 'NSALES', 'SALE', a.ml_acct_cat_cd) AS kpi_cd
                 ,b.pln_yyyymm AS base_yyyymm
                 ,b.week_no AS base_mmweek
                 ,a.pln_yyyymm AS pln_yyyymm
                 ,CASE WHEN a.pln_yyyymm = b.pln_yyyymm THEN 'PR0'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
                  END AS scenario_type_cd
                 ,a.subsdr_cd
                 ,c.subsdr_shrt_name
                 ,SUM(a.krw_amt) AS currm_krw_amt
                 ,SUM(a.usd_amt) AS currm_usd_amt
                 ,TRUNC(a.creation_date)
                 ,'ARES'
                 ,TRUNC(a.last_upd_date)
                 ,'ARES'
           FROM   tb_rfe_ml_upld_rslt_s a
                 ,tb_rfe_ml_week_m      b
                 ,(
                   SELECT DISTINCT s.subsdr_shrt_name, s.subsdr_cd, s.subsdr_kor_name
                   FROM  tb_cm_subsdr_period_h s
                   WHERE s.mgt_type_cd  = 'CM'
                   AND   s.acctg_yyyymm = '*'
                   AND   s.acctg_week   = '*'
                   AND   s.temp_flag    = 'N' ) C
           WHERE  b.pln_yyyymm   = iv_yyyymm -- 요부분변경
           AND    a.data_type_cd = 'SUBSDR'
           AND    a.pln_yyyyweek = b.pln_yyyyweek
           AND    b.pln_yyyymm   <= a.pln_yyyymm
           AND    a.pln_yyyymm   < TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 4), 'YYYYMM')
           AND    a.krw_amt      <> 0
           AND    c.subsdr_cd    = a.subsdr_cd(+)
           GROUP BY a.div_cd
                   ,a.ml_acct_cat_cd
                   ,b.pln_yyyymm 
                   ,b.week_no
                   ,a.pln_yyyymm 
                   ,CASE WHEN a.pln_yyyymm = b.pln_yyyymm THEN 'PR0'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
                    END 
                   ,a.subsdr_cd
                   ,c.subsdr_shrt_name
                   ,TRUNC(a.creation_date)
                   ,TRUNC(a.last_upd_date)
          ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_most_likely;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_pipeline - PipeLine ['BEP_SMART_PIPE']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_pipeline(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_pipeline (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1530'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
                 ,attribute9_value 
                 ,attribute10_value
                 ,attribute11_value
                 ,attribute12_value
                 ,attribute13_value
           )
           SELECT '1530'               AS prcs_seq                                 
                 ,'ARES'               AS rs_module_cd                             
                 ,'BEP_SMART'          AS rs_clsf_id                               
                 ,iv_category          AS rs_type_cd                               
                 ,iv_category          AS rs_type_name                             
                 ,a11.div_cd           AS div_cd                                   
                 ,a11.base_yyyymm      AS base_yyyymmdd                            
                 ,NULL                 AS cd_desc                                  
                 ,a14.sort_order       AS sort_seq                                 
                 ,'Y'                  AS use_flag                                 
                 ,a11.subsdr_cd        AS subsdr_cd
                 ,a14.subsdr_shrt_name AS new_subsdr_shrt_name
                 ,a14.sort_order       AS sort1_order
                 ,a11.mgt_org_cd       AS mgt_subsdr_cd
                 ,a13.mgt_subsdr_name  AS mgt_subsdr_name
                 ,a11.div_cd           AS div_cd
                 ,a12.div_kor_name     AS div_kor_name
                 ,a12.div_shrt_name    AS div_shrt_name
                 ,a11.base_yyyymm  base_yyyymm
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.chg_amt)) AS sales_usd_amount   --SALES AMT(매출-프로젝트)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.chg_amt)) AS sales_krw_amount
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.bal_amt)) AS backlog_usd_amount --BACKLOG AMOUNT(수주잔고)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.bal_amt)) AS backlog_krw_amount
           FROM   npt_app.nv_dww_b2b_bal_h  a11
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a12
                  ON (a11.div_cd = a12.div_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mgt_m  a13
                  ON (a11.mgt_org_cd = a13.mgt_subsdr_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a14
                  ON (a11.subsdr_cd = a14.subsdr_cd)
           WHERE (a11.base_yyyymm = iv_yyyymm -- 요부분변경
           AND    a11.pjt_stg_cd IN ('A'))
           GROUP BY a11.subsdr_cd
                   ,a14.subsdr_shrt_name
                   ,a14.sort_order
                   ,a11.mgt_org_cd
                   ,a13.mgt_subsdr_name
                   ,a11.div_cd
                   ,a12.div_kor_name
                   ,a12.div_shrt_name
                   ,a11.base_yyyymm
           UNION ALL
           SELECT '1530'                                                         AS prcs_seq                                 
                 ,'ARES'                                                         AS rs_module_cd                             
                 ,'BEP_SMART'                                                    AS rs_clsf_id                               
                 ,iv_category                                                    AS rs_type_cd                               
                 ,iv_category                                                    AS rs_type_name                             
                 ,DECODE(SUBSTR(A11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS div_cd                                   
                 ,a11.base_yyyymm                                                AS base_yyyymmdd                            
                 ,NULL                                                           AS cd_desc                                  
                 ,a14.sort_order                                                 AS sort_seq                                 
                 ,'Y'                                                            AS use_flag                                 
                 ,a11.subsdr_cd                                                  AS subsdr_cd
                 ,a14.subsdr_shrt_name                                           AS new_subsdr_shrt_name
                 ,a14.sort_order                                                 AS sort1_order
                 ,a11.mgt_org_cd                                                 AS mgt_subsdr_cd
                 ,a13.mgt_subsdr_name                                            AS mgt_subsdr_name
                 ,DECODE(SUBSTR(a11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS div_cd
                 ,a12.div_kor_name                                               AS div_kor_name
                 ,a12.div_shrt_name                                              AS div_shrt_name
                 ,a11.base_yyyymm                                                AS base_yyyymm
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.chg_amt))            AS sales_usd_amount        --SALES AMT(매출-프로젝트)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.chg_amt))            AS sales_krw_amount        
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.bal_amt))            AS backlog_usd_amount      --BACKLOG AMOUNT(수주잔고)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.bal_amt))            AS backlog_krw_amount
           FROM   npt_app.nv_dww_b2b_bal_h  a11
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a12
                  ON (a11.div_cd = a12.div_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mgt_m  a13
                  ON (a11.mgt_org_cd = a13.mgt_subsdr_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a14
                  ON (a11.subsdr_cd = a14.subsdr_cd)
           WHERE (a11.base_yyyymm = iv_yyyymm -- 요부분변경
           AND    a11.div_cd = 'GNT'
           AND    SUBSTR(a11.prod_lvl3_cd,1,2) IN ('CS','HT')
           AND    SUBSTR(a11.prod_lvl4_cd,1,2) IN ('CS','HT')
           AND    a11.mdl_sffx_cd = '*'
           AND    a11.pjt_stg_cd IN ('A'))
           GROUP BY a11.subsdr_cd
                   ,a14.subsdr_shrt_name
                   ,a14.sort_order
                   ,a11.mgt_org_cd
                   ,a13.mgt_subsdr_name
                   ,DECODE(SUBSTR(a11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT')
                   ,a12.div_kor_name
                   ,a12.div_shrt_name
                   ,a11.base_yyyymm
          ;


        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_pipeline;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_month_hr - Mon HR ['BEP_SMART_HR']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_month_hr(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_month_hr (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1620'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
                 ,attribute9_value 
                 ,attribute10_value 
                 ,attribute11_value 
           )
           SELECT '1620'                AS prcs_seq                                 
                 ,'ARES'                AS rs_module_cd                             
                 ,'BEP_SMART'           AS rs_clsf_id                               
                 ,iv_category           AS rs_type_cd                               
                 ,iv_category           AS rs_type_name                             
                 ,a.div_cd              AS div_cd                                   
                 ,a.base_yyyymm         AS base_yyyymmdd                            
                 ,NULL                  AS cd_desc                                  
                 ,NULL                  AS sort_seq                                 
                 ,'Y'                   AS use_flag                                 
                 ,a.base_yyyymm
                 ,a.scenario_type_cd
                 ,a.kpi_type_cd
                 ,a.div_cd
                 ,a.subsdr_cd
                 ,a.acct_cd
                 ,b.acct_nm
                 ,SUM(a.currm_krw_amt)  AS currm_krw_amt
                 ,SUM(a.currm_usd_amt)  AS currm_usd_amt
                 ,SUM(a.accum_krw_amt)  AS accum_krw_amt
                 ,SUM(a.accum_usd_amt)  AS accum_usd_amt
           FROM   tb_dsm_kpi_div_s a
                 ,(
                   SELECT acct_cd, acct_desc AS acct_nm
                   FROM   tb_dsd_acct_m
                   WHERE  acct_gr_cd = 'HR'
                   AND    acct_desc IN ('(HR) 원당매출액','(HR) 원당매출액_사내도급제외','(HR) 인당매출액','(HR) 인당매출액_사내도급제외','(HR) 인원수','(HR) 인원수_사내도급제외'
                                     ,'(HR) 인원수_FSE','(HR) 인원수_ISE ','(HR) 인원수_임시직 ','(HR) 인원수_사내도급 ','(HR) 인원수_ISE_영업/마케팅','(HR) 인원수_공통인원'
                                     ,'(HR) 인건비(HR생산성 기준)','(HR) 인건비_사내도급제외')
                 ) b            
           WHERE  a.base_yyyymm = iv_yyyymm
           AND    a.acct_cd = b.acct_cd
           AND    a.div_cd IN ('GLT','GTT','MST','CNT','DFT','DGT','DMT','CMS')
           AND    a.currency_cd = '*'
           AND    a.scenario_type_cd = 'AC0'
           AND    a.kpi_type_cd in ('HR', 'HR_CNT')
           GROUP BY a.base_yyyymm
                   ,a.scenario_type_cd
                   ,a.kpi_type_cd
                   ,a.div_cd
                   ,a.subsdr_cd
                   ,a.acct_cd
                   ,b.acct_nm
           ;
           

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_month_hr;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_prod - 제품별 ['BEP_SMART_PROD']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_prod(iv_yyyymm     IN VARCHAR2
                            ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_prod (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1510'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

           INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
           (
                  prcs_seq         
                 ,rs_module_cd     
                 ,rs_clsf_id       
                 ,rs_type_cd       
                 ,rs_type_name     
                 ,div_cd           
                 ,base_yyyymmdd    
                 ,cd_desc          
                 ,sort_seq         
                 ,use_flag         
                 ,attribute1_value 
                 ,attribute2_value 
                 ,attribute3_value 
                 ,attribute4_value 
                 ,attribute5_value 
                 ,attribute6_value 
                 ,attribute7_value 
                 ,attribute8_value 
                 ,attribute9_value 
                 ,attribute10_value
                 ,attribute11_value
                 ,attribute12_value
                 ,attribute13_value
                 ,attribute14_value
                 ,attribute15_value
                 ,attribute16_value
                 ,attribute17_value
                 ,attribute18_value
                 ,attribute19_value
                 ,attribute20_value
                 ,attribute21_value
                 ,attribute22_value
                 ,attribute23_value
                 ,attribute24_value
                 ,attribute25_value
                 ,attribute26_value
                 ,attribute27_value
                 ,attribute28_value
                 ,attribute29_value
                 ,attribute30_value
                 ,attribute31_value
                 ,attribute32_value
                 ,attribute33_value
                 ,attribute34_value
                 ,attribute35_value
                 ,attribute36_value
                 ,attribute37_value
                 ,attribute38_value
                 ,attribute39_value
                 ,attribute40_value
           )
           /*******************************************************/
           /* 주요제품군 Master                                   */
           /*******************************************************/
           WITH v_prod_mst (
              mgt_type_cd
             ,prod_gr_cd
             ,subsdr_cd
             ,prod_cd
             ,prod_lvl_cd
             ,prod_eng_name
             ,prod_kor_name
             ,up_prod_cd
             ,last_lvl_flag
             ,div_cd
             ,enable_flag
             ,sales_qty_incl_flag
             ,sum_excl_flag
             ,sort_order
             ,prod_cd_desc
           )
           AS
           (
             SELECT mgt_type_cd
                   ,prod_gr_cd
                   ,subsdr_cd
                   ,prod_cd
                   ,prod_lvl_cd
                   ,prod_eng_name
                   ,prod_kor_name
                   ,up_prod_cd
                   ,last_lvl_flag
                   ,div_cd
                   ,enable_flag
                   ,sales_qty_incl_flag
                   ,sum_excl_flag
                   ,sort_order
                   ,prod_cd_desc
             FROM   tb_cm_prod_period_h t
             WHERE  t.mgt_type_cd  = 'CM'
             AND    t.acctg_yyyymm = '*'
             AND    t.acctg_week   = '*'
             AND    t.temp_flag    = 'N'
             AND    prod_cd        NOT IN ( '847141' ,'ACAHHAW')-- IMSI
             UNION ALL
             SELECT 'CM'                                                 AS mgt_type_cd
                   ,'PRODUCT_LEVEL'                                      AS prod_gr_cd
                   ,'*'                                                  AS subsdr_cd
                   ,'*'                                                  AS prod_cd
                   ,b.prod_lvl_cd                                        AS prod_lvl_cd
                   ,'*'          ||'(LVL'||b.prod_lvl_cd||')'            AS prod_eng_name
                   ,'*'          ||'(LVL'||b.prod_lvl_cd||')'            AS prod_kor_name
                   ,'*'                                                  AS up_prod_cd
                   ,CASE WHEN b.prod_lvl_cd  = '4' THEN 'Y' ELSE 'N' END AS last_lvl_flag
                   ,'*'                                                  AS div_cd
                   ,'Y'                                                  AS enable_flag
                   ,'N'                                                  AS sales_qty_incl_flag
                   ,NULL                                                 AS sum_excl_flag
                   ,NULL                                                 AS sort_order
                   ,NULL                                                 AS prod_cd_desc
             FROM DUAL
                ,(SELECT '1' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '2' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '3' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '4' AS prod_lvl_cd FROM DUAL  ) b
           ),
           /*******************************************************/
           /* 주요제품군 Levle별 (1,2,3,4)                        */
           /*******************************************************/
           v_prod_m (
               mgt_type_cd
              ,prod_gr_cd
              ,subsdr_cd
              ,prod_cd
              ,prod_lvl_cd
              ,prod_eng_name
              ,prod_kor_name
              ,up_prod_cd
              ,last_lvl_flag
              ,div_cd
              ,enable_flag
              ,sales_qty_incl_flag
              ,sum_excl_flag
              ,sort_order
           )
           AS
           (
              SELECT mgt_type_cd
                    ,prod_gr_cd
                    ,subsdr_cd
                    ,prod_cd
                    ,prod_lvl_cd
                    ,prod_eng_name
                    ,prod_kor_name
                    ,up_prod_cd
                    ,last_lvl_flag
                    ,div_cd
                    ,enable_flag
                    ,sales_qty_incl_flag
                    ,sum_excl_flag
                    ,sort_order
              FROM   v_prod_mst
              WHERE  prod_gr_cd = 'RPT_PROD_GR'
           )
           /*******************************************************/
           /* 주요제품군 조회                    */
           /*******************************************************/
           SELECT /*+ PARALLEL(8)  USE_HASH(A11 A12 A13 A14 A15 A16 A17 A18 A19 A110 A111 A112 A113) */
                  '1510'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,iv_category                                                     AS rs_type_cd                               
                 ,iv_category                                                     AS rs_type_name                             
                 ,a11.div_cd                                                      AS div_cd                                   
                 ,a11.acctg_yyyymm                                                AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a11.acctg_yyyymm                                                AS base_yyyymm                              
                 ,a11.div_cd                                                      AS div_cd                                   
                 ,a17.scrn_dspl_seq                                               AS div_kor_name                             
                 ,a17.div_shrt_name                                               AS div_shrt_name                            
                 ,a11.subsdr_rnr_cd                                               AS subsdr_cd                                
                 ,a112.mgt_org_shrt_name                                          AS mgt_org_shrt_name                        
                 ,a112.sort_order                                                 AS sort_order                               
                 ,a11.subsdr_cd                                                   AS subsdr_rnr_cd                            
                 ,a111.subsdr_shrt_name                                           AS subsdr_name                              
                 ,a111.sort_order                                                 AS sort1_order                              
                 ,a11.production_subsdr_cd                                        AS subsdr_cd1                               
                 ,a19.subsdr_shrt_name                                            AS subsdr_name0                             
                 ,a19.sort_order                                                  AS sort_order0                              
                 ,a11.scenario_type_cd                                            AS scenario_type_cd                         
                 ,a110.scenario_type_name                                         AS scenario_type_name                       
                 ,a110.sort_order                                                 AS sort_order1                              
                 ,a16.up_prod_cd                                                  AS prod_cd                                  
                 ,a113.prod_kor_name                                              AS prod_kor_name                            
                 ,a113.prod_eng_name                                              AS prod_eng_name                            
                 ,a113.sort_order                                                 AS sort_order2                              
                 ,a15.up_prod_cd                                                  AS prod_cd0                                 
                 ,a16.prod_kor_name                                               AS prod_kor_name0                           
                 ,a16.prod_eng_name                                               AS prod_eng_name0                           
                 ,a16.sort_order                                                  AS sort_order3                              
                 ,a14.up_prod_cd                                                  AS prod_cd1                                 
                 ,a15.prod_kor_name                                               AS prod_kor_name1                           
                 ,a15.prod_eng_name                                               AS prod_eng_name1                           
                 ,a15.sort_order                                                  AS sort_order4                              
                 ,a12.usr_prod1_last_cd                                           AS prod_cd2                                 
                 ,a14.prod_kor_name                                               AS prod_kor_name2                           
                 ,a14.prod_eng_name                                               AS prod_eng_name2                           
                 ,a14.sort_order                                                  AS sort_order5                              
                 ,SUM(a11.sales_qty)                                              AS sales_qty                                
                 ,SUM(a11.nsales_amt)                                             AS nsales_amt                               
                 ,(SUM(a11.fix_cogs_amt) + SUM(a11.var_cogs_amt))                 AS cogs_amt                                 
                 ,SUM(a11.gross_sales_amt)                                        AS gross_sales_amt                          
                 ,SUM(a11.mgnl_prf_amt)                                           AS mgnl_prf_amt                             
                 ,SUM(a11.oi_amt)                                                 AS oi_amt                                   
                 ,SUM(a11.sales_deduct_amt)                                       AS sales_deduct_amt                         
                 ,(SUM(a11.fix_sell_adm_exp_amt) + SUM(a11.var_sell_adm_exp_amt)) AS sell_adm_exp_amt                           
           FROM   npt_app.nv_dww_consld_bep_summ_s  a11                                                                       
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mdl_period_h  a12                                                    
                  ON   a11.mdl_sffx_cd = a12.mdl_sffx_cd                                                                      
                  AND  a11.subsdr_cd   = a12.subsdr_cd                                                                        
                  LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a13                                                 
                  ON   a11.acctg_yyyymm = a13.acctg_yyyymm                                                                    
                  AND  a11.mdl_sffx_cd  = a13.mdl_sffx_cd                                                                     
                  AND  a11.subsdr_cd    = a13.subsdr_cd                                                                       
                  LEFT OUTER JOIN  v_prod_m  a14                                                                              
                  ON   a12.usr_prod1_last_cd = a14.prod_cd                                                                    
                  AND  a14.prod_lvl_cd       = '4'                                                                            
                  LEFT OUTER JOIN  v_prod_m  a15                                                                              
                  ON   a14.up_prod_cd   = a15.prod_cd                                                                         
                  AND  a15.prod_lvl_cd  = '3'                                                                                 
                  LEFT OUTER JOIN  v_prod_m  a16                                                                              
                  ON   a15.up_prod_cd  = a16.prod_cd                                                                          
                  AND  a16.prod_lvl_cd = '2'                                                                                  
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a17                                                             
                  ON   a11.div_cd = a17.div_cd                                                                                
                  LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18                                                              
                  ON   a13.grd_cd = a18.attribute_cd                                                                          
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a19                                                               
                  ON   a11.production_subsdr_cd = a19.subsdr_cd                                                               
                  LEFT OUTER JOIN  npt_app.nv_dwd_scenario_type_m  a110                                                       
                  ON   a11.scenario_type_cd = a110.scenario_type_cd                                                           
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a111                                                              
                  ON   a11.subsdr_cd = a111.subsdr_cd                                                                         
                  LEFT OUTER JOIN  npt_app.nv_dwd_mgt_org_rnr_m  a112                                                         
                  ON   a11.subsdr_rnr_cd = a112.mgt_org_cd                                                                    
                  LEFT OUTER JOIN  v_prod_m  a113                                                                             
                  ON   a16.up_prod_cd   = a113.prod_cd                                                                        
                  AND  a113.prod_lvl_cd = '1'                                                                                 
           WHERE  a11.acctg_yyyymm = iv_yyyymm -- &IV_YYYYMM    요부분변경                                                               
           AND    a11.scenario_type_cd      = 'AC0'                                                                           
           AND    a11.consld_sales_mdl_flag = 'Y'                                                                             
           AND    a11.currm_accum_type_cd   = 'CURRM'                                                                         
           AND    a11.vrnc_alc_incl_excl_cd = 'INCL'                                                                          
           AND    a11.currency_cd           = 'USD'                                                                           
           AND    a11.mdl_sffx_cd NOT LIKE 'VM-%.CPS'                                                                         
           GROUP BY  a11.acctg_yyyymm                                                                                         
                    ,a11.div_cd                                                                                               
                    ,a17.scrn_dspl_seq                                                                                        
                    ,a17.div_shrt_name                                                                                        
                    ,a11.subsdr_rnr_cd                                                                                        
                    ,a112.mgt_org_shrt_name                                                                                   
                    ,a112.sort_order                                                                                          
                    ,a11.subsdr_cd                                                                                            
                    ,a111.subsdr_shrt_name                                                                                    
                    ,a111.sort_order                                                                                          
                    ,a11.production_subsdr_cd                                                                                 
                    ,a19.subsdr_shrt_name                                                                                     
                    ,a19.sort_order                                                                                           
                    ,a11.scenario_type_cd                                                                                     
                    ,a110.scenario_type_name                                                                                  
                    ,a110.sort_order                                                                                          
                    ,a16.up_prod_cd                                                                                           
                    ,a113.prod_kor_name                                                                                       
                    ,a113.prod_eng_name                                                                                       
                    ,a113.sort_order                                                                                          
                    ,a15.up_prod_cd                                                                                           
                    ,a16.prod_kor_name                                                                                        
                    ,a16.prod_eng_name                                                                                        
                    ,a16.sort_order                                                                                           
                    ,a14.up_prod_cd                                                                                           
                    ,a15.prod_kor_name                                                                                        
                    ,a15.prod_eng_name                                                                                        
                    ,a15.sort_order                                                                                           
                    ,a12.usr_prod1_last_cd                                                                                    
                    ,a14.prod_kor_name                                                                                        
                    ,a14.prod_eng_name                                                                                        
                    ,a14.sort_order                                                                                           
           ORDER BY  a17.scrn_dspl_seq                                                                                        
                    ,a113.sort_order                                                                                          
                    ,a16.sort_order                                                                                           
                    ,a15.sort_order                                                                                           
                    ,a14.sort_order                                                                                           
           ;                                                                                                                  
                                                                                                                              

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_prod;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_mgn_profit - 제품별 ['BEP_SMART_SUBSDR']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_mgn_profit(iv_yyyymm     IN VARCHAR2
                                  ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_mgn_profit (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1550'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN

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
        WHERE  (a11.acctg_yyyymm = iv_yyyymm
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
        WHERE  (a11.acctg_yyyymm = iv_yyyymm
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
        WHERE  (a11.acctg_yyyymm = iv_yyyymm
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
        WHERE  (a11.acctg_yyyymm = iv_yyyymm
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
              ,iv_category                                                     AS rs_type_cd
              ,iv_category                                                     AS rs_type_name
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
              ,iv_category                                                     AS rs_type_cd
              ,iv_category                                                     AS rs_type_name
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
                                                                                                                              

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_mgn_profit;

/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_prod_mmgn - 제품별 한계적자금액 ['BEP_SMART_PROD_MMGN']
/* 5.입 력 변 수 :
/*                 [필수] iv_yyyymm( 기준월 )
/*                 [필수] iv_category( 시산구분 )
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/
    PROCEDURE sp_rs_kpi_prod_mmgn(iv_yyyymm     IN VARCHAR2
                                 ,iv_category   IN VARCHAR2)
    IS
        vv_act_name  VARCHAR2(1000) := 'sp_rs_kpi_prod_mmgn (' || iv_yyyymm || ')';
        vn_row_cnt   NUMBER;

        vv_exception EXCEPTION;
        vv_param_err_msg_content VARCHAR2(2000);
        vv_param_err_cd          VARCHAR2(30);
        vv_err_desc              VARCHAR2(4000);

        -- Insert / Delete Count
        vn_insert_row_cnt   NUMBER;
        vn_delete_row_cnt   NUMBER;

        -- Log Variable 추가
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
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

        -- 1) Delete : 기준월의 기존 TB 데이터 삭제
        BEGIN

            DELETE FROM npt_rs_mgr.tb_rs_excel_upld_data_d
            WHERE  prcs_seq      = '1550'
            AND    rs_module_cd  = 'ARES'
            AND    rs_clsf_id    = 'BEP_SMART'
            AND    rs_type_cd    = iv_category
            AND    base_yyyymmdd = iv_yyyymm
            ;

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('1) Delete Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
                ROLLBACK;
                RAISE vv_exception;

        END;

        vn_delete_row_cnt := SQL%ROWCOUNT;
        dbms_output.put_line('success row1 : ' || vn_delete_row_cnt);


        -- 2) Insert
        BEGIN
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
               ,iv_category                                                    AS rs_type_cd
               ,iv_category                                                    AS rs_type_name
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
         AND   a11.acctg_yyyymm = iv_yyyymm
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
                                                                                                                              

        EXCEPTION
            WHEN OTHERS THEN
                vv_param_err_cd          := SQLCODE;
                vv_param_err_msg_content := substr('2) Insert Table tb_rs_excel_upld_data_d Error:' || SQLERRM, 1, 256);
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

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
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


    END sp_rs_kpi_prod_mmgn;

END pg_rs_kpi_smart;
/
