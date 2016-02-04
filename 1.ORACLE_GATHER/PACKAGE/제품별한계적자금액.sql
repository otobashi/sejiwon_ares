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
/*   1.1     shlee  RS    2016.02.01 수정(한계매출금액/한계적자금액/한계매출비중/영업이익수정  mysik
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
            WHERE  prcs_seq      = '1700'
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
           SELECT '1700'                AS prcs_seq                                 
                 ,'ARES'                AS rs_module_cd                             
                 ,'BEP_SMART'           AS rs_clsf_id                               
                 ,iv_category           AS rs_type_cd                               
                 ,'BEP_SMART_PROD_MMGN' AS rs_type_name                             
                 ,a.prod_cd             AS prod_cd                                   
                 ,a.base_yyyymm         AS base_yyyymmdd                            
                 ,NULL                  AS cd_desc                                  
                 ,NULL                  AS sort_seq                                 
                 ,'Y'                   AS use_flag                                 
                 ,a.base_yyyymm
                 ,a.scenario_type_cd
                 ,a.kpi_type_cd
                 ,a.prod_cd
                 ,a.subsdr_cd
                 ,a.acct_cd
                 ,b.acct_nm
                 ,SUM(a.currm_krw_amt)  AS currm_krw_amt
                 ,SUM(a.currm_usd_amt)  AS currm_usd_amt
                 ,SUM(a.accum_krw_amt)  AS accum_krw_amt
                 ,SUM(a.accum_usd_amt)  AS accum_usd_amt
           FROM   tb_dsm_kpi_prod_s a
                 ,(
                   SELECT acct_cd, acct_desc AS acct_nm
                   FROM   tb_dsd_acct_m
                   WHERE  ACCT_GR_CD = 'BEP'
                   AND    acct_cd in ( 'BEP20000000'  -- 순매출
                                      ,'BEP20070000'  -- 가격성판촉비
                                      ,'BEP50000000'  -- 한계이익
                                      ,'BEP50000000%' -- 한계이익율
                                      ,'BEP40010400'  -- 광고선전비금액
                                      ,'BEP40010500'  -- 판매촉진비
                                      ,'BEP60000000'  -- 영업이익
                                      ,'BEP5000SALE'  -- 한계적자매출금액
                                      ,'BEP50000000R' -- 한계적자 모델 매출비중
                                      ,'BEP5000MGNL'  -- 한계적자금액
                                     )
                 ) b            
           WHERE  a.base_yyyymm = iv_yyyymm
           AND    a.acct_cd = b.acct_cd
           AND    a.scenario_type_cd = 'AC0'
           AND    a.kpi_type_cd = 'BEP'
           GROUP BY a.base_yyyymm
                   ,a.scenario_type_cd
                   ,a.kpi_type_cd
                   ,a.prod_cd
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


    END sp_rs_kpi_prod_mmgn;
