/***************************************************************************************************/
/* 1.�� �� �� Ʈ : New Plantopia
/* 2.��       �� : RS (ARES)
/* 3.���α׷� ID : pg_rs_kpi_smart
/* 4.��       �� : ARES SMART ����
/*                 1. sp_rs_kpi_prod_mmgn - ��ǰ�� �Ѱ����ڱݾ� ['BEP_SMART_PROD_MMGN']
/* 5.�� �� �� �� :
/*                 [�ʼ�] iv_yyyymm( ���ؿ� )
/*                 [�ʼ�] iv_category( �û걸�� )
/* 6.Source      :
/* 7.��  ��   �� :
/* 8.�� �� �� ġ :
/* 9.�� �� �� �� :
/*
/* Version  �ۼ���  �Ҽ�   ��    ��   ��       ��                                             ��û��
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 �����ۼ�                                                  mysik
/*   1.1     shlee  RS    2016.02.01 ����(�Ѱ����ݾ�/�Ѱ����ڱݾ�/�Ѱ�������/�������ͼ���  mysik
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

        -- Log Variable �߰�
        vv_module_cd VARCHAR2(10) := 'RS';
        vv_pgm_cd    VARCHAR2(30) := 'RSKPIxxxx';
        vv_usr_id    VARCHAR2(10) := 'NPT_RS';


        vv_job_log_txt           VARCHAR2(4000);
        vn_job_log_id            NUMBER;
    BEGIN

        dbms_application_info.set_module(module_name => cv_module_name, action_name => vv_act_name);

        -- JOB LOG ����
        -- Procedure ��� : SELECT * FROM tb_cm_pgm_m a WHERE MODULE_CD = 'RS'
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
            vv_param_err_msg_content := 'FETCH_JOG_LOG_ID_NULL' || ': JOB LOG ID FETCH PROC ���� [' || SQLERRM || ']';
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

        -- 1) Delete : ���ؿ��� ���� TB ������ ����
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
                   AND    acct_cd in ( 'BEP20000000'  -- ������
                                      ,'BEP20070000'  -- ���ݼ����˺�
                                      ,'BEP50000000'  -- �Ѱ�����
                                      ,'BEP50000000%' -- �Ѱ�������
                                      ,'BEP40010400'  -- ��������ݾ�
                                      ,'BEP40010500'  -- �Ǹ�������
                                      ,'BEP60000000'  -- ��������
                                      ,'BEP5000SALE'  -- �Ѱ����ڸ���ݾ�
                                      ,'BEP50000000R' -- �Ѱ����� �� �������
                                      ,'BEP5000MGNL'  -- �Ѱ����ڱݾ�
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

         --JOB �α� ����ó��
        vv_param_err_msg_content := NULL;
        vv_param_err_cd          := NULL;

        vv_job_log_txt := 'tb_rs_excel_upld_data_d SUCCESS (Insert Count : ' || vn_insert_row_cnt || ' Delete Count : ' || vn_delete_row_cnt||')';
        npt_app.pg_cm_job_log.sp_cm_end_job_log(ov_err_msg_content => vv_param_err_msg_content
                                               ,ov_err_cd          => vv_param_err_cd
                                               ,iv_job_log_id      => vn_job_log_id
                                               ,iv_job_log_txt     => vv_job_log_txt
                                               ,iv_usr_id          => vv_usr_id);

        --JOB �α� ����ó��
        dbms_application_info.set_module(module_name => NULL, action_name => NULL);

    EXCEPTION
        --JOB �α� ���� ����
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
