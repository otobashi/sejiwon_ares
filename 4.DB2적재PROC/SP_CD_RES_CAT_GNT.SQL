CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_CAT_GNT (
    IN p_yyyymm VARCHAR(6),
    IN p_category VARCHAR(30) )
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  /********************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                     */
  /* 2.실 행 방 법 : CALL SP_CD_RES_CAT_GNT('201301','BEP_SMART_GNT')                         */
  /* 3.프로그램 ID : SP_CD_RES_CAT_GNT                                                        */
  /* 4.설       명 : IPTDW_RES_EXCEL_UPLOAD_DATA에 적재된 ORACLE DATA를                       */
  /*                 IPTDW_RES_KPI_SUBSDR_CNTRY에 DB2 테이블에 적재한다.                      */
  /* 5.입 력 변 수 :                                                                          */
  /*                 IN p_yyyymm( 기준월 )                                                    */
  /*                 IN p_category( 시산구분 )                                                */
  /* 6.파 일 위 치 :                                                                          */
  /* 7.변 경 이 력 :                                                                          */
  /*                                                                                          */
  /*  version  작성자  일      자  내                 용                                      */
  /*  -------  ------  ----------  --------------------------------------------------------   */
  /*  1.0      shlee   2016.01.28  최초작성                                                   */
  /********************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_CAT_GNT';
    DECLARE v_load_start_timestamp       TIMESTAMP     DEFAULT NULL;
    DECLARE v_serial_no                  VARCHAR(30)   DEFAULT NULL;
    DECLARE v_load_progress_status_code  VARCHAR(10)   DEFAULT NULL;
    DECLARE v_target_insert_count        INTEGER       DEFAULT 0;
    DECLARE v_target_update_count        INTEGER       DEFAULT 0;
    DECLARE v_target_delete_count        INTEGER       DEFAULT 0;
    DECLARE v_source_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_target_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_job_notes                  VARCHAR(300)  DEFAULT NULL;
    DECLARE SQLSTATE                     CHAR(5)       DEFAULT '';
    DECLARE v_basis_yyyymmdd             VARCHAR(8)    DEFAULT NULL;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS EXCEPTION 1 v_job_notes = MESSAGE_TEXT;
        SET v_load_progress_status_code = SQLSTATE;

        ROLLBACK;

        CALL sp_cd_etl_job_logs( v_etl_job_no,
                                 v_basis_yyyymmdd,
                                 v_load_start_timestamp,
                                 v_serial_no,
                                 v_load_progress_status_code,
                                 v_target_insert_count,
                                 v_target_update_count,
                                 v_target_delete_count,
                                 v_source_table_name,
                                 v_target_table_name,
                                 v_job_notes
                               );
        COMMIT;

        SIGNAL SQLSTATE '70001' SET MESSAGE_TEXT = v_job_notes;
    END;

    /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_EXCEL_UPLOAD_DATA';
    SET v_target_table_name          = 'IPTDW_RES_KPI_SUBSDR_CNTRY';
    SET v_basis_yyyymmdd             = p_yyyymm;

    /*-----------------------------------
       기준월의 기존 데이터 삭제
    -----------------------------------*/
    DELETE
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
    WHERE  BASE_YYYYMM     = p_yyyymm
    AND    CAT_CD          = p_category
    AND    MANUAL_ADJ_FLAG = 'N';

    GET DIAGNOSTICS  v_target_delete_count = ROW_COUNT;

    INSERT INTO IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
    (
        BASE_YYYYMM
       ,SCENARIO_TYPE_CD
       ,DIV_CD
       ,SUBSDR_CD
       ,AU_CD
       ,MANUAL_ADJ_FLAG
       ,KPI_CD
       ,CAT_CD
       ,SUB_CAT_CD
       ,ZONE_RNR_CD
       ,SUBSDR_RNR_CD
       ,CNTRY_CD
       ,APPLY_YYYYMM
       ,CURRM_KRW_AMT
       ,CURRM_USD_AMT
       ,ATTRIBUTE1_VALUE
       ,ATTRIBUTE2_VALUE
       ,ATTRIBUTE3_VALUE
       ,CREATION_DATE
       ,CREATION_USR_ID
       ,LAST_UPD_DATE
       ,LAST_UPD_USR_ID
    )
    SELECT ATTRIBUTE10 AS    BASE_YYYYMM
          ,ATTRIBUTE1 AS SCENARIO_TYPE_CD
          ,ATTRIBUTE2 AS DIV_CD
          ,ATTRIBUTE3 AS SUBSDR_CD
          ,'*' AS AU_CD
          ,'N' AS MANUAL_ADJ_FLAG
          ,'SALE' AS KPI_CD
          ,'BEP_SMART_GNT' AS CAT_CD
          ,'*' AS SUB_CAT_CD
          ,ATTRIBUTE5 AS ZONE_RNR_CD
          ,'*' AS SUBSDR_RNR_CD
          ,ATTRIBUTE8 AS CNTRY_CD
          ,ATTRIBUTE11 AS APPLY_YYYYMM
          ,SUM(TO_NUMBER(ATTRIBUTE15)) AS CURRM_KRW_AMT
          ,SUM(TO_NUMBER(ATTRIBUTE14)) AS CURRM_USD_AMT
          ,ATTRIBUTE4 AS ATTRIBUTE1_VALUE
          ,DESCRIPTION AS ATTRIBUTE2_VALUE
          ,ATTRIBUTE9 AS ATTRIBUTE3_VALUE
          ,CURRENT TIMESTAMP   
          ,'ares'              
          ,CURRENT TIMESTAMP   
          ,'ares'                    
    FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
    WHERE  SEQ = '1600'
    AND    CODE_ID = p_category
    AND    YYYYMMDD = p_yyyymm
    GROUP BY ATTRIBUTE10
            ,ATTRIBUTE1 
            ,ATTRIBUTE2 
            ,ATTRIBUTE3 
            ,ATTRIBUTE5 
            ,ATTRIBUTE8 
            ,ATTRIBUTE11
            ,ATTRIBUTE4 
            ,DESCRIPTION
            ,ATTRIBUTE9 
    UNION ALL
    SELECT ATTRIBUTE10 AS    BASE_YYYYMM
          ,ATTRIBUTE1 AS SCENARIO_TYPE_CD
          ,ATTRIBUTE2 AS DIV_CD
          ,ATTRIBUTE3 AS SUBSDR_CD
          ,'*' AS AU_CD
          ,'N' AS MANUAL_ADJ_FLAG
          ,'COI' AS KPI_CD
          ,'BEP_SMART_GNT' AS CAT_CD
          ,'*' AS SUB_CAT_CD
          ,ATTRIBUTE5 AS ZONE_RNR_CD
          ,'*' AS SUBSDR_RNR_CD
          ,ATTRIBUTE8 AS CNTRY_CD
          ,ATTRIBUTE11 AS APPLY_YYYYMM
          ,SUM(TO_NUMBER(ATTRIBUTE23)) AS CURRM_KRW_AMT
          ,SUM(TO_NUMBER(ATTRIBUTE22)) AS CURRM_USD_AMT
          ,ATTRIBUTE4 AS ATTRIBUTE1_VALUE
          ,DESCRIPTION AS ATTRIBUTE2_VALUE
          ,ATTRIBUTE9 AS ATTRIBUTE3_VALUE
          ,CURRENT TIMESTAMP   
          ,'ares'              
          ,CURRENT TIMESTAMP   
          ,'ares'                    
    FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
    WHERE  SEQ = '1600'
    AND    CODE_ID = p_category
    AND    YYYYMMDD = p_yyyymm
    GROUP BY ATTRIBUTE10
            ,ATTRIBUTE1 
            ,ATTRIBUTE2 
            ,ATTRIBUTE3 
            ,ATTRIBUTE5 
            ,ATTRIBUTE8 
            ,ATTRIBUTE11
            ,ATTRIBUTE4 
            ,DESCRIPTION
            ,ATTRIBUTE9 
    UNION ALL
    SELECT ATTRIBUTE10 AS    BASE_YYYYMM
          ,ATTRIBUTE1 AS SCENARIO_TYPE_CD
          ,ATTRIBUTE2 AS DIV_CD
          ,ATTRIBUTE3 AS SUBSDR_CD
          ,'*' AS AU_CD
          ,'N' AS MANUAL_ADJ_FLAG
          ,'MGN_PROFIT' AS KPI_CD
          ,'BEP_SMART_GNT' AS CAT_CD
          ,'*' AS SUB_CAT_CD
          ,ATTRIBUTE5 AS ZONE_RNR_CD
          ,'*' AS SUBSDR_RNR_CD
          ,ATTRIBUTE8 AS CNTRY_CD
          ,ATTRIBUTE11 AS APPLY_YYYYMM
          ,SUM(TO_NUMBER(ATTRIBUTE21)) AS CURRM_KRW_AMT
          ,SUM(TO_NUMBER(ATTRIBUTE20)) AS CURRM_USD_AMT
          ,ATTRIBUTE4 AS ATTRIBUTE1_VALUE
          ,DESCRIPTION AS ATTRIBUTE2_VALUE
          ,ATTRIBUTE9 AS ATTRIBUTE3_VALUE
          ,CURRENT TIMESTAMP   
          ,'ares'              
          ,CURRENT TIMESTAMP   
          ,'ares'                    
    FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
    WHERE  SEQ = '1600'
    AND    CODE_ID  = p_category
    AND    YYYYMMDD = p_yyyymm
    GROUP BY ATTRIBUTE10
            ,ATTRIBUTE1 
            ,ATTRIBUTE2 
            ,ATTRIBUTE3 
            ,ATTRIBUTE5 
            ,ATTRIBUTE8 
            ,ATTRIBUTE11
            ,ATTRIBUTE4 
            ,DESCRIPTION
            ,ATTRIBUTE9 
    WITH UR;

    SET v_load_progress_status_code        = SQLSTATE;

    COMMIT;

    /*--------------------
       ETL JOB LOG 생성
    ---------------------*/
    CALL sp_cd_etl_job_logs( v_etl_job_no,
                             v_basis_yyyymmdd,
                             v_load_start_timestamp,
                             v_serial_no,
                             v_load_progress_status_code,
                             v_target_insert_count,
                             v_target_update_count,
                             v_target_delete_count,
                             v_source_table_name,
                             v_target_table_name,
                             v_job_notes
                           );

    COMMIT;

    /*--------------------------------
       ARES JOB MONITORING LOG 생성
    ---------------------------------*/
    FOR C1 AS
        SELECT DISTINCT
               DIV_CD as v_division_code,
               SUBSTR(SCENARIO_TYPE_CD,1,2) as v_scenario_code
        FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
        WHERE  BASE_YYYYMM  = p_yyyymm
        AND    CAT_CD       = p_category
        AND    MANUAL_ADJ_FLAG = 'N'
    DO
        CALL sp_cd_res_job_monitor_logs( p_yyyymm,
                                         p_category,
                                         v_scenario_code,
                                         'SYSTEM',
                                         v_division_code,
                                         'ares'
                                        );
    END FOR;

    COMMIT;

END