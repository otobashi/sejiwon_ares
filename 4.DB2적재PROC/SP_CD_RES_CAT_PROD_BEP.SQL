CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_CAT_PROD_BEP (
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
  /* 2.실 행 방 법 : CALL SP_CD_RES_CAT_PROD_BEP('201301','BEP_SMART_PROD_BEP')               */
  /* 3.프로그램 ID : SP_CD_RES_CAT_PROD_BEP                                                   */
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
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_CAT_PROD_BEP';
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
      ,ACCU_KRW_AMT
      ,ACCU_USD_AMT
      ,ATTRIBUTE1_VALUE
      ,ATTRIBUTE2_VALUE
      ,ATTRIBUTE3_VALUE
      ,ATTRIBUTE4_VALUE
      ,ATTRIBUTE5_VALUE
      ,CREATION_DATE
      ,CREATION_USR_ID
      ,LAST_UPD_DATE
      ,LAST_UPD_USR_ID
    )
    SELECT A.ATTRIBUTE1 AS BASE_YYYYMM
          ,A.ATTRIBUTE2 AS SCENARIO_TYPE_CD
          ,SUBSTR(A.ATTRIBUTE4,1,3) AS DIV_CD
          ,A.ATTRIBUTE5 AS SUBSDR_CD
          ,'*' AS AU_CD
          ,'N' AS MANUAL_ADJ_FLAG
          ,A.ATTRIBUTE6 AS KPI_CD
          ,p_category   AS CAT_CD
          ,A.ATTRIBUTE4 AS SUB_CAT_CD
          ,'*' AS ZONE_RNR_CD
          ,'*' AS SUBSDR_RNR_CD
          ,'*' AS CNTRY_CD
          ,A.ATTRIBUTE1 AS APPLY_YYYYMM
          ,SUM(TO_NUMBER(A.ATTRIBUTE8)) AS CURRM_KRW_AMT
          ,SUM(TO_NUMBER(A.ATTRIBUTE9)) AS CURRM_USD_AMT
          ,SUM(TO_NUMBER(A.ATTRIBUTE10)) AS ACCU_KRW_AMT
          ,SUM(TO_NUMBER(A.ATTRIBUTE11)) AS ACCU_USD_AMT
          ,MIN(ATTRIBUTE7) AS ATTRIBUTE1_VALUE
          ,NULL AS ATTRIBUTE2_VALUE
          ,NULL AS ATTRIBUTE3_VALUE
          ,NULL AS ATTRIBUTE4_VALUE
          ,NULL AS ATTRIBUTE5_VALUE
          ,CURRENT TIMESTAMP AS CREATION_DATE
          ,'ares' AS CREATION_USR_ID
          ,CURRENT TIMESTAMP AS LAST_UPD_DATE
          ,'ares' AS LAST_UPD_USR_ID  
    FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA A
    WHERE  A.SEQ = '1700'
    AND    A.CODE_ID  = p_category
    AND    A.YYYYMMDD = p_yyyymm
    GROUP BY A.ATTRIBUTE1
            ,A.ATTRIBUTE2
            ,SUBSTR(A.ATTRIBUTE4,1,3)
            ,A.ATTRIBUTE5
            ,A.ATTRIBUTE6
            ,A.ATTRIBUTE4
    
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