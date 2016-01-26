CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_COMM_NM (
     IN P_BASIS_YYYYMM VARCHAR(6)
     )
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  /************************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                         */
  /* 2.모       듈 :                                                                              */
  /* 3.프로그램 ID : SP_CD_RES_SMR_COMM_NM                                                        */
  /* 4.설       명 : 사업부코드 명 가져오기 공통                                                  */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.26  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_COMM_NM';
    DECLARE v_load_start_timestamp       TIMESTAMP     DEFAULT NULL;
    DECLARE v_serial_no                  VARCHAR(30)   DEFAULT NULL;
    DECLARE v_load_progress_status_code  VARCHAR(10)   DEFAULT NULL;
    DECLARE v_target_insert_count        INTEGER       DEFAULT 0;
    DECLARE v_target_update_count        INTEGER       DEFAULT 0;
    DECLARE v_target_delete_count        INTEGER       DEFAULT 0;
    DECLARE v_source_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_target_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_job_notes                  VARCHAR(300)  DEFAULT NULL;
    DECLARE v_basis_yyyymmdd             VARCHAR(8)    DEFAULT NULL;
    DECLARE SQLSTATE                     CHAR(5)       DEFAULT '';

    DECLARE C1 CURSOR WITH HOLD WITH RETURN FOR

-- 법인코드/명 가져오기 전체공통
    SELECT 'CORP' AS CODE_TYPE
          ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
          ,A.ATTRIBUTE1        AS CODE
          ,A.DISPLAY_NAME1     AS KOR_NM
          ,A.DISPLAY_NAME2     AS ENG_NM
          ,A.CORPORATION_CODE  AS REF_CD
    FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
    WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
    AND   A.ATTRIBUTE2  IS NULL

    UNION ALL
-- 사업부
    SELECT 'DIV' AS CODE_TYPE
          ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
          ,A.DIVISION_CODE     AS CODE
          ,A.DISPLAY_NAME      AS KOR_NM
          ,A.DIVISION_NAME     AS ENG_NM
          ,A.COMPANY_CODE      AS REF_CD
    FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
    WHERE  A.BASIS_YYYYMM = '201601'
    AND    A.USE_FLAG = 'Y'

    UNION ALL
-- 국가
    SELECT 'NATION'            AS CODE_TYPE
          ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
          ,A.DIVISION_CODE     AS NATION_CD
          ,A.DISPLAY_NAME2     AS KOR_NM
          ,A.DISPLAY_NAME1     AS ENG_NM
          ,A.CORPORATION_CODE  AS REF_CD                        
    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
    WHERE  A.CODE_TYPE = 'SMART_CORP_NATION'        

    WITH UR;

    OPEN C1;
   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_SUBSDR_CNTRY';
    SET v_basis_yyyymmdd             = p_basis_yyyymm;
    SET v_load_progress_status_code  = SQLSTATE;

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
END