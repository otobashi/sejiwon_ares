CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2B_PIPELINE (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD    VARCHAR(8)     
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2B_PIPELINE                                                   */
  /* 4.설       명 : SMART 월별Trend(파이프라인)을 Result Set으로 return함                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인코드 )                                                   */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.12  최초 작성                                                      */
  /*  1.1      shlee   2016.01.15  IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER로 정리                 */
  /*  1.2      shlee   2016.01.19  SUBSDR_CD 구분추가                                             */
  /*  1.3      shlee   2016.02.16  수주잔고 가져오는 테이블변경                                   */
  /*  1.4      shlee   2016.02.17  MIS DASHBOARD 데이타로 변경                                    */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2B_PIPELINE';
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

      SELECT B.KOR_NM           AS COL_INDEX
            ,CASE WHEN A.ACCT_CD = 'BACKLOGBALAMT' THEN '수주잔고'
                  WHEN A.ACCT_CD = 'BEP20000B2B'   THEN '매출(전체)'
                  WHEN A.ACCT_CD = 'BEP20000B2BP'  THEN '매출(프로젝트)' END AS KPI_CD
            ,CASE WHEN C.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN C.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,MIN(B.DISP_SEQ) AS DISP_SEQ
            ,B.ENG_NM         AS COL_INDEX_ENG
      FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
            ,(SELECT CASE DIVISION_CODE
                          WHEN 'GNTHT' THEN 'GNT_L2_5'
                          WHEN 'GNTCS' THEN 'GNT_L2_1'
                          ELSE DIVISION_CODE END AS DIV_CD
                    ,DISPLAY_NAME1 AS KOR_NM
                    ,DISPLAY_NAME2 AS ENG_NM
                    ,DISPLAY_ORDER_SEQ AS DISP_SEQ
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
              WHERE  CODE_TYPE = 'PIPELINE'
              AND    USE_FLAG = 'Y'
              UNION ALL
              SELECT 'ALL' AS DIV_CD
                    ,'법인전체' AS KOR_NM
                    ,'CORP TOTAL' AS ENG_NM
                    ,0 AS DISP_SEQ
              FROM   SYSIBM.SYSDUMMY1) B
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) C
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.ACCT_CD in ( 'BACKLOGBALAMT','BEP20000B2B','BEP20000B2BP')
      AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
      AND    A.PROD_CD = B.DIV_CD
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      GROUP BY B.KOR_NM
              ,CASE WHEN A.ACCT_CD = 'BACKLOGBALAMT' THEN '수주잔고'
                        WHEN A.ACCT_CD = 'BEP20000B2B'   THEN '매출(전체)'
                        WHEN A.ACCT_CD = 'BEP20000B2BP'  THEN '매출(프로젝트)' END
              ,CASE WHEN C.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                        WHEN C.SEQ = 2 THEN A.BASE_YYYYMM END 
              ,B.ENG_NM

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