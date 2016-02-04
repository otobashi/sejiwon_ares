CREATE OR REPLACE PROCEDURE SP_CD_RES_KPI_BACKLOG_B2B_HIS
(
  IN P_BASIS_YYYYMM  VARCHAR(6)
)
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL

BEGIN
  /************************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                         */
  /* 2.모       듈 :                                                                              */
  /* 3.프로그램 ID : SP_CD_RES_KPI_BACKLOG_B2B_HIS                                                */
  /* 4.설       명 :                                                                              */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.02.04  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_BACKLOG_B2B_HIS';
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

    DECLARE C1 CURSOR WITH HOLD WITH RETURN FOR -- CURSOR WITH RETURN FOR(프로시저 간 결과 세트를 호출할 때)
    
    SELECT A.GRP_CD
          ,A.PROD_CD
          ,A.KOR_NM
          ,A.ENG_NM
          ,CASE B.SEQ
                WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
          ,A.ACCT_CD
          ,A.SCENARIO_TYPE_CD
          ,SUM(A.CURRM_KRW_AMT)
          ,SUM(A.CURRM_USD_AMT)
          ,SUM(A.ACCUM_KRW_AMT)
          ,SUM(A.ACCUM_USD_AMT)
    FROM  (
           -- PROD
           SELECT B.GRP_CD  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,B.KOR_NM  AS KOR_NM
                 ,B.ENG_NM  AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BACKLOGBALAMT' THEN '수주잔고'
                       WHEN 'BBRATIO05W'    THEN 'BB RATIO' 
                       END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
                 ,(SELECT CORPORATION_CODE AS GRP_CD
                         ,ATTRIBUTE2 AS PROD_CD
                         ,DISPLAY_NAME1 AS KOR_NM
                         ,DISPLAY_NAME2 AS ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                   WHERE  CODE_TYPE = 'B2B_BACKLOG') B
           WHERE  A.PROD_CD = B.PROD_CD
           AND    A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.SCENARIO_TYPE_CD IN ('AC0')
           AND    A.ACCT_CD IN ('BACKLOGBALAMT','BBRATIO05W')
           AND    A.DATA_DELIMT_CD = 'DIV'
           GROUP BY B.GRP_CD
                   ,A.PROD_CD
                   ,B.KOR_NM
                   ,B.ENG_NM
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD
          ) A
         ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
           SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
    GROUP BY A.GRP_CD
            ,A.PROD_CD
            ,A.KOR_NM
            ,A.ENG_NM
            ,CASE B.SEQ
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    WITH UR;

    OPEN C1;

   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_DIV_S';
    SET v_basis_yyyymmdd             = P_BASIS_YYYYMM;
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