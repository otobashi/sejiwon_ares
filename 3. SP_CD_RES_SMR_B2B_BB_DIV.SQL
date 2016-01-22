CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2B_BB_DIV (
     IN P_BASIS_YYYYMMDD VARCHAR(8),
     IN P_DIV_NM         VARCHAR(50),
     IN P_SUBSDR_CD      VARCHAR(8)   
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2B_BB_DIV                                                     */
  /* 4.설       명 : SMART 주차별Trend(BB RATIO)을 DIVISION 국가별 Result Set으로 return함        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMMDD( 기준일 )                                                */
  /*                 IN P_DIV_NM( DIVISION NAME )                                                 */
  /*                 IN P_SUBSDR_CD( 법인코드 )                                                   */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.14  최초 작성                                                      */
  /*  1.1      shlee   2016.01.15  IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER로 정리                 */
  /*  1.2      shlee   2016.01.19  SUBSDR_CD 구분추가                                             */
  /*  1.3      shlee   2016.01.22  CURRM_USD_AMT 로 변경                                          */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2B_BB_DIV';
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

-- 1. DIVISION전체
      SELECT C.KOR_NM AS COL_INDEX
            ,CASE WHEN E.SEQ = 1 THEN 'ALL'
                  WHEN E.SEQ = 2 THEN D.DISP_KOR_NM END        AS NATION_NM
            ,CASE WHEN A.KPI_CD = 'BB'     THEN 'BB'
                  WHEN A.KPI_CD = 'BB_W5'  THEN 'W5'
                  WHEN A.KPI_CD = 'BB_W13' THEN 'W13'
                  WHEN A.KPI_CD = 'BB_W52' THEN 'W52' END      AS KPI_CD
            ,A.APPLY_YYYYMM       AS YYYYWW
            ,SUM(CASE WHEN A.KPI_CD = 'BB'     THEN A.CURRM_USD_AMT
                      WHEN A.KPI_CD = 'BB_W5'  THEN A.CURRM_USD_AMT
                      WHEN A.KPI_CD = 'BB_W13' THEN A.CURRM_USD_AMT
                      WHEN A.KPI_CD = 'BB_W52' THEN A.CURRM_USD_AMT END) AS AMOUNT
            ,MIN(C.DISP_SEQ) AS DISP_SEQ
            ,MIN(C.ENG_NM) AS COL_INDEX_ENG
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT MAX(WK.YYYYMM1) AS START_YYYYMM
                    ,MAX(WK.YYYYWW1) AS START_YYYYWW
                    ,MAX(WK.YYYYMM2) AS END_YYYYMM
                    ,MAX(WK.YYYYWW2) AS END_YYYYWW
              FROM  (SELECT '' AS YYYYMM1
                           ,'' AS YYYYWW1
                           ,ATTRIBUTE2 AS YYYYMM2
                           ,CODE_ID AS YYYYWW2
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'WEEK'
                     AND    P_BASIS_YYYYMMDD BETWEEN ATTRIBUTE3 AND ATTRIBUTE4
                     UNION ALL              
                     SELECT ATTRIBUTE2
                           ,CODE_ID
                           ,''
                           ,''
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'WEEK'
                     AND    TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY,'YYYYMMDD') BETWEEN ATTRIBUTE3 AND ATTRIBUTE4
                    ) WK ) B
            ,(SELECT DIVISION_CODE AS DIV_CD
                    ,DISPLAY_NAME1 AS KOR_NM
                    ,DISPLAY_NAME2 AS ENG_NM
                    ,DISPLAY_ORDER_SEQ AS DISP_SEQ
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
              WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
              AND    USE_FLAG = 'Y'
              AND    DISPLAY_NAME1 = P_DIV_NM ) C
            ,(SELECT CORPORATION_CODE AS CORP_CD
                    ,DISPLAY_NAME1 AS DISP_ENG_NM
                    ,DISPLAY_NAME2 AS DISP_KOR_NM
                    ,DISPLAY_ORDER_SEQ AS DISP_SEQ 
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
              WHERE  CODE_TYPE = 'SMART_CORP_NATION') D
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) E
      WHERE  A.DIV_CD    = C.DIV_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = D.CORP_CD
      AND    A.APPLY_YYYYMM BETWEEN B.START_YYYYWW AND B.END_YYYYWW
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD    LIKE 'BB%'
      AND    A.CAT_CD    = 'BEP_SMART_BB'
      GROUP BY C.KOR_NM
              ,CASE WHEN E.SEQ = 1 THEN 'ALL'
                    WHEN E.SEQ = 2 THEN D.DISP_KOR_NM END
              ,CASE WHEN A.KPI_CD = 'BB'     THEN 'BB'
                    WHEN A.KPI_CD = 'BB_W5'  THEN 'W5'
                    WHEN A.KPI_CD = 'BB_W13' THEN 'W13'
                    WHEN A.KPI_CD = 'BB_W52' THEN 'W52' END
              ,A.APPLY_YYYYMM
      WITH UR;
      
    OPEN C1;
   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_SUBSDR_CNTRY';
    SET v_basis_yyyymmdd             = P_BASIS_YYYYMMDD;
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