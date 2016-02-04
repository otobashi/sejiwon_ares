CREATE OR REPLACE PROCEDURE SP_CD_RES_KPI_SALE_DIV_B2B_HIS
(
  IN P_BASIS_YYYYMM  VARCHAR(6)
)
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL

BEGIN
  /************************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                         */
  /* 2.모       듈 :                                                                              */
  /* 3.프로그램 ID : SP_CD_RES_KPI_SALE_DIV_B2B_HIS                                               */
  /* 4.설       명 : 참조. 이동계획 적확도                                                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.02.03  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_SALE_DIV_B2B_HIS';
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
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
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
                   WHERE  CODE_TYPE = 'B2B_ETC_B2C') B
           WHERE  A.PROD_CD = B.PROD_CD
           AND    A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           AND    A.DATA_DELIMT_CD = 'PROD'
           GROUP BY B.GRP_CD
                   ,A.PROD_CD
                   ,B.KOR_NM
                   ,B.ENG_NM
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- DIV
           SELECT B.GRP_CD  AS GRP_CD
                 ,A.DIV_CD  AS PROD_CD
                 ,B.KOR_NM  AS KOR_NM
                 ,B.ENG_NM  AS ENG_NM
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN A.BASE_YYYYMM
                       WHEN 'PR1' THEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')+1 MONTH, 'YYYYMM') END AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN '41000000' THEN '결산매출'
                       WHEN '549999PL' THEN '결산영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '결산실적'
                       WHEN 'PR1' THEN '결산계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
                 ,(SELECT CORPORATION_CODE AS GRP_CD
                         ,ATTRIBUTE2    AS DIV_CD
                         ,DISPLAY_NAME1 AS KOR_NM
                         ,DISPLAY_NAME2 AS ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                   WHERE  CODE_TYPE = 'B2B_ETC_B2C') B
                 ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL 
                   SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
                   SELECT 3 AS SEQ FROM SYSIBM.SYSDUMMY1) C
           WHERE  A.DIV_CD = B.DIV_CD
           AND    A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.SCENARIO_TYPE_CD IN ('AC0','PR1')
           AND    A.ACCT_CD IN ('41000000','549999PL')
           AND    A.KPI_TYPE_CD     = 'TB'
           AND    A.LDGR_TYPE_CD    = '1'
           AND    A.CMPNY_EXCL_FLAG = 'N'
           AND    A.SUBSDR_CD       = 'ALL'
           GROUP BY B.GRP_CD
                   ,A.DIV_CD
                   ,B.KOR_NM
                   ,B.ENG_NM
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- B2B_ALL
           SELECT 'B2B합계'  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,'B2B합계'  AS KOR_NM
                 ,'B2B_ALL TOTAL' AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
           AND    A.SUBSDR_CD = 'ALL'
           AND    A.PROD_CD   = 'ALL'
           AND    A.KPI_TYPE_CD = 'B2B'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           GROUP BY A.PROD_CD
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- B2B_PR
           SELECT 'B2B제품합계'  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,'B2B제품합계'  AS KOR_NM
                 ,'B2B_PR TOTAL' AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.DIV_BIZ_TYPE_CD = 'B2B_PR'
           AND    A.SUBSDR_CD = 'ALL'
           AND    A.PROD_CD   = 'ALL'
           AND    A.KPI_TYPE_CD = 'B2B'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           GROUP BY A.PROD_CD
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- OT_B2B
           SELECT '기타B2B합계'  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,'기타B2B합계'  AS KOR_NM
                 ,'OT_B2B TOTAL' AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.DIV_BIZ_TYPE_CD = 'OT_B2B'
           AND    A.SUBSDR_CD = 'ALL'
           AND    A.PROD_CD   = 'ALL'
           AND    A.KPI_TYPE_CD = 'B2B'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           GROUP BY A.PROD_CD
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- B2B_OF
           SELECT 'B2B부문합계'  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,'B2B부문합계'  AS KOR_NM
                 ,'B2B_OF TOTAL' AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.DIV_BIZ_TYPE_CD = 'B2B_OF'
           AND    A.SUBSDR_CD = 'ALL'
           AND    A.PROD_CD   = 'ALL'
           AND    A.KPI_TYPE_CD = 'B2B'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           GROUP BY A.PROD_CD
                   ,A.BASE_YYYYMM
                   ,A.ACCT_CD
                   ,A.SCENARIO_TYPE_CD

           UNION ALL

           -- B2C_PR
           SELECT 'B2C제품합계'  AS GRP_CD
                 ,A.PROD_CD AS PROD_CD
                 ,'B2C제품합계'  AS KOR_NM
                 ,'B2C_PR TOTAL' AS ENG_NM
                 ,A.BASE_YYYYMM AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN 'BEP20000000' THEN '매출'
                       WHEN 'BEP60000000' THEN '영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '실적'
                       WHEN 'MP'  THEN '계획' END AS SCENARIO_TYPE_CD
                 ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                 ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                 ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                 ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
           FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM')- 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
           AND    A.DIV_BIZ_TYPE_CD = 'B2C_PR'
           AND    A.SUBSDR_CD = 'ALL'
           AND    A.PROD_CD   = 'ALL'
           AND    A.KPI_TYPE_CD = 'B2B'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','MP')
           AND    A.ACCT_CD IN ('BEP20000000','BEP60000000')
           GROUP BY A.PROD_CD
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