CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TOT_MOST_LIKELY (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD VARCHAR(8),
     IN P_CODE_TYPE VARCHAR(10)
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_TOT_MOST_LIKELY                                                    */
  /* 4.설       명 : SMART B2C/B2B 매출/영업이익 M/L 추이를 Result Set으로 return함               */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인 )                                                       */
  /*                 IN P_CODE_TYPE( B2C/B2B구분 )                                                */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.11  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TOT_MOST_LIKELY';
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

-- 1.전월 Most Likely
      SELECT '전월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 1-1.전월 Most Likely 3개월
      SELECT '전월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 1-2.전월 Most Likely 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD


      UNION ALL
-- 2.법인전체
      SELECT '법인전체'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 2-1.법인전체 3개월
      SELECT '법인전체'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 2-2.법인전체 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '법인전체'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '법인전체'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

      UNION ALL
-- 3.전년실적
      SELECT '전년실적'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 3-1.전년실적 3개월
      SELECT '전년실적'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 3-2.전년실적 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전년실적'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'),1,4)||'01' ||'-'|| TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '전년실적'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'),1,4)||'01' ||'-'|| TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM'), 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 12 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD
      UNION ALL

-- 4.당월 이동
      SELECT '당월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR0' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 4-1.당월 이동 3개월
      SELECT '당월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 4-2.당월 이동 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '당월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '당월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

      UNION ALL

-- 5.전월 이동
      SELECT '전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 5-1.전월 이동 3개월
      SELECT '전월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 5-2.전월 이동 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

      UNION ALL

-- 6.전전월 이동
      SELECT '전전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 6-1.전전월 이동 3개월
      SELECT '전전월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 6-2.전전월 이동 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '전전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

      UNION ALL


-- 7.Most Likely
      SELECT 'Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR0' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.ZONE_RNR_CD
              ,A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 7-1.Most Likely 3개월
      SELECT 'Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.ZONE_RNR_CD
              ,A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 7-2.Most Likely 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT 'Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.ZONE_RNR_CD
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT 'Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
            AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.ZONE_RNR_CD
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

      UNION ALL


-- 8.전월 Most Likely
      SELECT '전월 Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE WHEN A.SCENARIO_TYPE_CD = 'PR1' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR2' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')
                  WHEN A.SCENARIO_TYPE_CD = 'PR3' THEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')
             END  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.ZONE_RNR_CD
              ,A.SUBSDR_CD
              ,A.SCENARIO_TYPE_CD
              ,A.KPI_CD
      UNION ALL
-- 8-1.전월 Most Likely 3개월
      SELECT '전월 Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM')||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM')  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = P_CODE_TYPE
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.ZONE_RNR_CD
              ,A.SUBSDR_CD
              ,A.KPI_CD
      UNION ALL
-- 8-2.전월 Most Likely 01~현재월
      SELECT COL_INDEX
            ,SUBSDR_CD
            ,BASE_YYYYMM
            ,KPI_CD
            ,SUM(AMOUNT) AS AMOUNT
            ,MIN(SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전월 Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.ZONE_RNR_CD
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
            UNION ALL
            SELECT '전월 Most Likely W'||A.ZONE_RNR_CD         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(P_BASIS_YYYYMM,1,4)||'01' ||'-'|| TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM')        AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = P_CODE_TYPE
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.ZONE_RNR_CD
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
            )
      GROUP BY COL_INDEX
              ,SUBSDR_CD
              ,BASE_YYYYMM
              ,KPI_CD

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