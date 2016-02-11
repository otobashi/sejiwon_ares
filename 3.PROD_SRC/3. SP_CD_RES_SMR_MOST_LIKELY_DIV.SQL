CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TOT_MOST_LIKELY_DIV (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD    VARCHAR(8),
     IN P_DIV_CD       VARCHAR(10)
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_TOT_MOST_LIKELY_DIV                                            */
  /* 4.설       명 : SMART B2C/B2B 매출/영업이익 M/L 추이를 Result Set으로 return함               */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인 )                                                       */
  /*                 IN P_DIV_CD( DIVISION CODE )                                                 */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.11  최초 작성                                                      */
  /*  1.1      shlee   2016.01.12  BASE_YYYYMM 통합                                               */
  /*  1.2      shlee   2016.01.18  직전3개월평균추가 및 정리                                      */
  /*  1.3      shlee   2016.01.19  전월ML 및 이동계획 법인전체 추가                               */
  /*  1.4      shlee   2016.01.27  한글/영문 추가작업필요없음.                                    */
  /*  1.5      shlee   2016.02.11  BEP_SMART_DIV -> BEP_SMART_ML 변경                             */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TOT_MOST_LIKELY_DIV';
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
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.전월 Most Likely 3개월
      SELECT '전월 Most Likely'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.전월 Most Likely 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '전월 Most Likely'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        

      UNION ALL

-- 1.전월  법인전체ML
      SELECT '전월  법인전체ML'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.전월  법인전체ML 3개월
      SELECT '전월  법인전체ML'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.전월  법인전체ML 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '전월  법인전체ML'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '전월  법인전체ML'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        

      UNION ALL

-- 1.전월 이동
      SELECT '전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.전월 이동 3개월
      SELECT '전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.전월 이동 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '전월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '전월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        
              
      UNION ALL

-- 1.전전월 이동
      SELECT '전전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.전전월 이동 3개월
      SELECT '전전월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.전전월 이동 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '전전월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '전전월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        
              
      UNION ALL

-- 1.당월 이동
      SELECT '당월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.당월 이동 3개월
      SELECT '당월 이동'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.당월 이동 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '당월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '당월 이동'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        

      UNION ALL

-- 1.당월 법인전체
      SELECT '당월 법인전체'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD

      UNION ALL
      
-- 1-1.당월 법인전체 3개월
      SELECT '당월 법인전체'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 1-1.당월 법인전체 누계
      SELECT A.COL_INDEX
            ,A.SUBSDR_CD
            ,A.BASE_YYYYMM
            ,A.KPI_CD
            ,SUM(A.AMOUNT)
            ,'0'
      FROM  (      
            SELECT '당월 법인전체'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
            UNION ALL        
            SELECT '당월 법인전체'         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.APPLY_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            GROUP BY A.SUBSDR_CD
                    ,A.KPI_CD
                    ,SUBSTR(A.APPLY_YYYYMM,1,4)
            ) A
      GROUP BY A.COL_INDEX
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD        
                    
      UNION ALL

-- 5.전년실적
      SELECT Z.COL_INDEX
            ,Z.SUBSDR_CD
            ,Z.BASE_YYYYMM
            ,Z.KPI_CD
            ,SUM(Z.AMOUNT)
            ,Z.SORT_KEY
      FROM (
            SELECT '전년실적'           AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 YEAR, 'YYYY')||SUBSTR(A.APPLY_YYYYMM,5,2)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,A.CURRM_USD_AMT      AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 11 MONTH, 'YYYYMM') AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 9 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            ) Z
      GROUP BY Z.COL_INDEX
            ,Z.SUBSDR_CD
            ,Z.BASE_YYYYMM
            ,Z.KPI_CD
            ,Z.SORT_KEY            

      UNION ALL
-- 5-1.전년실적 3개월
      SELECT '전년실적'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 11 MONTH, 'YYYYMM') AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 9 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.KPI_CD

      UNION ALL
-- 5-2.전년실적 01~현재월
      SELECT Z.COL_INDEX
            ,Z.SUBSDR_CD
            ,Z.BASE_YYYYMM
            ,Z.KPI_CD
            ,SUM(Z.AMOUNT) AS AMOUNT
            ,MIN(Z.SORT_KEY) AS SORT_KE
      FROM (
            SELECT '전년실적'           AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,SUBSTR(A.BASE_YYYYMM,1,4)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                  ,(SELECT CODE_ID DIV_CD
                          ,CODE_NAME
                          ,ATTRIBUTE1 KOR_NM
                          ,ATTRIBUTE2 ENG_NM
                    FROM   IPTDW.IPTDW_RES_DIM_CODES
                    WHERE  CODE_ID = P_DIV_CD
                   ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 11 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 9 MONTH, 'YYYYMM')
            AND    A.BASE_YYYYMM <= TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY A.SUBSDR_CD
                    ,SUBSTR(A.BASE_YYYYMM,1,4)
                    ,A.KPI_CD
            ) Z
      GROUP BY Z.COL_INDEX
              ,Z.SUBSDR_CD
              ,Z.BASE_YYYYMM
              ,Z.KPI_CD

      UNION ALL

-- 8.직전 3개월 평균
      SELECT '직전 3개월 평균'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM  AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,AVG(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM') AND TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.APPLY_YYYYMM > TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.APPLY_YYYYMM 
              ,A.KPI_CD

      UNION ALL

-- 7.Most Likely
      SELECT 'Most Likely '||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
           ,(
             SELECT ATTRIBUTE1 AS WEEK_NO
                   ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                   ,ROWNUMBER() OVER() AS SEQ
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'SMART_WEEK'
             AND    ATTRIBUTE2 = P_BASIS_YYYYMM
            ) C
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
      UNION ALL
-- 7-1.Most Likely 3개월
      SELECT 'Most Likely '||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,'3개월'              AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,C.THU                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_ID = P_DIV_CD
            ) B
           ,(
             SELECT ATTRIBUTE1 AS WEEK_NO
                   ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                   ,ROWNUMBER() OVER() AS SEQ
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'SMART_WEEK'
             AND    ATTRIBUTE2 = P_BASIS_YYYYMM
            ) C
      WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.KPI_CD
              ,C.THU
      UNION ALL
-- 7-2.Most Likely 01~현재월
      SELECT Z.COL_INDEX
            ,Z.SUBSDR_CD
            ,Z.BASE_YYYYMM
            ,Z.KPI_CD
            ,SUM(Z.AMOUNT) AS AMOUNT
            ,MIN(Z.SORT_KEY) AS SORT_KE
      FROM (
            SELECT 'Most Likely '||C.SEQ         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,'누계'               AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,C.THU                AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
                 ,(
                   SELECT ATTRIBUTE1 AS WEEK_NO
                         ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                         ,ROWNUMBER() OVER() AS SEQ
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = 'SMART_WEEK'
                   AND    ATTRIBUTE2 = P_BASIS_YYYYMM
                  ) C
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(P_BASIS_YYYYMM,1,4)||'01' AND P_BASIS_YYYYMM
            AND    A.BASE_YYYYMM <= TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            GROUP BY C.SEQ
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
                    ,C.THU
            UNION ALL
            SELECT 'Most Likely '||C.SEQ         AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,'누계'  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,C.THU                AS SORT_KEY
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_ID = P_DIV_CD
                  ) B
                 ,(
                   SELECT ATTRIBUTE1 AS WEEK_NO
                         ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                         ,ROWNUMBER() OVER() AS SEQ
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = 'SMART_WEEK'
                   AND    ATTRIBUTE2 = P_BASIS_YYYYMM
                  ) C
            WHERE  A.BASE_YYYYMM = P_BASIS_YYYYMM
            AND    A.APPLY_YYYYMM <= TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = P_SUBSDR_CD
            AND    A.DIV_CD = B.DIV_CD
            AND    A.ZONE_RNR_CD = C.WEEK_NO
            GROUP BY C.SEQ
                    ,A.SUBSDR_CD
                    ,A.KPI_CD
                    ,C.THU
            ) Z
      GROUP BY Z.COL_INDEX
              ,Z.SUBSDR_CD
              ,Z.BASE_YYYYMM
              ,Z.KPI_CD
                                          
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