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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 2 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 2 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 2 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 2 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
      WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 1 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
      --      AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
            WHERE  A.BASE_YYYYMM = TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                  ,TO_CHAR(to_date('201512', 'YYYYMM') - 0 YEAR, 'YYYY')||SUBSTR(A.APPLY_YYYYMM,5,2)  AS BASE_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,A.CURRM_USD_AMT      AS AMOUNT
                  ,'0'                  AS SORT_KEY
            FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 ,(SELECT CODE_ID DIV_CD
                         ,CODE_NAME
                         ,ATTRIBUTE1 KOR_NM
                         ,ATTRIBUTE2 ENG_NM
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
            WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date('201512', 'YYYYMM') - 11 MONTH, 'YYYYMM') AND TO_CHAR(to_date('201512', 'YYYYMM') - 9 MONTH, 'YYYYMM')
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date('201512', 'YYYYMM') - 11 MONTH, 'YYYYMM') AND TO_CHAR(to_date('201512', 'YYYYMM') - 9 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                    WHERE  CODE_TYPE = 'B2C_DIV'
                   ) B
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR(TO_CHAR(to_date('201512', 'YYYYMM') - 11 MONTH, 'YYYYMM'),1,4)||'01' AND TO_CHAR(to_date('201512', 'YYYYMM') - 9 MONTH, 'YYYYMM')
            AND    A.BASE_YYYYMM <= TO_CHAR(to_date('201512', 'YYYYMM') - 1 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date('201512', 'YYYYMM') - 2 MONTH, 'YYYYMM') AND TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.APPLY_YYYYMM > TO_CHAR(to_date('201512', 'YYYYMM') - 0 MONTH, 'YYYYMM')
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(
             SELECT ATTRIBUTE1 AS WEEK_NO
                   ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                   ,ROWNUMBER() OVER() AS SEQ
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'SMART_WEEK'
             AND    ATTRIBUTE2 = '201512'
            ) C
      WHERE  A.BASE_YYYYMM = '201512'
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(
             SELECT ATTRIBUTE1 AS WEEK_NO
                   ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                   ,ROWNUMBER() OVER() AS SEQ
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'SMART_WEEK'
             AND    ATTRIBUTE2 = '201512'
            ) C
      WHERE  A.BASE_YYYYMM = '201512'
      AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
                 ,(
                   SELECT ATTRIBUTE1 AS WEEK_NO
                         ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                         ,ROWNUMBER() OVER() AS SEQ
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = 'SMART_WEEK'
                   AND    ATTRIBUTE2 = '201512'
                  ) C
            WHERE  A.BASE_YYYYMM BETWEEN SUBSTR('201512',1,4)||'01' AND '201512'
            AND    A.BASE_YYYYMM <= TO_CHAR(to_date('201512', 'YYYYMM') - 0 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD = 'AC0'
            AND    A.CAT_CD = 'BEP_SMART_DIV'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                   WHERE  CODE_TYPE = 'B2C_DIV'
                  ) B
                 ,(
                   SELECT ATTRIBUTE1 AS WEEK_NO
                         ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                         ,ROWNUMBER() OVER() AS SEQ
                   FROM   IPTDW.IPTDW_RES_DIM_CODES
                   WHERE  CODE_TYPE = 'SMART_WEEK'
                   AND    ATTRIBUTE2 = '201512'
                  ) C
            WHERE  A.BASE_YYYYMM = '201512'
            AND    A.APPLY_YYYYMM <= TO_CHAR(to_date('201512', 'YYYYMM') - 0 YEAR, 'YYYY')||'12'
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
            AND    A.CAT_CD = 'BEP_SMART_ML'
            AND    A.KPI_CD in ('SALE', 'COI')
            AND    A.SUBSDR_CD = 'EHAP'
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
                                          