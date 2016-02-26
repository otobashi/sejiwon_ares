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
