    -- 전체실적
    SELECT CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '매출'
                WHEN '549999PL' THEN '영업이익' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '실적'
                ELSE '계획' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
--    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
    WHERE  A.BASE_YYYYMM    = '201512'
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('AC0')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    
    -- 전체실적
    SELECT CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '매출'
                WHEN '549999PL' THEN '영업이익' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '실적'
                ELSE '계획' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
--    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
    WHERE  A.BASE_YYYYMM    = '201512'
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL

    -- M/L
    SELECT CASE A.SCENARIO_CODE
                WHEN 'AC0' THEN A.BASIS_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.KPI_TYPE_CODE
                WHEN 'SALE' THEN '매출'
                WHEN 'COI'  THEN '영업이익' END AS ACCT_CD
          ,CASE A.SCENARIO_CODE
                WHEN 'AC0' THEN '실적'
                ELSE '계획' END AS SCENARIO_TYPE_CD
          ,'ML' AS B2B_TYPE
          ,A.WEEK_INPUT AS WEEK_INPUT
          ,SUM(A.CURR_MON_KRW_AMOUNT) AS CURRM_KRW_AMT
          ,SUM(A.CURR_MON_USD_AMOUNT) AS CURRM_USD_AMT
          ,NULL AS ACCUM_KRW_AMT
          ,NULL AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_ADJ_MOST_LIKELY A
--    WHERE  A.BASIS_YYYYMM   BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
    WHERE  A.BASIS_YYYYMM   = '201512'
    AND    A.DIVISION_CODE  = 'GBU'
    AND    A.KPI_TYPE_CODE  IN ('SALE','COI')
    AND    A.SCENARIO_CODE  IN ('AC0','PR1','PR2','PR3','PR4')
    GROUP BY A.BASIS_YYYYMM
            ,A.KPI_TYPE_CODE
            ,A.SCENARIO_CODE
            ,A.WEEK_INPUT

    UNION ALL
    
    -- B2B실적
    SELECT CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN 'BEP20000B2B' THEN '매출'
                WHEN 'BEP60000B2B' THEN '영업이익' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '실적'
                ELSE '계획' 
                END AS SCENARIO_TYPE_CD
          ,'B2B' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
--    WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
    WHERE  A.BASE_YYYYMM     = '201512'
    AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
    AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
    AND    A.DATA_DELIMT_CD  = 'DIV'
    AND    A.PROD_CD         = 'ALL'
    AND    A.SUBSDR_CD       = 'ALL'
    AND    A.KPI_TYPE_CD     = 'B2B'
    AND    A.SUMM_FLAG       = 'Y'
    AND    A.SCENARIO_TYPE_CD IN ('AC0','PR1','PR2','PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    -- B2C
    SELECT A.BASE_YYYYMM
          ,A.ACCT_CD
          ,A.SCENARIO_TYPE_CD
          ,A.B2B_TYPE
          ,A.WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT      
    FROM   (
            -- 전체실적
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN '41000000' THEN '매출'
                        WHEN '549999PL' THEN '영업이익' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '실적'
                        ELSE '계획' END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
        --    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
            WHERE  A.BASE_YYYYMM    = '201512'
            AND    A.DIV_CD         = 'ALL'
            AND    A.SUBSDR_CD      = 'ALL'
            AND    A.LDGR_TYPE_CD   = '1'
            AND    A.SUBSDR_TYPE_CD = 'S'
            AND    A.KPI_TYPE_CD    = 'TB'
            AND    A.ACCT_CD        IN ('41000000','549999PL')
            AND    A.SCENARIO_TYPE_CD IN ('AC0','PR1','PR2','PR3','PR4')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD

            UNION ALL

            -- B2B실적
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN 'BEP20000B2B' THEN '매출'
                        WHEN 'BEP60000B2B' THEN '영업이익' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '실적'
                        ELSE '계획' 
                        END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
        --    WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE('201512', 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND '201512'
            WHERE  A.BASE_YYYYMM     = '201512'
            AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
            AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
            AND    A.DATA_DELIMT_CD  = 'DIV'
            AND    A.PROD_CD         = 'ALL'
            AND    A.SUBSDR_CD       = 'ALL'
            AND    A.KPI_TYPE_CD     = 'B2B'
            AND    A.SUMM_FLAG       = 'Y'
            AND    A.SCENARIO_TYPE_CD IN ('AC0','PR1','PR2','PR3','PR4')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD
           ) A    
    GROUP BY A.BASE_YYYYMM
          ,A.ACCT_CD
          ,A.SCENARIO_TYPE_CD
          ,A.B2B_TYPE
          ,A.WEEK_INPUT 
            