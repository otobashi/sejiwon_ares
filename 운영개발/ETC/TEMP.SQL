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
           AND    A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
                       WHEN 'PR1' THEN TO_CHAR(TO_DATE('201512', 'YYYYMM')+1 MONTH, 'YYYYMM') END AS BASE_YYYYMM
                 ,CASE A.ACCT_CD
                       WHEN '41000000' THEN '결산매출'
                       WHEN '549999PL' THEN '결산영업이익' END AS ACCT_CD
                 ,CASE A.SCENARIO_TYPE_CD
                       WHEN 'AC0' THEN '결산실적'
                       WHEN 'PR1'  THEN '결산계획' END AS SCENARIO_TYPE_CD
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
           WHERE  A.DIV_CD = B.DIV_CD
           AND    A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
           AND    A.SCENARIO_TYPE_CD IN ('AC0','PR1')
           AND    A.ACCT_CD IN ('41000000','549999PL')
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
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
           WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE('201512', 'YYYYMM')- 24 MONTH, 'YYYYMM') AND '201512'
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
