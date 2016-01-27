-- 1.법인합계
      SELECT '1.법인합계'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,MIN(BASE_YYYYMM)||'-'||MAX(BASE_YYYYMM)        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||'01' AND TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||SUBSTR('201601',5,2)
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EKHQ'
      GROUP BY A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

      UNION ALL

-- 2.사업부합계
      SELECT B.DISP_NM            AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,MIN(BASE_YYYYMM)||'-'||MAX(BASE_YYYYMM)        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT DIVISION_CODE AS DIV_CD
                   ,CASE WHEN DIVISION_CODE = 'GLT' THEN 'HE LCD TV'
                         WHEN DIVISION_CODE = 'MST' THEN 'MC 단말'
                         WHEN DIVISION_CODE = 'CNT' THEN 'H&A 냉장고'
                         ELSE DISPLAY_NAME END AS DISP_NM
             FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION
             WHERE  USE_FLAG = 'Y'
             AND    BASIS_YYYYMM = '201601'
             AND    DIVISION_CODE IN ('GLT','MST','CNT','DFT','DGT')
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||'01' AND TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||SUBSTR('201601',5,2)
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.DIV_CD IN ('GLT','MST','CNT','DFT','DGT')
      AND   A.DIV_CD = B.DIV_CD
      GROUP BY B.DISP_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

      UNION ALL

-- 3 LEVEL2
      SELECT B.PROD_KOR_NM        AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,MIN(BASE_YYYYMM)||'-'||MAX(BASE_YYYYMM)        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT ATTRIBUTE1    AS PROD_CD
                   ,DISPLAY_NAME1 AS PROD_KOR_NM
                   ,DISPLAY_NAME2 AS PROD_ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER 
             WHERE  CODE_TYPE = 'SMR_PROD_MST'
             AND    USE_FLAG  = 'Y'
             AND    ATTRIBUTE2= 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||'01' AND TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||SUBSTR('201601',5,2)
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.ATTRIBUTE3_VALUE = B.PROD_CD
      GROUP BY B.PROD_KOR_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD


      UNION ALL

-- 4 LEVEL3
      SELECT B.PROD_KOR_NM        AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,MIN(BASE_YYYYMM)||'-'||MAX(BASE_YYYYMM)        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT ATTRIBUTE1    AS PROD_CD
                   ,DISPLAY_NAME1 AS PROD_KOR_NM
                   ,DISPLAY_NAME2 AS PROD_ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER 
             WHERE  CODE_TYPE = 'SMR_PROD_MST'
             AND    USE_FLAG  = 'Y'
             AND    ATTRIBUTE2= 'B2C_DIV'
            ) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||'01' AND TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 0 YEAR, 'YYYY')||SUBSTR('201601',5,2)
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.ATTRIBUTE4_VALUE = B.PROD_CD
      GROUP BY B.PROD_KOR_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD
