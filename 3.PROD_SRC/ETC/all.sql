    -- �����1. W5 �����ֱ�
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B 
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 36 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����1. W5 �����ֱ� - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 43 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����1. W5 �����ֱ� - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 50 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����1. W5 �����ֱ� - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 57 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����1. W5 �����ֱ� - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 64 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    UNION ALL
    -- �����2. W13 �����ֱ�
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 92 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����2. W13 �����ֱ� - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 99 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����2. W13 �����ֱ� - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 106 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����2. W13 �����ֱ� - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 113 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����2. W13 �����ֱ� - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 120 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    UNION ALL
    -- �����3. W52 �����ֱ�
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 365 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����3. W52 �����ֱ� - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 372 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����3. W52 �����ֱ� - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 379 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����3. W52 �����ֱ� - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 386 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- �����3. W52 �����ֱ� - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS NATION_KOR_NM
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'ALL' AS DIV_KOR_NM
          ,'ALL' AS DIV_ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
--          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 393 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = P_DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW

    WITH UR;