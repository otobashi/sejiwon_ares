    -- 사업부1. W5 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W5' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B 
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 36 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부1. W5 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W5' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 43 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부1. W5 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W5' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 50 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부1. W5 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W5' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 57 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부1. W5 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W5' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 64 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    UNION ALL
    -- 사업부2. W13 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 92 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부2. W13 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 99 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부2. W13 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 106 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부2. W13 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 113 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부2. W13 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 120 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    UNION ALL
    -- 사업부3. W52 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 365 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부3. W52 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 372 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부3. W52 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 379 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부3. W52 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 386 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM
    -- 사업부3. W52 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,MAX(Z.LEV1) AS LEV1
          ,Z.ENG_NM    AS LEV2
          ,MAX(Z.KOR_NM) AS KOR_NM
          ,MAX(Z.ENG_NM) AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 393 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
          ,(SELECT A.DIV_CD
                  ,A.KOR_NM
                  ,A.ENG_NM 
                  ,B.ENG_NM AS LEV1
            FROM   (
                    SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('PIPELINE')
                   ) A
                  ,(SELECT DIVISION_CODE AS DIV_CD
                          ,DISPLAY_NAME1 AS KOR_NM
                          ,DISPLAY_NAME2 AS ENG_NM
                          ,DISPLAY_ORDER_SEQ AS DISP_SEQ
                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                    WHERE  CODE_TYPE IN ('BB_RATIO_UP','BB_RATIO')
                   ) B
            WHERE  A.DIV_CD = B.DIV_CD 
            AND    DIVISION_CODE = P_DIV_CD ) Z
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.DIV_CD = Z.DIV_CD
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    GROUP BY Z.ENG_NM

    WITH UR;
