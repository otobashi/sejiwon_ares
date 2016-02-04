
      SELECT A.DIV_CD
            ,A.SUB_CAT_CD
            ,D.KPI_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
/*
              SELECT 'BEP20000000'  AS KPI_CD, '매출'                   AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP20070000'  AS KPI_CD, '가격성판촉비'           AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP50000000'  AS KPI_CD, '한계이익'               AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
--              SELECT 'BEP50000000%' AS KPI_CD, '한계이익율'             AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP40010400'  AS KPI_CD, '광고선전비'             AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP40010500'  AS KPI_CD, '판매촉진비'             AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP60000000'  AS KPI_CD, '영업이익'               AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
--              SELECT 'BEP50000000R' AS KPI_CD, '한계적자 모델 매출비중' AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL*/
              SELECT 'BEP5000SALE'  AS KPI_CD, '한계적자매출'           AS KPI_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'BEP5000MGNL'  AS KPI_CD, '한계적자금액'           AS KPI_NM FROM SYSIBM.SYSDUMMY1
              
             ) D
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD_BEP'
      AND    A.KPI_CD = D.KPI_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,D.KPI_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD
