-- 1.매출/영업이익/가격성판촉비
      SELECT A.DIV_CD AS DIV_CD
            ,CASE A.KPI_CD
                  WHEN 'SALE' THEN '매출'
                  WHEN 'COI'  THEN '영업이익'
                  WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND '201510'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI','SALES_DEDUCTION')
      AND    A.SUBSDR_CD = 'EEUK'
      AND    A.DIV_CD = 'GLT'
      GROUP BY A.DIV_CD
              ,CASE A.KPI_CD
                    WHEN 'SALE' THEN '매출'
                    WHEN 'COI'  THEN '영업이익'
                    WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 2. 전년대비
      SELECT A.DIV_CD AS DIV_CD
            ,'전년대비'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE('201510', 'YYYYMM'), -12), 'YYYYMM'),1,4)||'12'
      AND    A.SCENARIO_TYPE_CD = 'MP'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = 'EEUK'
      AND    A.DIV_CD = 'GLT'
      GROUP BY A.DIV_CD
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END
              ,A.KPI_CD

      UNION ALL

-- 3. 광고선전비
      SELECT A.DIV_CD AS DIV_CD
            ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND '201510'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_OH'
      AND    A.KPI_CD in ('OH101000')
      AND    A.SUB_CAT_CD = 'FC'
      AND    A.SUBSDR_CD = 'EEUK'
      AND    A.DIV_CD = 'GLT'
      GROUP BY A.DIV_CD
              ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 4. 한계적자금액/한계이익
      SELECT A.DIV_CD AS DIV_CD
            ,CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자금액' END AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY           
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-10_-5'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15_-10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-5_0'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_0_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND '201510'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = 'EEUK'
      AND    A.DIV_CD = 'GLT'
      GROUP BY A.DIV_CD
              ,CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자금액' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 5. 한계적자매출
      SELECT A.DIV_CD AS DIV_CD
            ,'한계'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자매출'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND '201510'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = 'EEUK'
      AND    A.DIV_CD = 'GLT'
      GROUP BY A.DIV_CD
              ,'한계'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD
