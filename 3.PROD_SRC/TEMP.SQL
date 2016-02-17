      SELECT B.KOR_NM             AS COL_INDEX
            ,CASE WHEN C.SEQ = 1 THEN 'ALL'
                  WHEN C.SEQ = 2 THEN D.DISP_KOR_NM END AS NATION_NM
            ,CASE WHEN A.ACCT_CD = 'BACKLOGBALAMT' THEN '수주잔고'
                  WHEN A.ACCT_CD = 'BEP20000B2B'   THEN '매출(전체)'
                  WHEN A.ACCT_CD = 'BEP20000B2BP'  THEN '매출(프로젝트)' END AS KPI_CD
            ,A.BASE_YYYYMM        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,MIN(B.DISP_SEQ)      AS DISP_SEQ
            ,MIN(B.ENG_NM)        AS COL_INDEX_ENG
      FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
            ,(
              SELECT CASE DIVISION_CODE
                          WHEN 'GNTHT' THEN 'GNT_L2_5'
                          WHEN 'GNTCS' THEN 'GNT_L2_1'
                          ELSE DIVISION_CODE END AS DIV_CD
                    ,DISPLAY_NAME1 AS KOR_NM
                    ,DISPLAY_NAME2 AS ENG_NM
                    ,DISPLAY_ORDER_SEQ AS DISP_SEQ
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
              WHERE  CODE_TYPE = 'PIPELINE'
              AND    USE_FLAG = 'Y'
              AND    DIVISION_CODE = P_DIV_CD
             ) B
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) C
            ,(SELECT A.CORPORATION_CODE AS CORP_CD
                    ,B.DISPLAY_NAME2 AS DISP_ENG_NM
                    ,B.DISPLAY_NAME1 AS DISP_KOR_NM
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ     
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                    ,IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER B
              WHERE  A.CODE_TYPE = 'SMART_CORP_NATION'        
              AND    B.CODE_TYPE = 'SMART_CNTRY_NAME'
              AND    A.DIVISION_CODE = B.DIVISION_CODE) D
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = D.CORP_CD
      AND    A.ACCT_CD in ( 'BACKLOGBALAMT','BEP20000B2B','BEP20000B2BP')
      AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
      AND    A.PROD_CD = B.DIV_CD
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      GROUP BY B.KOR_NM
              ,CASE WHEN C.SEQ = 1 THEN 'ALL'
                    WHEN C.SEQ = 2 THEN D.DISP_KOR_NM END 
              ,CASE WHEN A.ACCT_CD = 'BACKLOGBALAMT' THEN '수주잔고'
                        WHEN A.ACCT_CD = 'BEP20000B2B'   THEN '매출(전체)'
                        WHEN A.ACCT_CD = 'BEP20000B2BP'  THEN '매출(프로젝트)' END
              ,A.BASE_YYYYMM