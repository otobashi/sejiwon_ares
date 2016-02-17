SELECT /*+ PARALLEL(8) */
       '1720'                AS prcs_seq                                 
      ,'ARES'                AS rs_module_cd                             
      ,'DSM_KPI_DIV_B2B'     AS rs_clsf_id                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_cd                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_name                             
      ,SUBSTR(PROD_CD,1,3)   AS div_cd                                   
      ,DECODE(SCENARIO_TYPE_CD,'PR1',TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM'),BASE_YYYYMM)         AS base_yyyymmdd                            
      ,NULL                  AS cd_desc                                  
      ,NULL                  AS sort_seq                                 
      ,'Y'                   AS use_flag                                 
      ,CASE SCENARIO_TYPE_CD
            WHEN 'PR1' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM')
            WHEN 'PR2' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),2),'YYYYMM')
            WHEN 'PR3' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),3),'YYYYMM')
            WHEN 'PR4' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),4),'YYYYMM') END
      ,SCENARIO_TYPE_CD
      ,PROD_CD
      ,SUBSDR_CD
      ,ACCT_CD
      ,MIN(LDGR_TYPE_CD)
      ,MIN(SUBSDR_TYPE_CD)
      ,DIV_BIZ_TYPE_CD
      ,MIN(DATA_DELIMT_CD)
      ,MIN(INTRNL_EXTERN_TYPE_CD)
      ,MIN(KPI_TYPE_CD)
      ,MIN(SUMM_FLAG)
      ,MIN(CURRENCY_CD)
      ,SUM(CURRM_LOC_AMT)
      ,SUM(CURRM_KRW_AMT)
      ,SUM(CURRM_USD_AMT)
      ,SUM(ACCUM_LOC_AMT)
      ,SUM(ACCUM_KRW_AMT)
      ,SUM(ACCUM_USD_AMT)
      ,MIN(LOAD_DATE)
FROM   TB_DSM_KPI_DIV_B2B_S
WHERE  BASE_YYYYMM BETWEEN '201301' AND '201512'
AND    SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
AND    ldgr_type_cd   = '1' -- 연결 장부 기준 
AND    subsdr_type_cd = 'S' -- 판매법인(제판포함)
-- AND    SUMM_FLAG = 'N' --MC 제외 여부

AND    ACCT_CD IN ( 'BEP20000B2B'   -- 순매출B2B
                   ,'BEP20000B2BR'  -- 순매출비중
                   ,'BEP60000B2B'   -- 영업이익B2B
                   ,'BEP60000B2B%'  -- 영업이익율B2B
                   ,'BEP20000000'
                   ,'BEP60000000'
                   ,'BACKLOGBALAMT'   -- 수주잔고
                   ,'BBRATIO05W'
                   ,'BBRATIO13W'
                   ,'BBRATIO52W'   -- BBRATIO
                   )
/*
AND    ACCT_CD IN ( 'BEP20000B2B'   -- 순매출B2B
                   ,'BEP20000B2BR'  -- 순매출비중
                   ,'BEP60000B2B'   -- 영업이익B2B
                   ,'BEP60000B2B%'  -- 영업이익율B2B
*/                   
AND    PROD_CD IN ( 'CLT'
                   ,'CNT'
                   ,'CVT'
                   ,'DBT'
                   ,'DFT'
                   ,'DGT'
                   ,'DMT'
                   ,'DVT'
                   ,'GEV'
                   ,'GJT'
                   ,'GLT'
                   ,'GNT_L2_1'
                   ,'GNT_L2_5'
                   ,'GTT'
                   ,'MST'
                   ,'MTT'
                   ,'PCT'
                   ,'PDT'
                   ,'PDT'
                   ,'PGT'
                   ,'PHT'
                   ,'PJT'
                   ,'PKT'
                   ,'PNT'
                   ,'PNT_L3_1'
                   ,'PWV'
                   ,'SDT'
                   ,'SRT'
                   ,'VHT'
                   ,'VQT'
                   ,'ALL' )                                    
GROUP BY SUBSTR(PROD_CD,1,3)
        ,CASE SCENARIO_TYPE_CD
            WHEN 'PR1' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM')
            WHEN 'PR2' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),2),'YYYYMM')
            WHEN 'PR3' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),3),'YYYYMM')
            WHEN 'PR4' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),4),'YYYYMM') END
        ,SCENARIO_TYPE_CD
        ,PROD_CD
        ,SUBSDR_CD
        ,ACCT_CD
        ,DIV_BIZ_TYPE_CD
UNION ALL
SELECT '1720'                AS prcs_seq                                 
      ,'ARES'                AS rs_module_cd                             
      ,'DSM_KPI_DIV_B2B'     AS rs_clsf_id                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_cd                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_name                             
      ,SUBSTR(PROD_CD,1,3)   AS div_cd                                   
      ,DECODE(SCENARIO_TYPE_CD,'PR1',TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM'),BASE_YYYYMM)         AS base_yyyymmdd                            
      ,NULL                  AS cd_desc                                  
      ,NULL                  AS sort_seq                                 
      ,'Y'                   AS use_flag                                 
      ,CASE SCENARIO_TYPE_CD
            WHEN 'PR1' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM')
            WHEN 'PR2' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),2),'YYYYMM')
            WHEN 'PR3' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),3),'YYYYMM')
            WHEN 'PR4' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),4),'YYYYMM') END
      ,SCENARIO_TYPE_CD
      ,PROD_CD
      ,SUBSDR_CD
      ,ACCT_CD
      ,MIN(LDGR_TYPE_CD)
      ,MIN(SUBSDR_TYPE_CD)
      ,DIV_BIZ_TYPE_CD
      ,MIN(DATA_DELIMT_CD)
      ,MIN(INTRNL_EXTERN_TYPE_CD)
      ,MIN(KPI_TYPE_CD)
      ,MIN(SUMM_FLAG)
      ,MIN(CURRENCY_CD)
      ,SUM(CURRM_LOC_AMT)
      ,SUM(CURRM_KRW_AMT)
      ,SUM(CURRM_USD_AMT)
      ,SUM(ACCUM_LOC_AMT)
      ,SUM(ACCUM_KRW_AMT)
      ,SUM(ACCUM_USD_AMT)
      ,MIN(LOAD_DATE)
FROM   TB_DSM_KPI_DIV_B2B_S
WHERE  BASE_YYYYMM BETWEEN '201301' AND '201512'
AND    SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
AND    ldgr_type_cd   = '1' -- 연결 장부 기준 
AND    subsdr_type_cd = 'S' -- 판매법인(제판포함)
-- AND    SUMM_FLAG = 'N' --MC 제외 여부

AND    ACCT_CD IN ( 'BACKLOGBALAMT'   -- 수주잔고
                   ,'BBRATIO05W'
                   ,'BBRATIO13W'
                   ,'BBRATIO52W'   -- BBRATIO
                  )
AND    PROD_CD IN ( 'DMT'
                   ,'DTT'
                   ,'GJT'
                   ,'GNT'
                   ,'PGT'
                   ,'SDT'
                   ,'CLT'
                   ,'ALL' )                                    
GROUP BY SUBSTR(PROD_CD,1,3)
        ,CASE SCENARIO_TYPE_CD
            WHEN 'PR1' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM')
            WHEN 'PR2' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),2),'YYYYMM')
            WHEN 'PR3' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),3),'YYYYMM')
            WHEN 'PR4' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),4),'YYYYMM') END
        ,SCENARIO_TYPE_CD
        ,PROD_CD
        ,SUBSDR_CD
        ,ACCT_CD
        ,DIV_BIZ_TYPE_CD

