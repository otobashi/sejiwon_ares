
INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
(
       prcs_seq         
      ,rs_module_cd     
      ,rs_clsf_id       
      ,rs_type_cd       
      ,rs_type_name     
      ,div_cd           
      ,base_yyyymmdd    
      ,cd_desc          
      ,sort_seq         
      ,use_flag         
      ,attribute1_value 
      ,attribute2_value 
      ,attribute3_value 
      ,attribute4_value 
      ,attribute5_value 
      ,attribute6_value 
      ,attribute7_value 
      ,attribute8_value 
      ,attribute9_value 
      ,attribute10_value 
      ,attribute11_value 
      ,attribute12_value 
      ,attribute13_value 
      ,attribute14_value 
      ,attribute15_value 
      ,attribute16_value 
      ,attribute17_value 
      ,attribute18_value 
      ,attribute19_value 
      ,attribute20_value 
)
SELECT /*+ PARALLEL(8) */
       '1720'                AS prcs_seq                                 
      ,'ARES'                AS rs_module_cd                             
      ,'DSM_KPI_DIV_B2B'     AS rs_clsf_id                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_cd                               
      ,'DSM_KPI_DIV_B2B'     AS rs_type_name                             
      ,SUBSTR(PROD_CD,1,3)   AS div_cd                                   
      ,BASE_YYYYMM           AS base_yyyymmdd                            
      ,NULL                  AS cd_desc                                  
      ,NULL                  AS sort_seq                                 
      ,'Y'                   AS use_flag                                 
      ,CASE SCENARIO_TYPE_CD
            WHEN 'AC0' THEN BASE_YYYYMM
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
WHERE  BASE_YYYYMM BETWEEN '201301' AND '201601'
AND    SCENARIO_TYPE_CD IN ('AC0','PR1','PR2','PR3','PR4')
AND    ldgr_type_cd   = '1' -- 연결 장부 기준 
AND    subsdr_type_cd = 'S' -- 판매법인(제판포함)
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
                   ,'BEP20000B2BP' -- 순매출(프로젝트)
                   )                                   
GROUP BY SUBSTR(PROD_CD,1,3)
        ,BASE_YYYYMM
        ,CASE SCENARIO_TYPE_CD
            WHEN 'AC0' THEN BASE_YYYYMM
            WHEN 'PR1' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM')
            WHEN 'PR2' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),2),'YYYYMM')
            WHEN 'PR3' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),3),'YYYYMM')
            WHEN 'PR4' THEN TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),4),'YYYYMM') END
        ,SCENARIO_TYPE_CD
        ,PROD_CD
        ,SUBSDR_CD
        ,ACCT_CD
        ,DIV_BIZ_TYPE_CD