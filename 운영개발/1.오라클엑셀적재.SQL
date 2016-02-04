
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
      ,DECODE(SCENARIO_TYPE_CD,'PR1',TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM'),BASE_YYYYMM)         AS base_yyyymmdd                            
      ,NULL                  AS cd_desc                                  
      ,NULL                  AS sort_seq                                 
      ,'Y'                   AS use_flag                                 
      ,DECODE(SCENARIO_TYPE_CD,'PR1',TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM'),BASE_YYYYMM)
      ,DECODE(SCENARIO_TYPE_CD,'PR1','MP',SCENARIO_TYPE_CD)
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
AND    SCENARIO_TYPE_CD IN ('AC0','PR1')
AND    ldgr_type_cd   = '1' -- 연결 장부 기준 
AND    subsdr_type_cd = 'S' -- 판매법인(제판포함)
-- AND    SUMM_FLAG = 'N' --MC 제외 여부
AND    ACCT_CD IN ( 'BEP20000000' -- 순매출
                   ,'BEP60000000' -- 영업이익
                   ,'BACKLOGBALAMT' -- 수주잔고
                  )
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
        ,DECODE(SCENARIO_TYPE_CD,'PR1',TO_CHAR(ADD_MONTHS(TO_DATE(BASE_YYYYMM,'YYYYMM'),1),'YYYYMM'),BASE_YYYYMM)
        ,DECODE(SCENARIO_TYPE_CD,'PR1','MP',SCENARIO_TYPE_CD)
        ,PROD_CD
        ,SUBSDR_CD
        ,ACCT_CD
        ,DIV_BIZ_TYPE_CD




