INSERT INTO NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
(
       PRCS_SEQ         
      ,RS_MODULE_CD     
      ,RS_CLSF_ID       
      ,RS_TYPE_CD       
      ,RS_TYPE_NAME     
      ,DIV_CD           
      ,BASE_YYYYMMDD    
      ,CD_DESC          
      ,SORT_SEQ         
      ,USE_FLAG         
      ,ATTRIBUTE1_VALUE 
      ,ATTRIBUTE2_VALUE 
      ,ATTRIBUTE3_VALUE 
      ,ATTRIBUTE4_VALUE 
      ,ATTRIBUTE5_VALUE 
      ,ATTRIBUTE6_VALUE 
      ,ATTRIBUTE7_VALUE 
      ,ATTRIBUTE8_VALUE 
      ,ATTRIBUTE9_VALUE 
      ,ATTRIBUTE10_VALUE
      ,ATTRIBUTE11_VALUE
      ,ATTRIBUTE12_VALUE
)
SELECT /*+ PARALLEL(8) */
       '1800'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_MOSTLIKELY'                                          AS RS_TYPE_CD                               
      ,'BEP_SMART_MOSTLIKELY'                                          AS RS_TYPE_NAME                             
      ,bep.div_cd                                                      AS DIV_CD                                   
      ,TO_CHAR(TO_DATE(wk.start_yyyymmdd, 'YYYYMMDD'), 'W')            AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,wk.start_yyyymmdd AS ATTRIBUTE1_VALUE
      ,wk.end_yyyymmdd   AS ATTRIBUTE2_VALUE
      ,substr(wk.start_yyyymmdd,1,6) as ATTRIBUTE3_VALUE --base_yyyymm
      ,bep.pln_yyyymm AS ATTRIBUTE4_VALUE -- APPLY_YYYYMM
      ,TO_CHAR(TO_DATE(wk.start_yyyymmdd, 'YYYYMMDD'), 'W') AS ATTRIBUTE5_VALUE
      ,case bep.pln_yyyymm
            when substr(wk.start_yyyymmdd,1,6) then 'PR0'
            when to_char(add_months(to_date(substr(wk.start_yyyymmdd,1,6),'yyyymm'),1),'yyyymm') then 'PR1' 
            when to_char(add_months(to_date(substr(wk.start_yyyymmdd,1,6),'yyyymm'),2),'yyyymm') then 'PR2' 
            when to_char(add_months(to_date(substr(wk.start_yyyymmdd,1,6),'yyyymm'),3),'yyyymm') then 'PR3' 
            when to_char(add_months(to_date(substr(wk.start_yyyymmdd,1,6),'yyyymm'),4),'yyyymm') then 'PR4' 
            when to_char(add_months(to_date(substr(wk.start_yyyymmdd,1,6),'yyyymm'),5),'yyyymm') then 'PR5' 
            end AS ATTRIBUTE6_VALUE -- SCENARIO_TYPE_CD
      ,bep.pln_yyyyweek AS ATTRIBUTE7_VALUE
      ,bep.au_cd AS ATTRIBUTE8_VALUE
      ,bep.subsdr_cd AS ATTRIBUTE9_VALUE
      ,bep.div_cd AS ATTRIBUTE10_VALUE
      ,bep.ml_acct_cat_cd AS ATTRIBUTE11_VALUE
      ,bep.usd_amt AS ATTRIBUTE12_VALUE     
FROM   tb_rfe_ml_div_bep_s  bep
      ,tb_cm_week_m wk
WHERE  bep.pln_yyyyweek = replace(base_yyyyweek,'W','')

; 

-- 544151  21.622 SEC