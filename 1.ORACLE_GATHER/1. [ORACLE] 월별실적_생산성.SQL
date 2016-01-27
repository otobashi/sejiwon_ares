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
)
SELECT '1620'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_HR'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_HR'                                                  AS RS_TYPE_NAME                             
      ,A.DIV_CD                                                        AS DIV_CD                                   
      ,A.BASE_YYYYMM                                                   AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,A.BASE_YYYYMM
      ,A.SCENARIO_TYPE_CD
      ,A.KPI_TYPE_CD
      ,A.DIV_CD
      ,A.SUBSDR_CD
      ,A.ACCT_CD
      ,B.ACCT_NM
      ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
      ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
      ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
      ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
FROM   TB_DSM_KPI_DIV_S A
      ,(
        SELECT ACCT_CD, ACCT_DESC AS ACCT_NM
        FROM   TB_DSD_ACCT_M
        WHERE  ACCT_GR_CD = 'HR'
        AND    ACCT_DESC IN ('(HR) 원당매출액','(HR) 원당매출액_사내도급제외','(HR) 인당매출액','(HR) 인당매출액_사내도급제외','(HR) 인원수','(HR) 인원수_사내도급제외'
                          ,'(HR) 인원수_FSE','(HR) 인원수_ISE ','(HR) 인원수_임시직 ','(HR) 인원수_사내도급 ','(HR) 인원수_ISE_영업/마케팅','(HR) 인원수_공통인원'
                          ,'(HR) 인건비(HR생산성 기준)','(HR) 인건비_사내도급제외')
      ) B            
WHERE  A.BASE_YYYYMM BETWEEN '201301' AND '201601'
AND    A.ACCT_CD = B.ACCT_CD
AND    A.SUBSDR_CD = 'EEUK'
AND    A.DIV_CD IN ('GLT','GTT','MST','CNT','DFT','DGT','DMT','CMS')
AND    A.CURRENCY_CD = '*'
AND    A.SCENARIO_TYPE_CD = 'AC0'
AND    A.KPI_TYPE_CD IN ('HR', 'HR_CNT')
GROUP BY A.BASE_YYYYMM
      ,A.SCENARIO_TYPE_CD
      ,A.KPI_TYPE_CD
      ,A.DIV_CD
      ,A.SUBSDR_CD
      ,A.ACCT_CD
      ,B.ACCT_NM
;
