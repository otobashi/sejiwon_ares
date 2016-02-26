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
      ,ATTRIBUTE13_VALUE
      ,ATTRIBUTE14_VALUE
      ,ATTRIBUTE15_VALUE
)
SELECT /*+ PARALLEL(8) */
       '1520'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_ML'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_ML'                                                  AS RS_TYPE_NAME                             
      ,A.DIV_CD                                                        AS DIV_CD                                   
      ,B.PLN_YYYYMM                                                    AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,A.DIV_CD AS CMPNY_CD
      ,A.DIV_CD AS RS_DIV_CD
      ,DECODE(A.ML_ACCT_CAT_CD, 'NSALES', 'SALE', A.ML_ACCT_CAT_CD) AS KPI_CD
      ,B.PLN_YYYYMM AS BASE_YYYYMM
      ,B.WEEK_NO AS BASE_MMWEEK
      ,A.PLN_YYYYMM AS PLN_YYYYMM
      ,CASE WHEN A.PLN_YYYYMM = B.PLN_YYYYMM THEN 'PR0'
            WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
            WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
            WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
            WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 4), 'YYYYMM') THEN 'PR4'
       END AS SCENARIO_TYPE_CD
      ,A.SUBSDR_CD
      ,C.SUBSDR_SHRT_NAME
      ,SUM(A.KRW_AMT) AS CURRM_KRW_AMT
      ,SUM(A.USD_AMT) AS CURRM_USD_AMT
      ,TRUNC(A.CREATION_DATE)
      ,'ARES'
      ,TRUNC(A.LAST_UPD_DATE)
      ,'ARES'
FROM   TB_RFE_ML_UPLD_RSLT_S A
      ,TB_RFE_ML_WEEK_M      B
      ,(
        SELECT DISTINCT S.SUBSDR_SHRT_NAME, S.SUBSDR_CD, S.SUBSDR_KOR_NAME
        FROM TB_CM_SUBSDR_PERIOD_H S
        WHERE S.MGT_TYPE_CD  = 'CM'
        AND   S.ACCTG_YYYYMM = '*'
        AND   S.ACCTG_WEEK   = '*'
        AND   S.TEMP_FLAG    = 'N' ) C
WHERE  B.PLN_YYYYMM   >= '201301' -- 요부분변경
AND    A.DATA_TYPE_CD = 'SUBSDR'
AND    A.PLN_YYYYWEEK = B.PLN_YYYYWEEK
AND    B.PLN_YYYYMM   <= A.PLN_YYYYMM
AND    A.PLN_YYYYMM   < TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 4), 'YYYYMM')
AND    A.KRW_AMT      <> 0
AND    C.SUBSDR_CD    = A.SUBSDR_CD(+)
GROUP BY A.DIV_CD
        ,A.ML_ACCT_CAT_CD
        ,B.PLN_YYYYMM 
        ,B.WEEK_NO
        ,A.PLN_YYYYMM 
        ,CASE WHEN A.PLN_YYYYMM = B.PLN_YYYYMM THEN 'PR0'
              WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
              WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
              WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
              WHEN A.PLN_YYYYMM = TO_CHAR(ADD_MONTHS(TO_DATE(B.PLN_YYYYMM, 'YYYYMM'), 4), 'YYYYMM') THEN 'PR4'
         END 
        ,A.SUBSDR_CD
        ,C.SUBSDR_SHRT_NAME
        ,TRUNC(A.CREATION_DATE)
        ,TRUNC(A.LAST_UPD_DATE)
;