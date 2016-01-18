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
)
SELECT '1530'               AS PRCS_SEQ                                 
      ,'ARES'               AS RS_MODULE_CD                             
      ,'BEP_SMART'          AS RS_CLSF_ID                               
      ,'BEP_SMART_PIPE'     AS RS_TYPE_CD                               
      ,'BEP_SMART_PIPE'     AS RS_TYPE_NAME                             
      ,A11.DIV_CD           AS DIV_CD                                   
      ,A11.BASE_YYYYMM      AS BASE_YYYYMMDD                            
      ,NULL                 AS CD_DESC                                  
      ,A14.SORT_ORDER       AS SORT_SEQ                                 
      ,'Y'                  AS USE_FLAG                                 
      ,A11.SUBSDR_CD        AS SUBSDR_CD
      ,A14.SUBSDR_SHRT_NAME AS NEW_SUBSDR_SHRT_NAME
      ,A14.SORT_ORDER       AS SORT1_ORDER
      ,A11.MGT_ORG_CD       AS MGT_SUBSDR_CD
      ,A13.MGT_SUBSDR_NAME  AS MGT_SUBSDR_NAME
      ,A11.DIV_CD           AS DIV_CD
      ,A12.DIV_KOR_NAME     AS DIV_KOR_NAME
      ,A12.DIV_SHRT_NAME    AS DIV_SHRT_NAME
      ,A11.BASE_YYYYMM  BASE_YYYYMM
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'USD',A11.CHG_AMT)) AS SALES_USD_AMOUNT   --SALES AMT(매출-프로젝트)
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'KRW',A11.CHG_AMT)) AS SALES_KRW_AMOUNT
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'USD',A11.BAL_AMT)) AS BACKLOG_USD_AMOUNT --BACKLOG AMOUNT(수주잔고)
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'KRW',A11.BAL_AMT)) AS BACKLOG_KRW_AMOUNT
FROM   NPT_APP.NV_DWW_B2B_BAL_H  A11
       LEFT OUTER JOIN  NPT_APP.NV_DWD_DIV_LEAF_M  A12
       ON (A11.DIV_CD = A12.DIV_CD)
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_MGT_M  A13
       ON (A11.MGT_ORG_CD = A13.MGT_SUBSDR_CD)
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_M  A14
       ON (A11.SUBSDR_CD = A14.SUBSDR_CD)
WHERE (A11.BASE_YYYYMM BETWEEN '201201' AND '201601' -- 요부분변경
AND    A11.PJT_STG_CD IN ('A'))
GROUP BY A11.SUBSDR_CD
        ,A14.SUBSDR_SHRT_NAME
        ,A14.SORT_ORDER
        ,A11.MGT_ORG_CD
        ,A13.MGT_SUBSDR_NAME
        ,A11.DIV_CD
        ,A12.DIV_KOR_NAME
        ,A12.DIV_SHRT_NAME
        ,A11.BASE_YYYYMM
UNION ALL
SELECT '1530'               AS PRCS_SEQ                                 
      ,'ARES'               AS RS_MODULE_CD                             
      ,'BEP_SMART'          AS RS_CLSF_ID                               
      ,'BEP_SMART_PIPE'     AS RS_TYPE_CD                               
      ,'BEP_SMART_PIPE'     AS RS_TYPE_NAME                             
      ,DECODE(SUBSTR(A11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS DIV_CD                                   
      ,A11.BASE_YYYYMM      AS BASE_YYYYMMDD                            
      ,NULL                 AS CD_DESC                                  
      ,A14.SORT_ORDER       AS SORT_SEQ                                 
      ,'Y'                  AS USE_FLAG                                 
      ,A11.SUBSDR_CD        AS SUBSDR_CD
      ,A14.SUBSDR_SHRT_NAME AS NEW_SUBSDR_SHRT_NAME
      ,A14.SORT_ORDER       AS SORT1_ORDER
      ,A11.MGT_ORG_CD       AS MGT_SUBSDR_CD
      ,A13.MGT_SUBSDR_NAME  AS MGT_SUBSDR_NAME
      ,DECODE(SUBSTR(A11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS DIV_CD
      ,A12.DIV_KOR_NAME     AS DIV_KOR_NAME
      ,A12.DIV_SHRT_NAME    AS DIV_SHRT_NAME
      ,A11.BASE_YYYYMM  BASE_YYYYMM
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'USD',A11.CHG_AMT)) AS SALES_USD_AMOUNT   --SALES AMT(매출-프로젝트)
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'KRW',A11.CHG_AMT)) AS SALES_KRW_AMOUNT
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'USD',A11.BAL_AMT)) AS BACKLOG_USD_AMOUNT --BACKLOG AMOUNT(수주잔고)
      ,SUM(DECODE(A11.CURRENCY_TYPE_CD,'KRW',A11.BAL_AMT)) AS BACKLOG_KRW_AMOUNT
FROM   NPT_APP.NV_DWW_B2B_BAL_H  A11
       LEFT OUTER JOIN  NPT_APP.NV_DWD_DIV_LEAF_M  A12
       ON (A11.DIV_CD = A12.DIV_CD)
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_MGT_M  A13
       ON (A11.MGT_ORG_CD = A13.MGT_SUBSDR_CD)
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_M  A14
       ON (A11.SUBSDR_CD = A14.SUBSDR_CD)
WHERE (A11.BASE_YYYYMM BETWEEN '201201' AND '201601' -- 요부분변경
AND    A11.div_cd = 'GNT'
AND    SUBSTR(A11.prod_lvl3_cd,1,2) IN ('CS','HT')
AND    SUBSTR(A11.prod_lvl4_cd,1,2) IN ('CS','HT')
AND    A11.mdl_sffx_cd = '*'
AND    A11.PJT_STG_CD IN ('A'))
GROUP BY A11.SUBSDR_CD
        ,A14.SUBSDR_SHRT_NAME
        ,A14.SORT_ORDER
        ,A11.MGT_ORG_CD
        ,A13.MGT_SUBSDR_NAME
        ,DECODE(SUBSTR(A11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT')
        ,A12.DIV_KOR_NAME
        ,A12.DIV_SHRT_NAME
        ,A11.BASE_YYYYMM
;
