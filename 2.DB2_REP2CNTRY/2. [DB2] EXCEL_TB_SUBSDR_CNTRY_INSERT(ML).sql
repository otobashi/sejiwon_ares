INSERT INTO IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
(
   BASE_YYYYMM
  ,SCENARIO_TYPE_CD
  ,DIV_CD
  ,SUBSDR_CD
  ,AU_CD
  ,MANUAL_ADJ_FLAG
  ,KPI_CD
  ,CAT_CD
  ,SUB_CAT_CD
  ,ZONE_RNR_CD
  ,SUBSDR_RNR_CD
  ,CNTRY_CD
  ,APPLY_YYYYMM
  ,CURRM_KRW_AMT
  ,CURRM_USD_AMT
--  ,ACCU_KRW_AMT
--  ,ACCU_USD_AMT
  ,ATTRIBUTE1_VALUE
  ,ATTRIBUTE2_VALUE
--  ,ATTRIBUTE3_VALUE
--  ,ATTRIBUTE4_VALUE
--  ,ATTRIBUTE5_VALUE
  ,CREATION_DATE
  ,CREATION_USR_ID
  ,LAST_UPD_DATE
  ,LAST_UPD_USR_ID
)
SELECT ATTRIBUTE4          
      ,ATTRIBUTE7          
      ,ATTRIBUTE2          
      ,ATTRIBUTE8          
      ,'*'                 
      ,'N'                 
      ,ATTRIBUTE3          
      ,'BEP_SMART_ML'      
      ,ATTRIBUTE9          
      ,ATTRIBUTE5          
      ,ATTRIBUTE8          
      ,'*'                 
      ,ATTRIBUTE6          
      ,SUM(TO_NUMBER(ATTRIBUTE10))    
      ,SUM(TO_NUMBER(ATTRIBUTE11))    
--      ,null                
--      ,null                
      ,ATTRIBUTE9          
      ,ATTRIBUTE5          
--      ,null                
--      ,null                
--      ,null                
      ,CURRENT TIMESTAMP   
      ,'ares'              
      ,CURRENT TIMESTAMP   
      ,'ares'              
FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
WHERE  SEQ = '1520'
AND    CODE_ID = 'BEP_SMART_ML'
AND    A.YYYYMMDD = '201301'
GROUP BY ATTRIBUTE4          
        ,ATTRIBUTE7          
        ,ATTRIBUTE2          
        ,ATTRIBUTE8          
        ,ATTRIBUTE3          
        ,ATTRIBUTE6          
        ,ATTRIBUTE9          
        ,ATTRIBUTE5 
UNION ALL
-- 이동계획추가
SELECT ATTRIBUTE6          
      ,'MP'          
      ,ATTRIBUTE2          
      ,ATTRIBUTE8          
      ,'*'                 
      ,'N'                 
      ,ATTRIBUTE3          
      ,'BEP_SMART_ML'      
      ,ATTRIBUTE9          
      ,ATTRIBUTE5          
      ,ATTRIBUTE8          
      ,'*'                 
      ,ATTRIBUTE6          
      ,SUM(TO_NUMBER(ATTRIBUTE10))    
      ,SUM(TO_NUMBER(ATTRIBUTE11))    
--      ,null                
--      ,null                
      ,ATTRIBUTE9          
      ,ATTRIBUTE5          
--      ,null                
--      ,null                
--      ,null                
      ,CURRENT TIMESTAMP   
      ,'ares'              
      ,CURRENT TIMESTAMP   
      ,'ares'              
FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
WHERE  SEQ = '1520'
AND    ATTRIBUTE7 = 'PR1'
AND    CODE_ID = 'BEP_SMART_ML'
AND    A.YYYYMMDD = '201301'
GROUP BY ATTRIBUTE4          
        ,ATTRIBUTE7          
        ,ATTRIBUTE2          
        ,ATTRIBUTE8          
        ,ATTRIBUTE3          
        ,ATTRIBUTE6          
        ,ATTRIBUTE9          
        ,ATTRIBUTE5                 
WITH UR;

/*
SQL(1)          : 62716개의 행이 Insert 되었습니다. SQL 소요시간 (3.583)


모든 SQL 실행이 완료되었습니다.
전체 SQL	: 1   SQL 소요시간( 3.583 )

SQL 완료	: 1

SQL(1)          : 284480개의 행이 Insert 되었습니다. SQL 소요시간 (7.026)


모든 SQL 실행이 완료되었습니다.
전체 SQL	: 1   SQL 소요시간( 7.026 )

SQL 완료	: 1
*/