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
--  ,ATTRIBUTE2_VALUE
--  ,ATTRIBUTE3_VALUE
--  ,ATTRIBUTE4_VALUE
--  ,ATTRIBUTE5_VALUE
  ,CREATION_DATE
  ,CREATION_USR_ID
  ,LAST_UPD_DATE
  ,LAST_UPD_USR_ID
)
SELECT ATTRIBUTE9
      ,'AC0'
      ,ATTRIBUTE6
      ,ATTRIBUTE1
      ,'*'
      ,'N'
      ,'SALE_PRJ'
      ,'BEP_SMART_PIPE'
      ,'*'
      ,'*'
      ,'*'
      ,'*'
      ,ATTRIBUTE9
      ,SUM(TO_NUMBER(ATTRIBUTE11))
      ,SUM(TO_NUMBER(ATTRIBUTE10))
      ,MIN(ATTRIBUTE2)
      ,CURRENT TIMESTAMP   
      ,'ares'              
      ,CURRENT TIMESTAMP   
      ,'ares'                 
FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
WHERE  SEQ = '1530'
AND    CODE_ID = 'BEP_SMART_PIPE'
GROUP BY ATTRIBUTE9          
        ,ATTRIBUTE6          
        ,ATTRIBUTE1          
UNION ALL
SELECT ATTRIBUTE9
      ,'AC0'
      ,ATTRIBUTE6
      ,ATTRIBUTE1
      ,'*'
      ,'N'
      ,'BLSN'
      ,'BEP_SMART_PIPE'
      ,'*'
      ,'*'
      ,'*'
      ,'*'
      ,ATTRIBUTE9
      ,SUM(TO_NUMBER(ATTRIBUTE13))
      ,SUM(TO_NUMBER(ATTRIBUTE12))
      ,MIN(ATTRIBUTE2)
      ,CURRENT TIMESTAMP   
      ,'ares'              
      ,CURRENT TIMESTAMP   
      ,'ares'                 
FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
WHERE  SEQ = '1530'
AND    CODE_ID = 'BEP_SMART_PIPE'
GROUP BY ATTRIBUTE9          
        ,ATTRIBUTE6          
        ,ATTRIBUTE1          
WITH UR;

/*
SQL(1)          : 32228개의 행이 Insert 되었습니다. SQL 소요시간 (1.149)


모든 SQL 실행이 완료되었습니다.
전체 SQL	: 1   SQL 소요시간( 1.149 )

SQL 완료	: 1

*/