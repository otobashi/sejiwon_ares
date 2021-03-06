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
,ACCU_KRW_AMT
,ACCU_USD_AMT
,ATTRIBUTE1_VALUE
,ATTRIBUTE2_VALUE
,ATTRIBUTE3_VALUE
,ATTRIBUTE4_VALUE
,ATTRIBUTE5_VALUE
,CREATION_DATE
,CREATION_USR_ID
,LAST_UPD_DATE
,LAST_UPD_USR_ID
)
SELECT 
B.YYYYMM BASE_YYYYMM
,A.SCENARIO_TYPE_CD
,A.DIV_CD
,A.SUBSDR_CD
,A.AU_CD
,A.MANUAL_ADJ_FLAG
,A.KPI_CD
,A.CAT_CD
,A.SUB_CAT_CD
,A.ZONE_RNR_CD
,A.SUBSDR_RNR_CD
,A.CNTRY_CD
,A.APPLY_YYYYMM
,A.CURRM_KRW_AMT
,A.CURRM_USD_AMT
,A.ACCU_KRW_AMT
,A.ACCU_USD_AMT
,A.ATTRIBUTE1_VALUE
,A.ATTRIBUTE2_VALUE
,A.ATTRIBUTE3_VALUE
,A.ATTRIBUTE4_VALUE
,A.ATTRIBUTE5_VALUE
,A.CREATION_DATE
,A.CREATION_USR_ID
,A.LAST_UPD_DATE
,A.LAST_UPD_USR_ID
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
,(
SELECT '201401' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201402' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201403' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201404' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201405' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201406' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201407' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201408' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201409' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201410' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201411' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201412' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201501' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201502' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201503' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201504' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201505' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201506' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201507' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201508' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201509' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 UNION ALL
SELECT '201510' AS YYYYMM FROM   SYSIBM.SYSDUMMY1 
) B
WHERE A.CAT_CD = 'BEP_SMART_ML'
AND   A.BASE_YYYYMM = '201511'
WITH UR
;

/*
SQL(1)          : 594220개의 행이 Insert 되었습니다. SQL 소요시간 (7.049)


모든 SQL 실행이 완료되었습니다.
전체 SQL	: 1   SQL 소요시간( 7.049 )

SQL 완료	: 1
*/