INSERT INTO IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
(
 CODE_TYPE
,DIVISION_CODE
,CORPORATION_CODE
,DISPLAY_NAME1
,DISPLAY_NAME2
,DISPLAY_ORDER_SEQ
,USE_FLAG
,ATTRIBUTE1
,ATTRIBUTE2
)
SELECT 'B2B_ETC_B2C' AS CODE_TYPE
      ,SUBSTR(ATTRIBUTE1,1,8) AS DIV_CD
      ,'B2B_B2B' AS CORP_CD
      ,DISPLAY_NAME1 AS KOR_NM
      ,DISPLAY_NAME2 AS ENG_NM
      ,0
      ,'Y'
      ,'PROD' AS PD_TYPE
      ,ATTRIBUTE1 AS DIV_CD
FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
WHERE  CODE_TYPE = 'SMR_PROD_MST'
AND    ATTRIBUTE1 IN ('GNT_L2_1' -- TV사이니지
                     ,'GNT_L2_5' -- HOTEL TV
                     )
UNION ALL

SELECT 'B2B_ETC_B2C' AS CODE_TYPE
      ,A.DIVISION_CODE AS DIV_CD
      ,'B2B_B2B' AS CORP_CD
      ,A.DISPLAY_NAME AS KOR_NM
      ,A.DIVISION_NAME AS ENG_NM
      ,0
      ,'Y'
      ,'DIV' AS PD_TYPE
      ,A.DIVISION_CODE AS DIV_CD
FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
WHERE  A.BASIS_YYYYMM = '201601'
AND    A.DIVISION_CODE IN ('GJT' -- 모니터사이니지
                          ,'DMT' -- CAC
                          ,'DBT' -- Chiller
                          ,'SDT' -- Solar
                          ,'CLT' -- Lighting
                          ,'SRT' -- EMS
                          )

UNION ALL

SELECT 'B2B_ETC_B2C' AS CODE_TYPE
      ,A.DIVISION_CODE AS DIV_CD
      ,'B2B_ETC' AS CORP_CD
      ,A.DISPLAY_NAME AS KOR_NM
      ,A.DIVISION_NAME AS ENG_NM
      ,0
      ,'Y'
      ,'DIV' AS PD_TYPE
      ,A.DIVISION_CODE AS DIV_CD
FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
WHERE  A.BASIS_YYYYMM = '201601'
AND    A.DIVISION_CODE IN ('PKT' -- Security
                          ,'PGT' -- IVI
                          ,'VHT' -- VE
                          ,'VQT' -- e-PT
                          ,'GEV' -- C&M(외판)
                          ,'MTT' -- CEM(외판)
                          ,'PWV' -- 수처리
                          ,'PDT' -- ....생산기술원(외판)은 뭔지 모름.
                          ,'PCT'
                          )

UNION ALL

SELECT 'B2B_ETC_B2C' AS CODE_TYPE
      ,SUBSTR(ATTRIBUTE1,1,8) AS PROD_CD
      ,'B2B_ETC' AS CORP_CD
      ,DISPLAY_NAME1 AS KOR_NM
      ,DISPLAY_NAME2 AS ENG_NM
      ,0
      ,'Y'
      ,'PROD' AS PD_TYPE
      ,ATTRIBUTE1 AS PROD_CD
FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
WHERE  CODE_TYPE = 'SMR_PROD_MST'
AND    ATTRIBUTE1 IN ('PNT_L3_16' -- '' -- 셋탑박스 PNT_L2_8(스마트박스)
                     )

UNION ALL

SELECT 'B2B_ETC_B2C' AS CODE_TYPE
      ,A.DIVISION_CODE AS DIV_CD
      ,'B2C' AS CORP_CD
      ,A.DISPLAY_NAME AS KOR_NM
      ,A.DIVISION_NAME AS ENG_NM
      ,0
      ,'Y'
      ,'DIV' AS PD_TYPE
      ,A.DIVISION_CODE AS DIV_CD
FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
WHERE  A.BASIS_YYYYMM = '201601'
AND    A.DIVISION_CODE IN ('GLT' -- LCD TV
                          ,'PJT' -- 프로젝터
                          ,'GTT' -- 모니터
                          ,'PNT' -- AV
                          ,'PHT' -- 포켓포토
                          ,'MST' -- 단말
                          ,'CNT' -- 냉장고
                          ,'DFT' -- 세탁기
                          ,'DVT' -- 청소기
                          ,'CVT' -- 쿠킹
                          ,'DGT' -- RAC
                          )

;
-- 수주잔고

INSERT INTO IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
(
 CODE_TYPE
,DIVISION_CODE
,CORPORATION_CODE
,DISPLAY_NAME1
,DISPLAY_NAME2
,DISPLAY_ORDER_SEQ
,USE_FLAG
,ATTRIBUTE1
,ATTRIBUTE2
)
SELECT 'B2B_BACKLOG' AS CODE_TYPE
      ,A.DIVISION_CODE AS DIV_CD
      ,'*' AS CORP_CD
      ,A.DISPLAY_NAME AS KOR_NM
      ,A.DIVISION_NAME AS ENG_NM
      ,0
      ,'Y'
      ,'DIV' AS PD_TYPE
      ,A.DIVISION_CODE AS DIV_CD
FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
WHERE  A.BASIS_YYYYMM = '201601'
AND    A.DIVISION_CODE IN ( 'DMT'
                   ,'DTT'
                   ,'GJT'
                   ,'GNT'
                   ,'PGT'
                   ,'SDT'
                   ,'CLT'
                   ,'ALL' )                       