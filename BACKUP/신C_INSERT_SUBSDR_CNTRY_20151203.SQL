
SELECT *
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM = '201406'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
AND   SCENARIO_TYPE_CD <> 'AC0'
;

DELETE
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM = '201510'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
--AND   SCENARIO_TYPE_CD <> 'AC0'
;

/*
INSERT INTO IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
(
BASE_YYYYMM	,
SCENARIO_TYPE_CD	,
DIV_CD	,
SUBSDR_CD	,
AU_CD	,
MANUAL_ADJ_FLAG	,
KPI_CD	,
CAT_CD	,
SUB_CAT_CD	,
ZONE_RNR_CD	,
SUBSDR_RNR_CD	,
CNTRY_CD	,
APPLY_YYYYMM	,
CURRM_KRW_AMT	,
CURRM_USD_AMT	,
ACCU_KRW_AMT	,
ACCU_USD_AMT	,
ATTRIBUTE1_VALUE	,
ATTRIBUTE2_VALUE	,
ATTRIBUTE3_VALUE	,
ATTRIBUTE4_VALUE	,
ATTRIBUTE5_VALUE	,
CREATION_DATE	,
CREATION_USR_ID	,
LAST_UPD_DATE	,
LAST_UPD_USR_ID	
)




WITH V_BEP_SMART AS 
       (SELECT SEQ AS SEQ ,
              MODULE_CODE AS MODULE_CODE ,
              CODE_TYPE AS CODE_TYPE ,
              CODE_ID AS CODE_ID ,
              CODE_NAME AS CODE_NAME ,
              DIVISION_CODE AS DIVISION_CODE ,
              YYYYMMDD AS YYYYMMDD ,
              DESCRIPTION AS DESCRIPTION ,
              DISPLAY_ORDER_SEQ AS DISPLAY_ORDER_SEQ ,
              USE_FLAG AS USE_FLAG ,
              CREATION_DATE AS CREATION_DATE ,
              CREATE_USER_ID AS CREATE_USER_ID ,
              CREATE_USER_NAME AS CREATE_USER_NAME ,
              ATTRIBUTE1 AS SCENARIO_TYPE_CD ,
              ATTRIBUTE2 AS SCENARIO_TYPE_NAME ,
              ATTRIBUTE3 AS SORT_ORDER ,
              ATTRIBUTE4 AS DIV_CD ,
              ATTRIBUTE5 AS DIV_KOR_NAME ,
              ATTRIBUTE6 AS DIV_SHRT_NAME ,
              ATTRIBUTE7 AS SUBSDR_RNR_CD ,
              ATTRIBUTE8 AS MGT_ORG_SHRT_NAME ,
              ATTRIBUTE9 AS SORT_ORDER0 ,
              ATTRIBUTE10 AS SUBSDR_CD0 ,
              ATTRIBUTE11 AS NEW_SUBSDR_SHRT_NAME ,
              ATTRIBUTE12 AS SORT1_ORDER ,
              ATTRIBUTE13 AS ZONE_CD ,
              ATTRIBUTE14 AS ZONE_NAME ,
              ATTRIBUTE15 AS SCRN_DSPL_SEQ ,
              ATTRIBUTE16 AS CNTRY_RNR_CD ,
              ATTRIBUTE17 AS CNTRY_NAME ,
              ATTRIBUTE18 AS SORT_ORDER1 ,
              ATTRIBUTE19 AS BASE_YYYYMM ,
/*              
              ATTRIBUTE20 AS CURRENCY_CD ,
              ATTRIBUTE21 AS CURRM_ACCUM_TYPE_CD ,
              ATTRIBUTE22 AS SALES_QTY ,
              ATTRIBUTE23 AS NSALES_AMT ,
              ATTRIBUTE24 AS RNR_SALES_AMT ,
              ATTRIBUTE25 AS GROSS_SALES_AMT ,
              ATTRIBUTE26 AS SALES_DEDUCT_AMT ,
              ATTRIBUTE27 AS MGNL_PRF_AMT ,
              ATTRIBUTE28 AS OI_AMT 
*/          
/*    
			ATTRIBUTE20    AS    CURR_NSALES_AMT_KRW  ,
			ATTRIBUTE21    AS    CURR_RNR_SALES_AMT_KRW  ,
			ATTRIBUTE22    AS    CURR_GROSS_SALES_AMT_KRW  ,
			ATTRIBUTE23    AS    CURR_SALES_DEDUCT_AMT_KRW  ,
			ATTRIBUTE24    AS    CURR_MGNL_PRF_AMT_KRW  ,
			ATTRIBUTE25    AS    CURR_OI_AMT_KRW  ,
			ATTRIBUTE26    AS    CURR_NSALES_AMT_USD  ,
			ATTRIBUTE27    AS    CURR_RNR_SALES_AMT_USD  ,
			ATTRIBUTE28    AS    CURR_GROSS_SALES_AMT_USD  ,
			ATTRIBUTE29    AS    CURR_SALES_DEDUCT_AMT_USD  ,
			ATTRIBUTE30    AS    CURR_MGNL_PRF_AMT_USD  ,
			ATTRIBUTE31    AS    CURR_OI_AMT_USD  ,
			ATTRIBUTE32    AS    ACCU_NSALES_AMT_KRW  ,
			ATTRIBUTE33    AS    ACCU_RNR_SALES_AMT_KRW  ,
			ATTRIBUTE34    AS    ACCU_GROSS_SALES_AMT_KRW  ,
			ATTRIBUTE35    AS    ACCU_SALES_DEDUCT_AMT_KRW  ,
			ATTRIBUTE36    AS    ACCU_MGNL_PRF_AMT_KRW  ,
			ATTRIBUTE37    AS    ACCU_OI_AMT_KRW  ,
			ATTRIBUTE38    AS    ACCU_NSALES_AMT_USD  ,
			ATTRIBUTE39    AS    ACCU_RNR_SALES_AMT_USD  ,
			ATTRIBUTE40    AS    ACCU_GROSS_SALES_AMT_USD  ,
			ATTRIBUTE41    AS    ACCU_SALES_DEDUCT_AMT_USD  ,
			ATTRIBUTE42    AS    ACCU_MGNL_PRF_AMT_USD  ,
			ATTRIBUTE43    AS    ACCU_OI_AMT_USD  
              
              
         FROM IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA 
        WHERE SEQ = '1500' 
              AND MODULE_CODE = 'ARES' 
              AND CODE_TYPE = 'BEP_SMART' 
              AND YYYYMMDD BETWEEN '201501' and '201503'
       )  
       
       
       


       
SELECT A.BASE_YYYYMM AS BASE_YYYYMM , 
       A.SCENARIO_TYPE_CD AS SCENARIO_TYPE_CD, 
       A.DIV_CD AS DIV_CD, 
       A.SUBSDR_CD0 AS SUBSDR_CD, 
       '*' AS AU_CD, 
       'N' AS MANUAL_ADJ_FLAG, 
       C2.CODE_ID AS KPI_CD, 
       'BEP_SMART_DIV' AS CAT_CD, 
       '*' AS SUB_CAT_CD, 
       A.ZONE_CD AS ZONE_RNR_CD, 
       A.SUBSDR_RNR_CD AS SUBSDR_RNR_CD, 
       A.CNTRY_RNR_CD AS CNTRY_CD,    
       CASE A.SCENARIO_TYPE_CD 
           WHEN 'AC0' 
           THEN A.BASE_YYYYMM 
           WHEN 'PR1' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 1 MONTHS,'YYYYMM') 
           WHEN 'PR2' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 2 MONTHS,'YYYYMM') 
           WHEN 'PR3' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 3 MONTHS,'YYYYMM') 
           WHEN 'PR4' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 4 MONTHS,'YYYYMM') 
           ELSE A.BASE_YYYYMM 
       END AS APPLY_YYYYMM,  

       -- CURRM 'KRW'
       SUM(
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN RNR_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'COI' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN OI_AMT 
                   ELSE 0 
               END  
           WHEN 'MGN_PROFIT' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN MGNL_PRF_AMT 
                   ELSE 0 
               END 
           WHEN 'GROSS_SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN GROSS_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'SALES_DEDUCTION' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN SALES_DEDUCT_AMT 
                   ELSE 0 
               END 
           ELSE 0                       
       END ) AS CURRM_KRW_AMT,   
       
       
       -- CURRM 'USD'
       SUM (
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN RNR_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'COI' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN OI_AMT 
                   ELSE 0 
               END  
           WHEN 'MGN_PROFIT' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN MGNL_PRF_AMT 
                   ELSE 0 
               END 
           WHEN 'GROSS_SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN GROSS_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'SALES_DEDUCTION' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN SALES_DEDUCT_AMT 
                   ELSE 0 
               END 
           ELSE 0            
       END ) AS CURRM_USD_AMT,  
       

       -- ACCUM 'KRW'
       SUM (
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN RNR_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'COI' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN OI_AMT 
                   ELSE 0 
               END  
           WHEN 'MGN_PROFIT' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN MGNL_PRF_AMT 
                   ELSE 0 
               END 
           WHEN 'GROSS_SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN GROSS_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'SALES_DEDUCTION' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'KRW' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN SALES_DEDUCT_AMT 
                   ELSE 0 
               END 
           ELSE 0           
           
       END ) AS ACCUM_KRW_AMT,   
       -- ACCUM 'USD'
       SUM (
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN RNR_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'COI' 
           THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN OI_AMT 
                   ELSE 0 
               END  
           WHEN 'MGN_PROFIT' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'ACCUM' THEN MGNL_PRF_AMT 
                   ELSE 0 
               END 
           WHEN 'GROSS_SALE' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN GROSS_SALES_AMT 
                   ELSE 0 
               END 
           WHEN 'SALES_DEDUCTION' THEN 
               CASE 
                   WHEN A.CURRENCY_CD = 'USD' AND A.CURRM_ACCUM_TYPE_CD = 'CURRM' THEN SALES_DEDUCT_AMT 
                   ELSE 0 
               END 
           ELSE 0                
       END ) AS ACCUM_USD_AMT,
       
       A.NEW_SUBSDR_SHRT_NAME AS ATTRIBUTE1_VALUE,
       NULL AS ATTRIBUTE2_VALUE,
       NULL AS ATTRIBUTE3_VALUE,
       NULL AS ATTRIBUTE4_VALUE,
       NULL AS ATTRIBUTE5_VALUE,
       current timestamp AS CREATION_DATE,
       'ares' AS CREATION_USR_ID,
       current timestamp AS LAST_UPD_DATE,
       'ares' AS LAST_UPD_USR_ID         
       
  FROM V_BEP_SMART A 
   LEFT OUTER JOIN IPTDW.IPTDW_RES_DIM_CODES C2 
       ON C2.CODE_TYPE = 'KPI_TYPE' 
       AND C2.CODE_ID IN ('SALE','MGN_PROFIT','COI','GROSS_SALE','SALES_DEDUCTION') 
 WHERE A.YYYYMMDD between '201501' and  '201512' 
       AND 1=1  
       
 GROUP BY 
       A.BASE_YYYYMM,
       A.SCENARIO_TYPE_CD, 
       A.DIV_CD , 
       A.SUBSDR_CD0, 
       C2.CODE_ID ,
       A.ZONE_CD , 
       A.SUBSDR_RNR_CD , 
       A.CNTRY_RNR_CD ,    
       CASE A.SCENARIO_TYPE_CD 
           WHEN 'AC0' 
           THEN A.BASE_YYYYMM 
           WHEN 'PR1' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 1 MONTHS,'YYYYMM') 
           WHEN 'PR2' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 2 MONTHS,'YYYYMM') 
           WHEN 'PR3' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 3 MONTHS,'YYYYMM') 
           WHEN 'PR4' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 4 MONTHS,'YYYYMM') 
           ELSE A.BASE_YYYYMM 
       END  ,    
       A.NEW_SUBSDR_SHRT_NAME 
       
WITH UR ; 
*/

GROSS_SALE
SALES_DEDUCTION


SELECT *
FROM IPTDW.IPTDW.IPTDW_RES_DIM_CODES C2 
WHERE C2.CODE_TYPE = 'KPI_TYPE' 
AND   C2.CODE_ID LIKE 'SALE%'




SELECT *
         FROM IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA 
        WHERE SEQ = '1500' 
              AND MODULE_CODE = 'ARES' 
              AND CODE_TYPE = 'BEP_SMART'     
              AND YYYYMMDD = '201503'
              and ATTRIBUTE16 = 'RW'
               BETWEEN '201501' AND '201503'       
               
               
               
/* 2015.12.07 201501 ~ */


/*
DELETE
FROM IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA 
        WHERE SEQ = '1500' 
              AND MODULE_CODE = 'ARES' 
              AND CODE_TYPE = 'BEP_SMART' 
              AND YYYYMMDD BETWEEN '201407' and '201510'
              ;


INSERT INTO IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA 

SELECT *
FROM IPTDW.IPTDW_RES_N2G_EXCEL_UPLOAD_DATA 
        WHERE SEQ = '1500' 
              AND MODULE_CODE = 'ARES' 
              AND CODE_TYPE = 'BEP_SMART' 
              AND YYYYMMDD BETWEEN '201407' and '201510'
WITH UR ;  
*/            




SELECT *
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM BETWEEN '201407' AND '201509'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
--AND   SCENARIO_TYPE_CD <> 'AC0'
;

DELETE
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM BETWEEN '201407' AND '201509'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
--AND   SCENARIO_TYPE_CD <> 'AC0'
;


-- 매출,COI, 가격성 판촉 INSERT

INSERT INTO IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
(
BASE_YYYYMM	,
SCENARIO_TYPE_CD	,
DIV_CD	,
SUBSDR_CD	,
AU_CD	,
MANUAL_ADJ_FLAG	,
KPI_CD	,
CAT_CD	,
SUB_CAT_CD	,
ZONE_RNR_CD	,
SUBSDR_RNR_CD	,
CNTRY_CD	,
APPLY_YYYYMM	,
CURRM_KRW_AMT	,
CURRM_USD_AMT	,
ACCU_KRW_AMT	,
ACCU_USD_AMT	,
ATTRIBUTE1_VALUE	,
ATTRIBUTE2_VALUE	,
ATTRIBUTE3_VALUE	,
ATTRIBUTE4_VALUE	,
ATTRIBUTE5_VALUE	,
CREATION_DATE	,
CREATION_USR_ID	,
LAST_UPD_DATE	,
LAST_UPD_USR_ID	
)



WITH V_BEP_SMART AS 
       (SELECT SEQ AS SEQ ,
              MODULE_CODE AS MODULE_CODE ,
              CODE_TYPE AS CODE_TYPE ,
              CODE_ID AS CODE_ID ,
              CODE_NAME AS CODE_NAME ,
              DIVISION_CODE AS DIVISION_CODE ,
              YYYYMMDD AS YYYYMMDD ,
              DESCRIPTION AS DESCRIPTION ,
              DISPLAY_ORDER_SEQ AS DISPLAY_ORDER_SEQ ,
              USE_FLAG AS USE_FLAG ,
              CREATION_DATE AS CREATION_DATE ,
              CREATE_USER_ID AS CREATE_USER_ID ,
              CREATE_USER_NAME AS CREATE_USER_NAME ,
              ATTRIBUTE1 AS SCENARIO_TYPE_CD ,
              ATTRIBUTE2 AS SCENARIO_TYPE_NAME ,
              ATTRIBUTE3 AS SORT_ORDER ,
              ATTRIBUTE4 AS DIV_CD ,
              ATTRIBUTE5 AS DIV_KOR_NAME ,
              ATTRIBUTE6 AS DIV_SHRT_NAME ,
              ATTRIBUTE7 AS SUBSDR_RNR_CD ,
              ATTRIBUTE8 AS MGT_ORG_SHRT_NAME ,
              ATTRIBUTE9 AS SORT_ORDER0 ,
              ATTRIBUTE10 AS SUBSDR_CD0 ,
              ATTRIBUTE11 AS NEW_SUBSDR_SHRT_NAME ,
              ATTRIBUTE12 AS SORT1_ORDER ,
              ATTRIBUTE13 AS ZONE_CD ,
              ATTRIBUTE14 AS ZONE_NAME ,
              ATTRIBUTE15 AS SCRN_DSPL_SEQ ,
              ATTRIBUTE16 AS CNTRY_RNR_CD ,
              ATTRIBUTE17 AS CNTRY_NAME ,
              ATTRIBUTE18 AS SORT_ORDER1 ,
              ATTRIBUTE19 AS BASE_YYYYMM ,
/*              
              ATTRIBUTE20 AS CURRENCY_CD ,
              ATTRIBUTE21 AS CURRM_ACCUM_TYPE_CD ,
              ATTRIBUTE22 AS SALES_QTY ,
              ATTRIBUTE23 AS NSALES_AMT ,
              ATTRIBUTE24 AS RNR_SALES_AMT ,
              ATTRIBUTE25 AS GROSS_SALES_AMT ,
              ATTRIBUTE26 AS SALES_DEDUCT_AMT ,
              ATTRIBUTE27 AS MGNL_PRF_AMT ,
              ATTRIBUTE28 AS OI_AMT 
*/              
			ATTRIBUTE20    AS    CURR_NSALES_AMT_KRW  ,
			ATTRIBUTE21    AS    CURR_RNR_SALES_AMT_KRW  ,
			ATTRIBUTE22    AS    CURR_GROSS_SALES_AMT_KRW  ,
			ATTRIBUTE23    AS    CURR_SALES_DEDUCT_AMT_KRW  ,
			ATTRIBUTE24    AS    CURR_MGNL_PRF_AMT_KRW  ,
			ATTRIBUTE25    AS    CURR_OI_AMT_KRW  ,
			ATTRIBUTE26    AS    CURR_NSALES_AMT_USD  ,
			ATTRIBUTE27    AS    CURR_RNR_SALES_AMT_USD  ,
			ATTRIBUTE28    AS    CURR_GROSS_SALES_AMT_USD  ,
			ATTRIBUTE29    AS    CURR_SALES_DEDUCT_AMT_USD  ,
			ATTRIBUTE30    AS    CURR_MGNL_PRF_AMT_USD  ,
			ATTRIBUTE31    AS    CURR_OI_AMT_USD  ,
			ATTRIBUTE32    AS    ACCU_NSALES_AMT_KRW  ,
			ATTRIBUTE33    AS    ACCU_RNR_SALES_AMT_KRW  ,
			ATTRIBUTE34    AS    ACCU_GROSS_SALES_AMT_KRW  ,
			ATTRIBUTE35    AS    ACCU_SALES_DEDUCT_AMT_KRW  ,
			ATTRIBUTE36    AS    ACCU_MGNL_PRF_AMT_KRW  ,
			ATTRIBUTE37    AS    ACCU_OI_AMT_KRW  ,
			ATTRIBUTE38    AS    ACCU_NSALES_AMT_USD  ,
			ATTRIBUTE39    AS    ACCU_RNR_SALES_AMT_USD  ,
			ATTRIBUTE40    AS    ACCU_GROSS_SALES_AMT_USD  ,
			ATTRIBUTE41    AS    ACCU_SALES_DEDUCT_AMT_USD  ,
			ATTRIBUTE42    AS    ACCU_MGNL_PRF_AMT_USD  ,
			ATTRIBUTE43    AS    ACCU_OI_AMT_USD  
              
              
         FROM IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA 
        WHERE SEQ = '1500' 
              AND MODULE_CODE = 'ARES' 
              AND CODE_TYPE = 'BEP_SMART' 
              AND YYYYMMDD = '201510'
       )  
       
       

      

SELECT A.BASE_YYYYMM AS BASE_YYYYMM , 
       A.SCENARIO_TYPE_CD AS SCENARIO_TYPE_CD, 
       A.DIV_CD AS DIV_CD, 
       A.SUBSDR_CD0 AS SUBSDR_CD, 
       '*' AS AU_CD, 
       'N' AS MANUAL_ADJ_FLAG, 
       C2.CODE_ID AS KPI_CD, 
       'BEP_SMART_DIV' AS CAT_CD, 
       '*' AS SUB_CAT_CD, 
       A.ZONE_CD AS ZONE_RNR_CD, 
       A.SUBSDR_RNR_CD AS SUBSDR_RNR_CD, 
       A.CNTRY_RNR_CD AS CNTRY_CD,    
       CASE A.SCENARIO_TYPE_CD 
           WHEN 'AC0' 
           THEN A.BASE_YYYYMM 
           WHEN 'PR1' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 1 MONTHS,'YYYYMM') 
           WHEN 'PR2' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 2 MONTHS,'YYYYMM') 
           WHEN 'PR3' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 3 MONTHS,'YYYYMM') 
           WHEN 'PR4' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 4 MONTHS,'YYYYMM') 
           ELSE A.BASE_YYYYMM 
       END AS APPLY_YYYYMM,  

       -- CURRM 'KRW'
       SUM(
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN A.CURR_RNR_SALES_AMT_KRW
           WHEN 'COI' THEN A.CURR_OI_AMT_KRW
           WHEN 'MGN_PROFIT' THEN A.CURR_MGNL_PRF_AMT_KRW
           WHEN 'GROSS_SALE' THEN A.CURR_GROSS_SALES_AMT_KRW
           WHEN 'SALES_DEDUCTION' THEN A.CURR_SALES_DEDUCT_AMT_KRW
           ELSE 0                       
       END ) AS CURRM_KRW_AMT,   
       
       
       -- CURRM 'USD'
       SUM(
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN A.CURR_RNR_SALES_AMT_USD
           WHEN 'COI' THEN A.CURR_OI_AMT_USD
           WHEN 'MGN_PROFIT' THEN A.CURR_MGNL_PRF_AMT_USD
           WHEN 'GROSS_SALE' THEN A.CURR_GROSS_SALES_AMT_USD
           WHEN 'SALES_DEDUCTION' THEN A.CURR_SALES_DEDUCT_AMT_USD
           ELSE 0                       
       END ) AS CURRM_USD_AMT,  
       

       -- ACCUM 'KRW'
       SUM(
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN A.ACCU_RNR_SALES_AMT_KRW
           WHEN 'COI' THEN A.ACCU_OI_AMT_KRW
           WHEN 'MGN_PROFIT' THEN A.ACCU_MGNL_PRF_AMT_KRW
           WHEN 'GROSS_SALE' THEN A.ACCU_GROSS_SALES_AMT_KRW
           WHEN 'SALES_DEDUCTION' THEN A.ACCU_SALES_DEDUCT_AMT_KRW
           ELSE 0                       
       END ) AS ACCUM_KRW_AMT,   
       -- ACCUM 'USD'
       SUM(
       CASE C2.CODE_ID 
           WHEN 'SALE' THEN A.ACCU_RNR_SALES_AMT_USD
           WHEN 'COI' THEN A.ACCU_OI_AMT_USD
           WHEN 'MGN_PROFIT' THEN A.ACCU_MGNL_PRF_AMT_USD
           WHEN 'GROSS_SALE' THEN A.ACCU_GROSS_SALES_AMT_USD
           WHEN 'SALES_DEDUCTION' THEN A.ACCU_SALES_DEDUCT_AMT_USD
           ELSE 0                       
       END ) AS ACCUM_USD_AMT,         
       
       A.NEW_SUBSDR_SHRT_NAME AS ATTRIBUTE1_VALUE,
       NULL AS ATTRIBUTE2_VALUE,
       NULL AS ATTRIBUTE3_VALUE,
       NULL AS ATTRIBUTE4_VALUE,
       NULL AS ATTRIBUTE5_VALUE,
       current timestamp AS CREATION_DATE,
       'ares' AS CREATION_USR_ID,
       current timestamp AS LAST_UPD_DATE,
       'ares' AS LAST_UPD_USR_ID         
       
  FROM V_BEP_SMART A 
   LEFT OUTER JOIN IPTDW.IPTDW_RES_DIM_CODES C2 
       ON C2.CODE_TYPE = 'KPI_TYPE' 
       AND C2.CODE_ID IN ('SALE','MGN_PROFIT','COI','GROSS_SALE','SALES_DEDUCTION') 
 WHERE A.YYYYMMDD BETWEEN '201407' and '201510'
       AND 1=1  
       
 GROUP BY 
       A.BASE_YYYYMM,
       A.SCENARIO_TYPE_CD, 
       A.DIV_CD , 
       A.SUBSDR_CD0, 
       C2.CODE_ID ,
       A.ZONE_CD , 
       A.SUBSDR_RNR_CD , 
       A.CNTRY_RNR_CD ,    
       CASE A.SCENARIO_TYPE_CD 
           WHEN 'AC0' 
           THEN A.BASE_YYYYMM 
           WHEN 'PR1' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 1 MONTHS,'YYYYMM') 
           WHEN 'PR2' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 2 MONTHS,'YYYYMM') 
           WHEN 'PR3' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 3 MONTHS,'YYYYMM') 
           WHEN 'PR4' 
           THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM,'YYYYMM') + 4 MONTHS,'YYYYMM') 
           ELSE A.BASE_YYYYMM 
       END  ,    
       A.NEW_SUBSDR_SHRT_NAME 
       
  WITH UR ;
       

              
              

SELECT DISTINCT BASE_YYYYMM
FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM BETWEEN '201401' AND '201510'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
AND   DIV_CD = 'GBU'
;

-- GBU

INSERT INTO IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY

SELECT 

BASE_YYYYMM	,
SCENARIO_TYPE_CD	,
'GBU' AS DIV_CD	,
SUBSDR_CD	,
AU_CD	,
MANUAL_ADJ_FLAG	,
KPI_CD	,
CAT_CD	,
SUB_CAT_CD	,
ZONE_RNR_CD	,
SUBSDR_RNR_CD	,
CNTRY_CD	,
APPLY_YYYYMM	,
SUM(CURRM_KRW_AMT) AS CURRM_KRW_AMT	,
SUM(CURRM_USD_AMT) AS CURRM_USD_AMT	,
SUM(ACCU_KRW_AMT)  AS ACCU_KRW_AMT	,
SUM(ACCU_USD_AMT)  AS ACCU_USD_AMT	,
ATTRIBUTE1_VALUE	,
ATTRIBUTE2_VALUE	,
ATTRIBUTE3_VALUE	,
ATTRIBUTE4_VALUE	,
ATTRIBUTE5_VALUE	,
       current timestamp AS CREATION_DATE,
       'ares' AS CREATION_USR_ID,
       current timestamp AS LAST_UPD_DATE,
       'ares' AS LAST_UPD_USR_ID  



FROM IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE BASE_YYYYMM = '201510' -- BETWEEN '201401' AND '201509'
AND   CAT_CD = 'BEP_SMART_DIV'
AND   MANUAL_ADJ_FLAG = 'N'
GROUP BY 

BASE_YYYYMM	,
SCENARIO_TYPE_CD	,
--'GBU' AS DIV_CD	,
SUBSDR_CD	,
AU_CD	,
MANUAL_ADJ_FLAG	,
KPI_CD	,
CAT_CD	,
SUB_CAT_CD	,
ZONE_RNR_CD	,
SUBSDR_RNR_CD	,
CNTRY_CD	,
APPLY_YYYYMM	,
--CURRM_KRW_AMT	,
--CURRM_USD_AMT	,
--ACCU_KRW_AMT	,
--ACCU_USD_AMT	,
ATTRIBUTE1_VALUE	,
ATTRIBUTE2_VALUE	,
ATTRIBUTE3_VALUE	,
ATTRIBUTE4_VALUE	,
ATTRIBUTE5_VALUE	
;

/* 조회 쿼리 */
   
               
      SELECT  '1.전년실적' AS col_index,
             SUBSDR_CD,
             ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             KPI_CD,
             sum(CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
      WHERE BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201510',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE('201510', 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   SCENARIO_TYPE_CD = 'AC0'
      AND   CAT_CD = 'BEP_SMART_DIV'
      AND   KPI_CD in ('SALE', 'COI')
      AND   SUBSDR_CD = 'EEUK'
      
      GROUP BY SUBSDR_CD,
               ATTRIBUTE1_VALUE,
               DIV_CD,
               BASE_YYYYMM,
               KPI_CD                  
               
      UNION ALL
      SELECT  '2.당월실적' AS col_index,
             SUBSDR_CD,
             ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             DIV_CD,
             BASE_YYYYMM,
             KPI_CD,
             sum(CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
      WHERE BASE_YYYYMM BETWEEN '201401' AND  '201510'
      AND   SCENARIO_TYPE_CD = 'AC0'
      AND   CAT_CD = 'BEP_SMART_DIV'
      AND   KPI_CD in ('SALE', 'COI')
      AND   SUBSDR_CD = 'EEUK'
      
      GROUP BY SUBSDR_CD,
               ATTRIBUTE1_VALUE,
               DIV_CD,
               BASE_YYYYMM,
               KPI_CD       
               

      UNION ALL
      SELECT  '3.전월이동' AS col_index,
             SUBSDR_CD,
             ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             DIV_CD,
             APPLY_YYYYMM AS BASE_YYYYMM,
             KPI_CD,
             sum(CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
      WHERE BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201510',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE('201510', 'YYYYMM') - 1 MONTH , 'YYYYMM') 
      AND   SCENARIO_TYPE_CD = 'PR1'
      AND   CAT_CD = 'BEP_SMART_DIV'
      AND   KPI_CD in ('SALE', 'COI')
      AND   SUBSDR_CD = 'EEUK'
      
      GROUP BY SUBSDR_CD,
               ATTRIBUTE1_VALUE,
               DIV_CD,
               APPLY_YYYYMM,
               KPI_CD    
               
      WITH UR ;  
      

/* B2C */

SELECT *
FROM IPTDW.IPTDW_RES_DIM_CODES
WHERE CODE_TYPE LIKE 'B2C%'
                   
                   


/* B2C */

      SELECT  '4.B2C_당월실적' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EEUK'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
               
      UNION ALL
      SELECT  '5.B2C_전월이동' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.APPLY_YYYYMM AS BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID      
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201510',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE('201510', 'YYYYMM') - 1 MONTH , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'PR1'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EEUK'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.APPLY_YYYYMM,
               A.KPI_CD  
               
               
      WITH UR ;  
                     


/*
국가별 매출/ 영업이익
CALL SP_CD_RES_SMR_TREND_CNTRY_HISTORY ('201510','LGESW') ;  
*/

/* 법인내 관할국 매출 순위 4개 */

WITH V_CNTRY_RNK_B2C AS 
( 

      SELECT  --'2.B2C_국가별' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.CNTRY_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount,
             
             
             smallint( row_number() over( partition BY A.SUBSDR_CD,A.BASE_YYYYMM,A.KPI_CD  ORDER BY A.BASE_YYYYMM, A.KPI_CD, SUM(A.ACCU_USD_AMT) DESC )) AS SORT_KEY
             
             
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE 1=1 --A.BASE_YYYYMM = '201510' -- BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE')
      --AND   A.DIV_CD = 'GBU'
      AND   A.ATTRIBUTE1_VALUE = 'LGEUK'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.CNTRY_CD,
               A.BASE_YYYYMM,
               A.KPI_CD )
      




-- 1.법인전체
      SELECT  '1.법인전체' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             '*' AS CNTRY_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount,
             '0' AS SORT_KEY
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE A.BASE_YYYYMM = '201510' --BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.DIV_CD = 'GBU'
      AND   A.SUBSDR_CD = 'EMGF'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               --A.CNTRY_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
                     
      UNION ALL   

-- 2.B2C_국가별
      SELECT  '2.B2C_국가별' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.CNTRY_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             SUM(A.CURRM_USD_AMT) AS amount,
             C.SORT_KEY
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      
      INNER JOIN  V_CNTRY_RNK_B2C C
         ON C.SUBSDR_CD = A.SUBSDR_CD
        AND C.CNTRY_CD = A.CNTRY_CD
        AND C.BASE_YYYYMM = A.BASE_YYYYMM
        --AND C.KPI_CD = A.KPI_CD
        AND C.SORT_KEY IN ( '1','2','3','4')
      
      WHERE A.BASE_YYYYMM = '201510' -- BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'COI')
      --AND   A.DIV_CD = 'GBU'
      AND   A.SUBSDR_CD = 'EMGF'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.CNTRY_CD,
               A.BASE_YYYYMM,
               A.KPI_CD,
               C.SORT_KEY
      
      --WITH UR ;


      UNION ALL



-- 3.B2C_기타국가
      SELECT  '3.B2C_기타국가' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             '*' AS CNTRY_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount,
             '900' AS SORT_KEY
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      
      INNER JOIN  V_CNTRY_RNK_B2C C
         ON C.SUBSDR_CD = A.SUBSDR_CD
        AND C.CNTRY_CD = A.CNTRY_CD
        AND C.BASE_YYYYMM = A.BASE_YYYYMM
        --AND C.KPI_CD = A.KPI_CD
        AND C.SORT_KEY NOT IN ( '1','2','3','4')
      
      WHERE A.BASE_YYYYMM = '201510' -- BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'COI')
      --AND   A.DIV_CD = 'GBU'
      AND   A.SUBSDR_CD = 'EMGF'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               --A.CNTRY_CD,
               A.BASE_YYYYMM,
               A.KPI_CD
               --C.SORT_KEY
      
   


     
     WITH UR ;
     
     
     
-- 가격성 판촉
-- 가격성판촉율 = SALES_DEDUCTION * -1 / (SALE - SALES_DEDUCTION)
CALL SP_CD_RES_SMR_TREND_SALES_DEDUCT_HISTORY ('201510','LGESW') ; 



     SELECT  '1.당월실적_법인전체' AS col_index,
             A.DIV_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             --A.DIV_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      --INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
      --   ON B.CODE_TYPE = 'B2C_DIV'
      --  AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM = '201510' -- BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      AND   A.SUBSDR_CD = 'EEDG'
      and   a.zone_rnr_cd <> 'ZZZ'
      and   a.div_cd = 'GBU'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
               ;

     SELECT  '2.당월실적_B2C' AS col_index,
             A.DIV_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             --A.DIV_CD,
             A.BASE_YYYYMM,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM = '201510' -- BETWEEN '201401' AND  '201510'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      AND   A.SUBSDR_CD = 'EEDG'
      and   a.zone_rnr_cd <> 'ZZZ'
      --and   a.div_cd = 'GBU'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
               ;

-- 2016.01.14 SHLEE 추가

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
SELECT APPLY_YYYYMM
      ,'MP'
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
FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY
WHERE  CAT_CD = 'BEP_SMART_DIV'
AND    SCENARIO_TYPE_CD = 'PR1'
WITH UR;
     
           