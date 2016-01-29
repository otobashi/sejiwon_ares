DELETE

FROM npt_rs_mgr.TB_RS_EXCEL_UPLD_DATA_D A
WHERE a.prcs_seq = '1540'
and   a.rs_module_cd = 'ARES'
and   a.rs_clsf_id = 'BEP_SMART'
and   a.rs_type_cd = 'BEP_SMART_BB'
and   a.base_yyyymmdd = &iv_yyyymm
;




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
)
-- 43938 37.425
-- BB RATIO
-- BB RATIO
SELECT '1540'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_BB'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_BB'                                                  AS RS_TYPE_NAME                             
      ,A.DIVISION_CODE                                                 AS DIV_CD                                   
      ,A.BASIS_YYYYMM                                                  AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,A.BASIS_YYYYMM
      ,A.BASIS_YYYYWW
      ,A.SUBSDR_CD
      ,A.DIVISION_CODE
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.DECREASE_AMT)) AS AWARD_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.DECREASE_AMT)) AS AWARD_KRW_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.CHANGE_AMT)) AS SALES_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.CHANGE_AMT)) AS SALES_KRW_AMT
FROM   (
        SELECT A.BASIS_YYYYMM
              ,A.BASIS_YYYYWW
              ,A.CORPORATION_CODE
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))   SUBSDR_CD
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))   MGT_ORG_CD
              ,A.AU_CODE
              ,A.DIVISION_CODE
              ,A.CURRENCY_CODE
              ,SUM(A.NEW_AMT     ) AS NEW_AMT     
              ,SUM(A.INCREASE_AMT) AS INCREASE_AMT
              ,SUM(A.DECREASE_AMT) AS DECREASE_AMT
              ,SUM(A.CHANGE_AMT  ) AS CHANGE_AMT  
        FROM   TB_I24_B2B_PIPELINE_BALANCE A
              ,TB_CM_MGT_ORG_M O
        WHERE  A.STAGE_CODE              = 'A'
        AND    A.CURRENCY_CODE           IN ('KRW', 'USD')
        AND    O.MGT_ORG_TYPE_CD         (+)= 'IS'
        AND    O.CURR_FLAG               (+)= 'Y'
        AND    O.MGT_ORG_ENG_NAME        (+)= A.CORPORATION_NAME
        GROUP BY A.BASIS_YYYYMM
                ,A.BASIS_YYYYWW
                ,A.CORPORATION_CODE
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))
                ,A.AU_CODE
                ,A.DIVISION_CODE
                ,A.CURRENCY_CODE
       ) A
      ,TB_CM_WEEK_M B
WHERE  A.BASIS_YYYYWW = REPLACE(B.BASE_YYYYWEEK,'W','')
GROUP BY A.BASIS_YYYYMM
        ,A.BASIS_YYYYWW
        ,A.SUBSDR_CD
        ,A.DIVISION_CODE

UNION ALL
-- 53128
-- BB RATIO W5
SELECT '1540'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_BBW5'                                                  AS RS_TYPE_CD                               
      ,'BEP_SMART_BBW5'                                                  AS RS_TYPE_NAME                             
      ,A.DIVISION_CODE                                                 AS DIV_CD                                   
      ,B.YYYYMM                                                        AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,B.YYYYMM
      ,B.W1_WEEK
      ,A.SUBSDR_CD
      ,A.DIVISION_CODE
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.DECREASE_AMT)) AS AWARD_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.DECREASE_AMT)) AS AWARD_KRW_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.CHANGE_AMT)) AS SALES_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.CHANGE_AMT)) AS SALES_KRW_AMT
FROM   (
        SELECT A.BASIS_YYYYMM
              ,A.BASIS_YYYYWW
              ,A.CORPORATION_CODE
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))   SUBSDR_CD
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))   MGT_ORG_CD
              ,A.AU_CODE
              ,A.DIVISION_CODE
              ,A.CURRENCY_CODE
              ,SUM(A.NEW_AMT     ) AS NEW_AMT     
              ,SUM(A.INCREASE_AMT) AS INCREASE_AMT
              ,SUM(A.DECREASE_AMT) AS DECREASE_AMT
              ,SUM(A.CHANGE_AMT  ) AS CHANGE_AMT  
        FROM   TB_I24_B2B_PIPELINE_BALANCE A
              ,TB_CM_MGT_ORG_M O
        WHERE  A.STAGE_CODE              = 'A'
        AND    A.CURRENCY_CODE           IN ('KRW', 'USD')
        AND    O.MGT_ORG_TYPE_CD         (+)= 'IS'
        AND    O.CURR_FLAG               (+)= 'Y'
        AND    O.MGT_ORG_ENG_NAME        (+)= A.CORPORATION_NAME
        GROUP BY A.BASIS_YYYYMM
                ,A.BASIS_YYYYWW
                ,A.CORPORATION_CODE
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))
                ,A.AU_CODE
                ,A.DIVISION_CODE
                ,A.CURRENCY_CODE
       ) A
      ,(SELECT SUBSTR(W1.START_YYYYMMDD, 1, 6) YYYYMM
              ,REPLACE(W1.BASE_YYYYWEEK,'W','') W1_WEEK
              ,REPLACE(W2.BASE_YYYYWEEK,'W','') W2_WEEK
        FROM   TB_CM_WEEK_M W1
              ,TB_CM_WEEK_M W2
        WHERE    w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 5주 데이터를 읽어옴 */
        AND    w1.base_yyyy >= '2013'
        AND    W2.BASE_YYYY < '2017') B
WHERE  A.BASIS_YYYYWW = B.W2_WEEK
GROUP BY B.YYYYMM
        ,B.W1_WEEK
        ,A.SUBSDR_CD
        ,A.DIVISION_CODE

UNION ALL
-- 63961
-- BB RATIO W13
SELECT '1540'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_BBW13'                                               AS RS_TYPE_CD                               
      ,'BEP_SMART_BBW13'                                               AS RS_TYPE_NAME                             
      ,A.DIVISION_CODE                                                 AS DIV_CD                                   
      ,B.YYYYMM                                                        AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,B.YYYYMM
      ,B.W1_WEEK
      ,A.SUBSDR_CD
      ,A.DIVISION_CODE
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.DECREASE_AMT)) AS AWARD_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.DECREASE_AMT)) AS AWARD_KRW_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.CHANGE_AMT)) AS SALES_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.CHANGE_AMT)) AS SALES_KRW_AMT
FROM   (
        SELECT A.BASIS_YYYYMM
              ,A.BASIS_YYYYWW
              ,A.CORPORATION_CODE
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))   SUBSDR_CD
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))   MGT_ORG_CD
              ,A.AU_CODE
              ,A.DIVISION_CODE
              ,A.CURRENCY_CODE
              ,SUM(A.NEW_AMT     ) AS NEW_AMT     
              ,SUM(A.INCREASE_AMT) AS INCREASE_AMT
              ,SUM(A.DECREASE_AMT) AS DECREASE_AMT
              ,SUM(A.CHANGE_AMT  ) AS CHANGE_AMT  
        FROM   TB_I24_B2B_PIPELINE_BALANCE A
              ,TB_CM_MGT_ORG_M O
        WHERE  A.STAGE_CODE              = 'A'
        AND    A.CURRENCY_CODE           IN ('KRW', 'USD')
        AND    O.MGT_ORG_TYPE_CD         (+)= 'IS'
        AND    O.CURR_FLAG               (+)= 'Y'
        AND    O.MGT_ORG_ENG_NAME        (+)= A.CORPORATION_NAME
        GROUP BY A.BASIS_YYYYMM
                ,A.BASIS_YYYYWW
                ,A.CORPORATION_CODE
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))
                ,A.AU_CODE
                ,A.DIVISION_CODE
                ,A.CURRENCY_CODE
       ) A
      ,(SELECT SUBSTR(W1.START_YYYYMMDD, 1, 6) YYYYMM
              ,REPLACE(W1.BASE_YYYYWEEK,'W','') W1_WEEK
              ,REPLACE(W2.BASE_YYYYWEEK,'W','') W2_WEEK
        FROM   TB_CM_WEEK_M W1
              ,TB_CM_WEEK_M W2
        WHERE    w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 13주 데이터를 읽어옴 */
        AND    w1.base_yyyy >= '2013'
        AND    W2.BASE_YYYY < '2017') B
WHERE  A.BASIS_YYYYWW = B.W2_WEEK
GROUP BY B.YYYYMM
        ,B.W1_WEEK
        ,A.SUBSDR_CD
        ,A.DIVISION_CODE

UNION ALL

-- 94996
-- BB RATIO W52
SELECT '1540'                                                          AS PRCS_SEQ                                 
      ,'ARES'                                                          AS RS_MODULE_CD                             
      ,'BEP_SMART'                                                     AS RS_CLSF_ID                               
      ,'BEP_SMART_BBW52'                                               AS RS_TYPE_CD                               
      ,'BEP_SMART_BBW52'                                               AS RS_TYPE_NAME                             
      ,A.DIVISION_CODE                                                 AS DIV_CD                                   
      ,B.YYYYMM                                                        AS BASE_YYYYMMDD                            
      ,NULL                                                            AS CD_DESC                                  
      ,NULL                                                            AS SORT_SEQ                                 
      ,'Y'                                                             AS USE_FLAG                                 
      ,B.YYYYMM
      ,B.W1_WEEK
      ,A.SUBSDR_CD
      ,A.DIVISION_CODE
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'USD',A.DECREASE_AMT)) AS AWARD_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.NEW_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.INCREASE_AMT)) + SUM(DECODE(A.CURRENCY_CODE,'KRW',A.DECREASE_AMT)) AS AWARD_KRW_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'USD',A.CHANGE_AMT)) AS SALES_USD_AMT
      ,SUM(DECODE(A.CURRENCY_CODE,'KRW',A.CHANGE_AMT)) AS SALES_KRW_AMT
FROM   (
        SELECT A.BASIS_YYYYMM
              ,A.BASIS_YYYYWW
              ,A.CORPORATION_CODE
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))   SUBSDR_CD
              ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))   MGT_ORG_CD
              ,A.AU_CODE
              ,A.DIVISION_CODE
              ,A.CURRENCY_CODE
              ,SUM(A.NEW_AMT     ) AS NEW_AMT     
              ,SUM(A.INCREASE_AMT) AS INCREASE_AMT
              ,SUM(A.DECREASE_AMT) AS DECREASE_AMT
              ,SUM(A.CHANGE_AMT  ) AS CHANGE_AMT  
        FROM   TB_I24_B2B_PIPELINE_BALANCE A
              ,TB_CM_MGT_ORG_M O
        WHERE  A.STAGE_CODE              = 'A'
        AND    A.CURRENCY_CODE           IN ('KRW', 'USD')
        AND    O.MGT_ORG_TYPE_CD         (+)= 'IS'
        AND    O.CURR_FLAG               (+)= 'Y'
        AND    O.MGT_ORG_ENG_NAME        (+)= A.CORPORATION_NAME
        GROUP BY A.BASIS_YYYYMM
                ,A.BASIS_YYYYWW
                ,A.CORPORATION_CODE
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.UP_MGT_ORG_CD, A.CORPORATION_CODE))
                ,DECODE(A.CORPORATION_CODE, 'EEEB', 'EEBN', NVL(O.MGT_ORG_CD   , A.CORPORATION_CODE))
                ,A.AU_CODE
                ,A.DIVISION_CODE
                ,A.CURRENCY_CODE
       ) A
      ,(SELECT SUBSTR(W1.START_YYYYMMDD, 1, 6) YYYYMM
              ,REPLACE(W1.BASE_YYYYWEEK,'W','') W1_WEEK
              ,REPLACE(W2.BASE_YYYYWEEK,'W','') W2_WEEK
        FROM   TB_CM_WEEK_M W1
              ,TB_CM_WEEK_M W2
        WHERE    w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 52주 데이터를 읽어옴 */
        AND    w1.base_yyyy >= '2013'
        AND    W2.BASE_YYYY < '2017') B
WHERE  A.BASIS_YYYYWW = B.W2_WEEK
GROUP BY B.YYYYMM
        ,B.W1_WEEK
        ,A.SUBSDR_CD
        ,A.DIVISION_CODE

;
