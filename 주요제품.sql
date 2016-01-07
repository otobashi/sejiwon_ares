/*******************************************************/
/* 林夸力前焙 Master                                   */
/*******************************************************/
WITH V_PROD_MST (
   MGT_TYPE_CD
  ,PROD_GR_CD
  ,SUBSDR_CD
  ,PROD_CD
  ,PROD_LVL_CD
  ,PROD_ENG_NAME
  ,PROD_KOR_NAME
  ,UP_PROD_CD
  ,LAST_LVL_FLAG
  ,DIV_CD
  ,ENABLE_FLAG
  ,SALES_QTY_INCL_FLAG
  ,SUM_EXCL_FLAG
  ,SORT_ORDER
  ,PROD_CD_DESC
)
AS
(
  SELECT MGT_TYPE_CD
        ,PROD_GR_CD
        ,SUBSDR_CD
        ,PROD_CD
        ,PROD_LVL_CD
        ,PROD_ENG_NAME
        ,PROD_KOR_NAME
        ,UP_PROD_CD
        ,LAST_LVL_FLAG
        ,DIV_CD
        ,ENABLE_FLAG
        ,SALES_QTY_INCL_FLAG
        ,SUM_EXCL_FLAG
        ,SORT_ORDER
        ,PROD_CD_DESC
  FROM   TB_CM_PROD_PERIOD_H T
  WHERE  T.MGT_TYPE_CD = 'CM'
  AND    T.ACCTG_YYYYMM = '*'
  AND    T.ACCTG_WEEK = '*'
  AND    T.TEMP_FLAG = 'N'
  AND    PROD_CD NOT IN ( '847141' ,'ACAHHAW')-- IMSI
  UNION ALL
  SELECT 'CM'                                                 AS MGT_TYPE_CD
        ,'PRODUCT_LEVEL'                                      AS PROD_GR_CD
        ,'*'                                                  AS SUBSDR_CD
        ,'*'                                                  AS PROD_CD
        ,B.PROD_LVL_CD                                        AS PROD_LVL_CD
        ,'*'          ||'(LVL'||B.PROD_LVL_CD||')'            AS PROD_ENG_NAME
        ,'*'          ||'(LVL'||B.PROD_LVL_CD||')'            AS PROD_KOR_NAME
        ,'*'                                                  AS UP_PROD_CD
        ,CASE WHEN B.PROD_LVL_CD  = '4' THEN 'Y' ELSE 'N' END AS LAST_LVL_FLAG
        ,'*'                                                  AS DIV_CD
        ,'Y'                                                  AS ENABLE_FLAG
        ,'N'                                                  AS SALES_QTY_INCL_FLAG
        ,NULL                                                 AS SUM_EXCL_FLAG
        ,NULL                                                 AS SORT_ORDER
        ,NULL                                                 AS PROD_CD_DESC
  FROM DUAL
     ,(SELECT '1' AS PROD_LVL_CD FROM DUAL UNION ALL
       SELECT '2' AS PROD_LVL_CD FROM DUAL UNION ALL
       SELECT '3' AS PROD_LVL_CD FROM DUAL UNION ALL
       SELECT '4' AS PROD_LVL_CD FROM DUAL  ) B
),
/*******************************************************/
/* 林夸力前焙 Levle喊 (1,2,3,4)                        */
/*******************************************************/
V_PROD_M (
    MGT_TYPE_CD
   ,PROD_GR_CD
   ,SUBSDR_CD
   ,PROD_CD
   ,PROD_LVL_CD
   ,PROD_ENG_NAME
   ,PROD_KOR_NAME
   ,UP_PROD_CD
   ,LAST_LVL_FLAG
   ,DIV_CD
   ,ENABLE_FLAG
   ,SALES_QTY_INCL_FLAG
   ,SUM_EXCL_FLAG
   ,SORT_ORDER
)
AS
(
   SELECT MGT_TYPE_CD
         ,PROD_GR_CD
         ,SUBSDR_CD
         ,PROD_CD
         ,PROD_LVL_CD
         ,PROD_ENG_NAME
         ,PROD_KOR_NAME
         ,UP_PROD_CD
         ,LAST_LVL_FLAG
         ,DIV_CD
         ,ENABLE_FLAG
         ,SALES_QTY_INCL_FLAG
         ,SUM_EXCL_FLAG
         ,SORT_ORDER
   FROM   V_PROD_MST
   WHERE  PROD_GR_CD = 'RPT_PROD_GR'
)

/*******************************************************/
/* 林夸力前焙 炼雀                    */
/*******************************************************/
SELECT /*+ PARALLEL(32) */
       '1510'                                                          AS PRCS_SEQ
      ,'ARES'                                                          AS RS_MODULE_CD
      ,'BEP_SMART'                                                     AS RS_CLSF_ID
      ,'BEP_SMART_PROD'                                                AS RS_TYPE_CD
      ,'BEP_SMART_PROD'                                                AS RS_TYPE_NAME
      ,A11.DIV_CD                                                      AS DIV_CD
      ,A11.ACCTG_YYYYMM                                                AS BASE_YYYYMMDD
      ,NULL                                                            AS CD_DESC
      ,NULL                                                            AS SORT_SEQ
      ,'Y'                                                             AS USE_FLAG
      ,A11.ACCTG_YYYYMM                                                AS BASE_YYYYMM
      ,A11.DIV_CD                                                      AS DIV_CD
      ,A17.SCRN_DSPL_SEQ                                               AS DIV_KOR_NAME
      ,A17.DIV_SHRT_NAME                                               AS DIV_SHRT_NAME
      ,A11.SUBSDR_RNR_CD                                               AS SUBSDR_CD
      ,A112.MGT_ORG_SHRT_NAME                                          AS MGT_ORG_SHRT_NAME
      ,A112.SORT_ORDER                                                 AS SORT_ORDER
      ,A11.SUBSDR_CD                                                   AS SUBSDR_RNR_CD
      ,A111.SUBSDR_SHRT_NAME                                           AS SUBSDR_NAME
      ,A111.SORT_ORDER                                                 AS SORT1_ORDER
      ,A11.PRODUCTION_SUBSDR_CD                                        AS SUBSDR_CD1
      ,A19.SUBSDR_SHRT_NAME                                            AS SUBSDR_NAME0
      ,A19.SORT_ORDER                                                  AS SORT_ORDER0
      ,A11.SCENARIO_TYPE_CD                                            AS SCENARIO_TYPE_CD
      ,A110.SCENARIO_TYPE_NAME                                         AS SCENARIO_TYPE_NAME
      ,A110.SORT_ORDER                                                 AS SORT_ORDER1
      ,A16.UP_PROD_CD                                                  AS PROD_CD
      ,A113.PROD_KOR_NAME                                              AS PROD_KOR_NAME
      ,A113.PROD_ENG_NAME                                              AS PROD_ENG_NAME
      ,A113.SORT_ORDER                                                 AS SORT_ORDER2
      ,A15.UP_PROD_CD                                                  AS PROD_CD0
      ,A16.PROD_KOR_NAME                                               AS PROD_KOR_NAME0
      ,A16.PROD_ENG_NAME                                               AS PROD_ENG_NAME0
      ,A16.SORT_ORDER                                                  AS SORT_ORDER3
      ,A14.UP_PROD_CD                                                  AS PROD_CD1
      ,A15.PROD_KOR_NAME                                               AS PROD_KOR_NAME1
      ,A15.PROD_ENG_NAME                                               AS PROD_ENG_NAME1
      ,A15.SORT_ORDER                                                  AS SORT_ORDER4
      ,A12.USR_PROD1_LAST_CD                                           AS PROD_CD2
      ,A14.PROD_KOR_NAME                                               AS PROD_KOR_NAME2
      ,A14.PROD_ENG_NAME                                               AS PROD_ENG_NAME2
      ,A14.SORT_ORDER                                                  AS SORT_ORDER5
      ,SUM(A11.SALES_QTY)                                              AS SALES_QTY
      ,SUM(A11.NSALES_AMT)                                             AS NSALES_AMT
      ,(SUM(A11.FIX_COGS_AMT) + SUM(A11.VAR_COGS_AMT))                 AS COGS_AMT
      ,SUM(A11.GROSS_SALES_AMT)                                        AS GROSS_SALES_AMT
      ,SUM(A11.MGNL_PRF_AMT)                                           AS MGNL_PRF_AMT
      ,SUM(A11.OI_AMT)                                                 AS OI_AMT
      ,SUM(A11.SALES_DEDUCT_AMT)                                       AS SALES_DEDUCT_AMT
      ,(SUM(A11.FIX_SELL_ADM_EXP_AMT) + SUM(A11.VAR_SELL_ADM_EXP_AMT)) AS SELL_ADM_EXP_AMT
FROM   NPT_APP.NV_DWW_CONSLD_BEP_SUMM_S  A11
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_MDL_PERIOD_H  A12
       ON   A11.MDL_SFFX_CD = A12.MDL_SFFX_CD
       AND  A11.SUBSDR_CD   = A12.SUBSDR_CD
       LEFT OUTER JOIN  NPT_DW_MGR.TB_DWD_SUBSDR_MDL_PERIOD_H  A13
       ON   A11.ACCTG_YYYYMM = A13.ACCTG_YYYYMM
       AND  A11.MDL_SFFX_CD  = A13.MDL_SFFX_CD
       AND  A11.SUBSDR_CD    = A13.SUBSDR_CD
       LEFT OUTER JOIN  V_PROD_M  A14
       ON   A12.USR_PROD1_LAST_CD = A14.PROD_CD
       AND  A14.PROD_LVL_CD       = '4'
       LEFT OUTER JOIN  V_PROD_M  A15
       ON   A14.UP_PROD_CD   = A15.PROD_CD
       AND  A15.PROD_LVL_CD  = '3'
       LEFT OUTER JOIN  V_PROD_M  A16
       ON   A15.UP_PROD_CD  = A16.PROD_CD
       AND  A16.PROD_LVL_CD = '2'
       LEFT OUTER JOIN  NPT_APP.NV_DWD_DIV_LEAF_M  A17
       ON   A11.DIV_CD = A17.DIV_CD
       LEFT OUTER JOIN  NPT_APP.NV_DWD_02_GRD_CD  A18
       ON   A13.GRD_CD = A18.ATTRIBUTE_CD
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_M  A19
       ON   A11.PRODUCTION_SUBSDR_CD = A19.SUBSDR_CD
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SCENARIO_TYPE_M  A110
       ON   A11.SCENARIO_TYPE_CD = A110.SCENARIO_TYPE_CD
       LEFT OUTER JOIN  NPT_APP.NV_DWD_SUBSDR_M  A111
       ON   A11.SUBSDR_CD = A111.SUBSDR_CD
       LEFT OUTER JOIN  NPT_APP.NV_DWD_MGT_ORG_RNR_M  A112
       ON   A11.SUBSDR_RNR_CD = A112.MGT_ORG_CD
       LEFT OUTER JOIN  V_PROD_M  A113
       ON   A16.UP_PROD_CD   = A113.PROD_CD
       AND  A113.PROD_LVL_CD = '1'
WHERE  A11.ACCTG_YYYYMM = '201508' -- &IV_YYYYMM
AND    A11.SCENARIO_TYPE_CD      = 'AC0'
AND    A11.CONSLD_SALES_MDL_FLAG = 'Y'
AND    A11.CURRM_ACCUM_TYPE_CD   = 'CURRM'
AND    A11.VRNC_ALC_INCL_EXCL_CD = 'INCL'
AND    A11.CURRENCY_CD           = 'USD'
AND    A11.MDL_SFFX_CD NOT LIKE 'VM-%.CPS'
GROUP BY  A11.ACCTG_YYYYMM
         ,A11.DIV_CD
         ,A17.SCRN_DSPL_SEQ
         ,A17.DIV_SHRT_NAME
         ,A11.SUBSDR_RNR_CD
         ,A112.MGT_ORG_SHRT_NAME
         ,A112.SORT_ORDER
         ,A11.SUBSDR_CD
         ,A111.SUBSDR_SHRT_NAME
         ,A111.SORT_ORDER
         ,A11.PRODUCTION_SUBSDR_CD
         ,A19.SUBSDR_SHRT_NAME
         ,A19.SORT_ORDER
         ,A11.SCENARIO_TYPE_CD
         ,A110.SCENARIO_TYPE_NAME
         ,A110.SORT_ORDER
         ,A16.UP_PROD_CD
         ,A113.PROD_KOR_NAME
         ,A113.PROD_ENG_NAME
         ,A113.SORT_ORDER
         ,A15.UP_PROD_CD
         ,A16.PROD_KOR_NAME
         ,A16.PROD_ENG_NAME
         ,A16.SORT_ORDER
         ,A14.UP_PROD_CD
         ,A15.PROD_KOR_NAME
         ,A15.PROD_ENG_NAME
         ,A15.SORT_ORDER
         ,A12.USR_PROD1_LAST_CD
         ,A14.PROD_KOR_NAME
         ,A14.PROD_ENG_NAME
         ,A14.SORT_ORDER
ORDER BY  A17.SCRN_DSPL_SEQ
         ,A113.SORT_ORDER
         ,A16.SORT_ORDER
         ,A15.SORT_ORDER
         ,A14.SORT_ORDER
;
