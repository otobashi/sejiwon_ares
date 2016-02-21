-- 7.Most Likely
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
      UNION ALL
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
      UNION ALL
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU