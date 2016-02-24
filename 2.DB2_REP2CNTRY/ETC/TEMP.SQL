      SELECT 'Most Likely '||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.APPLY_YYYYMM       AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date('20160115', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                                        ,ATTRIBUTE2 AS BASE_YYYYMM
                                        ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                                        ,ROWNUMBER() OVER() AS SEQ
                                  FROM   IPTDW.IPTDW_RES_DIM_CODES
                                  WHERE  CODE_TYPE = 'SMART_WEEK'
                                  AND    TO_CHAR(to_date('20160115', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                                  )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0','PR1','PR2','PR3')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EHAP'
      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU