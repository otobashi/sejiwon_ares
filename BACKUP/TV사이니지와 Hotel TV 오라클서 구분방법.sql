   -- 사업군 분류에서 Commercial TV(GNT)만 가져옴. 그리고, 분리 비율 PL4 추가
        TEMPC (DIV_CD, BIZ_TYPE, BIZ_NAME, RULE_TYPE_CD, PROD_LVL3_CD, PROD_LVL4_CD, MDL_CD, SALES_MDL_SFFX_CD, MDL_SFFX_CD, ENABLE_FLAG, APPLY_FLAG) AS
        (
          SELECT A.DIV_CD, A.BIZ_TYPE, B.prod_kor_name AS BIZ_NAME, A.RULE_TYPE_CD, A.PROD_LVL3_CD, A.PROD_LVL4_CD, A.MDL_CD,
                 A.SALES_MDL_SFFX_CD, A.MDL_SFFX_CD, A.ENABLE_FLAG, A.APPLY_FLAG
          FROM   (
                  SELECT DIV_CD, ATTRIBUTE_VALUE AS BIZ_TYPE, RULE_TYPE_CD, PROD_LVL3_CD, PROD_LVL4_CD, MDL_CD, SALES_MDL_SFFX_CD, MDL_SFFX_CD, ENABLE_FLAG, APPLY_FLAG
                  FROM   NV_CM_FORML_RULE_MDL_NEW_H
                  WHERE  ATTRIBUTE_NAME = 'BIZ_TYPE_LEVEL'
                  AND    DIV_CD = 'GNT'
                  -- Commercail TV 분리 PL4
                  UNION ALL
                  SELECT 'GNT' DIV_CD, 'BIZ_B3_L4_GNT_2' AS BIZ_TYPE, 'PROD_LVL4' RULE_TYPE_CD, 'CSXXXX' PROD_LVL3_CD, 'CSXXXXXX' PROD_LVL4_CD, '*' MDL_CD,
                         '*' SALES_MDL_SFFX_CD, '*' MDL_SFFX_CD, 'Y' ENABLE_FLAG, 'Y' APPLY_FLAG
                  FROM DUAL
                  UNION ALL
                  SELECT 'GNT' DIV_CD, 'BIZ_B3_L4_GNT_3' AS BIZ_TYPE, 'PROD_LVL4' RULE_TYPE_CD, 'HTXXXX' PROD_LVL3_CD, 'HTXXXXXX' PROD_LVL4_CD, '*' MDL_CD,
                         '*' SALES_MDL_SFFX_CD, '*' MDL_SFFX_CD, 'Y' ENABLE_FLAG, 'Y' APPLY_FLAG
                  FROM DUAL
                 ) A
                left outer join  NPT_APP.NV_DWD_BIZ_TYPE_PROD4_M  B
                on    A.BIZ_TYPE = B.PROD_CD
        )
