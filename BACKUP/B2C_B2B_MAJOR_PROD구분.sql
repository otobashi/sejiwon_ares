SELECT *
FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
WHERE  CODE_TYPE = 'SMR_PROD_MST'
AND    USE_FLAG  = 'Y'
AND    ATTRIBUTE2 = 'B2B_DIV'
--AND    ATTRIBUTE1 IN ('GLT_L1_1','MST_L1_6','CNT_L1_1','DFT_L1_1','DGT_L1_3')
--AND    ATTRIBUTE1 IN ('SDT_L1_1')
WITH UR;


UPDATE IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
SET    USE_FLAG  = 'N'
WHERE  CODE_TYPE = 'SMR_PROD_MST'
AND    USE_FLAG  = 'Y'
AND    ATTRIBUTE2 = 'B2B_DIV'
--AND    ATTRIBUTE1 IN ('GLT_L1_1','MST_L1_6','CNT_L1_1','DFT_L1_1','DGT_L1_3')
AND    ATTRIBUTE1 IN ('SDT_L1_1')
WITH UR;