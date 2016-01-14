SELECT *
FROM   TB_CM_MGT_ORG_M
;

select *
from NPT_APP.NV_DWD_MGT_ORG_M
WHERE  MGT_ORG_CD NOT IN ('#','*')
;

select MGT_ORG_CD, MGT_ORG_SHRT_NAME
from NPT_APP.NV_DWD_MGT_ORG_M
WHERE  MGT_ORG_CD NOT IN ('#','*')
;

select MGT_ORG_CD, MGT_ORG_SHRT_NAME
from NPT_APP.NV_DWD_MGT_ORG_M
WHERE  MGT_ORG_CD IN (
SELECT DISTINCT UP_MGT_ORG_CD
FROM   NPT_APP.NV_DWD_MGT_ORG_M)
;

SELECT SUBSDR_TYPE_CD,COUNT(*)
FROM TB_CM_SUBSDR_PERIOD_H S
/*WHERE S.MGT_TYPE_CD = 'CM'
AND   S.ACCTG_YYYYMM = '*'
AND   S.ACCTG_WEEK = '*'
AND   S.TEMP_FLAG = 'N'*/
GROUP BY SUBSDR_TYPE_CD
;
-- 제판법인
SELECT 
subsdr_cd, 
subsdr_shrt_name, 
subsdr_name, 
subsdr_kor_name, 
cntry_cd, 
zone_cd, 
regn_cd, 
loc_currency_cd, 
mdms_subsdr_shrt_name, 
mdms_subsdr_eng_name, 
mdms_subsdr_kor_name, 
erp_shrt_name, 
use_flag, 
sort_order
FROM   TB_CM_SUBSDR_PERIOD_H
WHERE  SUBSDR_TYPE_CD = 'T'
AND    MGT_TYPE_CD = 'CM'
AND    ACCTG_YYYYMM = '*'
AND    ACCTG_WEEK = '*'
AND    TEMP_FLAG = 'N'
UNION ALL
-- 판매법인
SELECT 
subsdr_cd, 
subsdr_shrt_name, 
subsdr_name, 
subsdr_kor_name, 
cntry_cd, 
zone_cd, 
regn_cd, 
loc_currency_cd, 
mdms_subsdr_shrt_name, 
mdms_subsdr_eng_name, 
mdms_subsdr_kor_name, 
erp_shrt_name, 
use_flag, 
sort_order
FROM   TB_CM_SUBSDR_PERIOD_H
WHERE  SUBSDR_TYPE_CD = 'S'
AND    MGT_TYPE_CD = 'CM'
AND    ACCTG_YYYYMM = '*'
AND    ACCTG_WEEK = '*'
AND    TEMP_FLAG = 'N'

;


SELECT *
FROM   TB_CM_MGT_ORG_M
