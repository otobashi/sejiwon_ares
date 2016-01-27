

/* 2015.07.04 ML 추출 */


delete 
from TB_RS_ADJ_ML_H 
where base_yyyymm in ( '201509', '201510')
;


INSERT INTO TB_RS_ADJ_ML_H

(
CMPNY_CD ,
RS_DIV_CD,
KPI_CD,
BASE_YYYYMM,
BASE_MMWEEK,
PLN_YYYYMM,
SCENARIO_TYPE_CD,
CURRM_KRW_AMT,
CURRM_USD_AMT,
CREATION_DATE,
CREATION_USR_ID,
LAST_UPD_DATE,
LAST_UPD_USR_ID
)


select
/*
       d.cmpny_cd as CMPNY_CD,
       case
            when a.div_cd = 'ALL' then
              'GBU'
            else
              a.div_cd
       end as RS_DIV_CD, 
*/
 a.div_cd as CMPNY_CD,
 a.div_cd as RS_DIV_CD,
 decode(a.ml_acct_cat_cd, 'NSALES', 'SALE', a.ml_acct_cat_cd) as KPI_CD,
 b.pln_yyyymm as BASE_YYYYMM,
 b.week_no as BASE_MMWEEK,
 a.pln_yyyymm as PLN_YYYYMM,
 CASE
   WHEN a.pln_yyyymm = b.pln_yyyymm THEN
    'PR0'
   WHEN a.pln_yyyymm =
        to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN
    'PR1'
   WHEN a.pln_yyyymm =
        to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN
    'PR2'
   WHEN a.pln_yyyymm =
        to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN
    'PR3'
   ELSE
    null
 END AS SCENARIO_TYPE_CD,
 a.krw_amt as CURRM_KRW_AMT,
 a.usd_amt as CURRM_USD_AMT,
 trunc(a.creation_date),
 'ares',
 trunc(a.last_upd_date),
 'ares'
  FROM tb_rfe_ml_upld_rslt_s a, TB_RFE_ML_WEEK_M b
--TB_RS_DIV_H D

--WHERE a.PLN_YYYYWEEK  between  '201519' and  '201522'
 where 1=1 --a.pln_yyyyweek IN ('201523', '201524')
   and b.pln_yyyymm IN ( '201511','201512')
   --AND A.DIV_CD = 'CMT'
   --and  b.week_no in ('4','5')
   and a.data_type_cd = 'DIV'
   and a.pln_yyyyweek = b.pln_yyyyweek
  -- and a.ml_acct_cat_cd = 'COI'
      and b.pln_yyyymm <= a.pln_yyyymm
      and 
      a.pln_yyyymm <
        to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 4), 'YYYYMM')      
   and a.krw_amt <> 0;
   


/* 해외법인용 ML */

SELECT
 'GBU' AS cmpny_cd
,'GBU' AS rs_div_cd
,decode(a.ml_acct_cat_cd, 'NSALES', 'SALE', a.ml_acct_cat_cd) AS kpi_cd
,b.pln_yyyymm AS base_yyyymm
,b.week_no AS base_mmweek
,a.pln_yyyymm AS pln_yyyymm
,CASE
     WHEN a.pln_yyyymm = b.pln_yyyymm THEN
      'PR0'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN
      'PR1'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN
      'PR2'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN
      'PR3'
     ELSE
      NULL
 END AS scenario_type_cd
,a.subsdr_cd
,c.subsdr_shrt_name
,sum(a.krw_amt) AS currm_krw_amt
,sum(a.usd_amt) AS currm_usd_amt
, trunc(a.creation_date)
, 'ares'
, trunc(a.last_upd_date)
, 'ares'
FROM   tb_rfe_ml_upld_rslt_s a
      ,tb_rfe_ml_week_m      b
      ,(
        SELECT DISTINCT S.SUBSDR_SHRT_NAME, S.SUBSDR_CD, S.SUBSDR_KOR_NAME
        FROM TB_CM_SUBSDR_PERIOD_H S
        WHERE S.MGT_TYPE_CD = 'CM'
        AND   S.ACCTG_YYYYMM = '*'
        AND   S.ACCTG_WEEK = '*'
        AND   S.TEMP_FLAG = 'N' ) c

--TB_RS_DIV_H D

--WHERE a.PLN_YYYYWEEK  between  '201519' and  '201522'
WHERE  1 = 1 --a.pln_yyyyweek IN ('201523', '201524')
AND    b.pln_yyyymm in ( '201511','201512')
AND    a.div_cd IN ('HE', 'MC', 'HNA', 'VC', 'EBC', 'IC')
      --and  b.week_no in ('4','5')
AND    a.data_type_cd = 'SUBSDR'
AND    a.pln_yyyyweek = b.pln_yyyyweek
      -- and a.ml_acct_cat_cd = 'COI'
AND    b.pln_yyyymm <= a.pln_yyyymm
AND    a.pln_yyyymm < to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 4), 'YYYYMM')
AND    a.krw_amt <> 0
and    c.subsdr_cd = a.subsdr_cd(+)
group by a.ml_acct_cat_cd
,b.pln_yyyymm 
,b.week_no
,a.pln_yyyymm 
,CASE
     WHEN a.pln_yyyymm = b.pln_yyyymm THEN
      'PR0'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN
      'PR1'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN
      'PR2'
     WHEN a.pln_yyyymm = to_char(add_months(to_date(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN
      'PR3'
     ELSE
      NULL
 END 
,a.subsdr_cd
,c.subsdr_shrt_name
, trunc(a.creation_date)
, trunc(a.last_upd_date)

;
   
