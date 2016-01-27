

              insert into npt_rs_mgr.TB_RS_EXCEL_UPLD_DATA_D
              (
              PRCS_SEQ  ,
              RS_MODULE_CD  ,
              RS_CLSF_ID  ,
              RS_TYPE_CD  ,
              RS_TYPE_NAME  ,
              DIV_CD  ,
              BASE_YYYYMMDD  ,
              CD_DESC  ,
              SORT_SEQ  ,
              USE_FLAG  ,
              ATTRIBUTE1_VALUE  ,
              ATTRIBUTE2_VALUE  ,
              ATTRIBUTE3_VALUE  ,
              ATTRIBUTE4_VALUE  ,
              ATTRIBUTE5_VALUE  ,
              ATTRIBUTE6_VALUE  ,
              ATTRIBUTE7_VALUE  ,
              ATTRIBUTE8_VALUE  ,
              ATTRIBUTE9_VALUE  ,
              ATTRIBUTE10_VALUE  ,
              ATTRIBUTE11_VALUE  ,
              ATTRIBUTE12_VALUE  ,
              ATTRIBUTE13_VALUE  ,
              ATTRIBUTE14_VALUE  ,
              ATTRIBUTE15_VALUE  ,
              ATTRIBUTE16_VALUE  ,
              ATTRIBUTE17_VALUE  ,
              ATTRIBUTE18_VALUE  ,
              ATTRIBUTE19_VALUE  ,
              ATTRIBUTE20_VALUE  ,
              ATTRIBUTE21_VALUE  ,
              ATTRIBUTE22_VALUE  ,
              ATTRIBUTE23_VALUE  ,
              ATTRIBUTE24_VALUE  ,
              ATTRIBUTE25_VALUE  ,
              ATTRIBUTE26_VALUE  ,
              ATTRIBUTE27_VALUE  ,
              ATTRIBUTE28_VALUE                
              )
--select  /*+ use_hash(a11 a12 a13 a14 a15 a16 a17 a18 a19 a110 a111 a112 a113 a114 a115 a116 a117 a118 a119) */ 
/* 20151201 ARES-SMART 통합 BEP 매출 추출 */
-- 실적 AC0

select /*+ parallel(32) */
  '1500' as            PRCS_SEQ  ,
  'ARES' as           RS_MODULE_CD  ,
  'BEP_SMART' as           RS_CLSF_ID  ,
  'BEP_SMART_DIV' as RS_TYPE_CD  ,
  'BEP_SMART_DIV' as RS_TYPE_NAME  ,
   a11.DIV_CD     as            DIV_CD  ,
   a11.ACCTG_YYYYMM as           BASE_YYYYMMDD  ,
   null as          CD_DESC  ,
   null as          SORT_SEQ  ,
  'Y'   as          USE_FLAG  ,  
  a11.SCENARIO_TYPE_CD  SCENARIO_TYPE_CD,
  a15.SCENARIO_TYPE_NAME  SCENARIO_TYPE_NAME,
  a15.SORT_ORDER  SORT_ORDER,
  a11.DIV_CD  DIV_CD,
  a14.SCRN_DSPL_SEQ  DIV_KOR_NAME,
  a14.DIV_SHRT_NAME  DIV_SHRT_NAME,
  a11.SALES_SUBSDR_RNR_CD  SUBSDR_RNR_CD,
  a17.MGT_ORG_SHRT_NAME  MGT_ORG_SHRT_NAME,
  a17.SORT_ORDER  SORT_ORDER0,
  a11.SUBSDR_CD  SUBSDR_CD0,
  a16.SUBSDR_SHRT_NAME  NEW_SUBSDR_SHRT_NAME,
  a16.SORT_ORDER  SORT1_ORDER,
  a11.ZONE_RNR_CD  ZONE_CD,
  a18.ZONE_NAME  ZONE_NAME,
  a18.SCRN_DSPL_SEQ  SCRN_DSPL_SEQ,
  a11.CNTRY_RNR_CD  CNTRY_RNR_CD,
  a13.CNTRY_NAME  CNTRY_NAME,
  a13.SORT_ORDER  SORT_ORDER1,
  a11.ACCTG_YYYYMM  BASE_YYYYMM,
  a11.CURRENCY_CD,
  a11.CURRM_ACCUM_TYPE_CD,
  sum(a11.SALES_QTY)  SALES_QTY,
  sum(a11.NSALES_AMT)  NSALES_AMT,
  sum(a11.RNR_SALES_AMT) RNR_SALES_AMT,
  sum(a11.GROSS_SALES_AMT)  GROSS_SALES_AMT,
  sum(a11.SALES_DEDUCT_AMT)  SALES_DEDUCT_AMT,
  sum(a11.MGNL_PRF_AMT)  MGNL_PRF_AMT,
  sum(a11.OI_AMT)  OI_AMT  
  
from  NPT_APP.NV_DWW_CON_BEP_SUMM_DW_S  a11
  left outer join  NPT_APP.NV_DWD_PRFT_CONFM_SCENARIO_H  a12
    on   (a11.ACCTG_YYYYMM = a12.ACCTG_YYYYMM and 
  a11.DIV_CD = a12.DIV_CD and 
  a11.SCENARIO_TYPE_CD = a12.SCENARIO_TYPE_CD)
  left outer join  NPT_APP.NV_DWD_CNTRY_M  a13
    on   (a11.CNTRY_RNR_CD = a13.CNTRY_CD)
  left outer join  NPT_APP.NV_DWD_DIV_LEAF_M  a14
    on   (a11.DIV_CD = a14.DIV_CD)
  left outer join  NPT_APP.NV_DWD_SCENARIO_TYPE_M  a15
    on   (a11.SCENARIO_TYPE_CD = a15.SCENARIO_TYPE_CD)
  left outer join  NPT_APP.NV_DWD_SUBSDR_M  a16
    on   (a11.SUBSDR_CD = a16.SUBSDR_CD)
  left outer join  NPT_APP.NV_DWD_MGT_ORG_RNR_M  a17
    on   (a11.SALES_SUBSDR_RNR_CD = a17.MGT_ORG_CD)
  left outer join  NPT_APP.NV_DWD_ZONE_M  a18
    on   (a11.ZONE_RNR_CD = a18.ZONE_CD)
where  (a11.SCENARIO_TYPE_CD in ('AC0','PR1') -- , 'PR2', 'PR3', 'PR4')
 and a11.ACCTG_YYYYMM between '201301' and '201303'
 and a11.CONSLD_SALES_MDL_FLAG in ('Y')
 and a11.CURRM_ACCUM_TYPE_CD in ('CURRM','ACCUM')
 and a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
 and a11.CURRENCY_CD in ('USD','KRW')
 and a12.CONFIRM_FLAG = 'Y')
group by  
  a11.ACCTG_YYYYMM,
  a11.SCENARIO_TYPE_CD,
  a15.SCENARIO_TYPE_NAME,
  a15.SORT_ORDER,
  a11.DIV_CD,
  a14.SCRN_DSPL_SEQ,
  a14.DIV_SHRT_NAME,
  a11.SALES_SUBSDR_RNR_CD,
  a17.MGT_ORG_SHRT_NAME,
  a17.SORT_ORDER,
  a11.SUBSDR_CD,
  a16.SUBSDR_SHRT_NAME,
  a16.SORT_ORDER,
  a11.ZONE_RNR_CD,
  a18.ZONE_NAME,
  a18.SCRN_DSPL_SEQ,
  a11.CNTRY_RNR_CD,
  a13.CNTRY_NAME,
  a13.SORT_ORDER,
  a11.ACCTG_YYYYMM,
  a11.CURRENCY_CD,
  a11.CURRM_ACCUM_TYPE_CD
  ;



select *
from npt_rs_mgr.tb_rs_clss_cd_m a
where a.cd_clsf_id = 'KPI_TYPE'
and   a.cd_id  IN ('SALE','MGN_PROFIT','COI','GROSS_SALE','SALES_DEDUCTION') 
;




/* 2015.12.07 수정 */

SELECT distinct a.base_yyyymmdd
FROM npt_rs_mgr.TB_RS_EXCEL_UPLD_DATA_D A
WHERE a.prcs_seq = '1500'
and   a.rs_module_cd = 'ARES'
and   a.rs_clsf_id = 'BEP_SMART'
and   a.rs_type_cd = 'BEP_SMART_DIV'
and   a.base_yyyymmdd between &iv_yyyymm and &iv_yyyymm_to
;


              insert into npt_rs_mgr.TB_RS_EXCEL_UPLD_DATA_D
              (
              PRCS_SEQ  ,
              RS_MODULE_CD  ,
              RS_CLSF_ID  ,
              RS_TYPE_CD  ,
              RS_TYPE_NAME  ,
              DIV_CD  ,
              BASE_YYYYMMDD  ,
              CD_DESC  ,
              SORT_SEQ  ,
              USE_FLAG  ,
              ATTRIBUTE1_VALUE  ,
              ATTRIBUTE2_VALUE  ,
              ATTRIBUTE3_VALUE  ,
              ATTRIBUTE4_VALUE  ,
              ATTRIBUTE5_VALUE  ,
              ATTRIBUTE6_VALUE  ,
              ATTRIBUTE7_VALUE  ,
              ATTRIBUTE8_VALUE  ,
              ATTRIBUTE9_VALUE  ,
              ATTRIBUTE10_VALUE  ,
              ATTRIBUTE11_VALUE  ,
              ATTRIBUTE12_VALUE  ,
              ATTRIBUTE13_VALUE  ,
              ATTRIBUTE14_VALUE  ,
              ATTRIBUTE15_VALUE  ,
              ATTRIBUTE16_VALUE  ,
              ATTRIBUTE17_VALUE  ,
              ATTRIBUTE18_VALUE  ,
              ATTRIBUTE19_VALUE  ,
              ATTRIBUTE20_VALUE  ,
              ATTRIBUTE21_VALUE  ,
              ATTRIBUTE22_VALUE  ,
              ATTRIBUTE23_VALUE  ,
              ATTRIBUTE24_VALUE  ,
              ATTRIBUTE25_VALUE  ,
              ATTRIBUTE26_VALUE  ,
              ATTRIBUTE27_VALUE  ,
              ATTRIBUTE28_VALUE  ,
              ATTRIBUTE29_VALUE  ,
              ATTRIBUTE30_VALUE  ,
              ATTRIBUTE31_VALUE  ,
              ATTRIBUTE32_VALUE  ,
              ATTRIBUTE33_VALUE  ,
              ATTRIBUTE34_VALUE  ,
              ATTRIBUTE35_VALUE  ,
              ATTRIBUTE36_VALUE  ,
              ATTRIBUTE37_VALUE  ,
              ATTRIBUTE38_VALUE  ,
              ATTRIBUTE39_VALUE  ,
              ATTRIBUTE40_VALUE  ,
              ATTRIBUTE41_VALUE  ,
              ATTRIBUTE42_VALUE  ,
              ATTRIBUTE43_VALUE  
                           
              )



 SELECT /*+ parallel(32) */
  '1500' as            PRCS_SEQ  ,
  'ARES' as           RS_MODULE_CD  ,
  'BEP_SMART' as           RS_CLSF_ID  ,
  'BEP_SMART_DIV' as RS_TYPE_CD  ,
  'BEP_SMART_DIV' as RS_TYPE_NAME  ,
   a11.DIV_CD     as            DIV_CD  ,
   a11.ACCTG_YYYYMM as           BASE_YYYYMMDD  ,
   null as          CD_DESC  ,
   null as          SORT_SEQ  ,
  'Y'   as          USE_FLAG  , 
   
   a11.scenario_type_cd scenario_type_cd
  ,a15.scenario_type_name scenario_type_name
  ,a15.sort_order sort_order
  ,a11.div_cd div_cd
  ,a14.scrn_dspl_seq div_kor_name
  ,a14.div_shrt_name div_shrt_name
  ,a11.sales_subsdr_rnr_cd subsdr_rnr_cd
  ,a17.mgt_org_shrt_name mgt_org_shrt_name
  ,a17.sort_order sort_order0
  ,a11.subsdr_cd subsdr_cd0
  ,a16.subsdr_shrt_name new_subsdr_shrt_name
  ,a16.sort_order sort1_order
  ,a11.zone_rnr_cd zone_cd
  ,a18.zone_name zone_name
  ,a18.scrn_dspl_seq scrn_dspl_seq
  ,a11.cntry_rnr_cd cntry_rnr_cd
  ,a13.cntry_name cntry_name
  ,a13.sort_order sort_order1
  ,a11.acctg_yyyymm base_yyyymm
  --,a11.currency_cd
  --,a11.currm_accum_type_cd
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then nsales_amt else 0 end) as curr_nsales_amt_krw
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then rnr_sales_amt else 0 end) as curr_rnr_sales_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then gross_sales_amt else 0 end) as curr_gross_sales_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then sales_deduct_amt else 0 end) as curr_sales_deduct_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then mgnl_prf_amt else 0 end) as curr_mgnl_prf_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'CURRM' then oi_amt else 0 end) as curr_oi_amt_krw  

  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then nsales_amt else 0 end) as curr_nsales_amt_usd
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then rnr_sales_amt else 0 end) as curr_rnr_sales_amt_usd
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then gross_sales_amt else 0 end) as curr_gross_sales_amt_usd  
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then sales_deduct_amt else 0 end) as curr_sales_deduct_amt_usd 
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then mgnl_prf_amt else 0 end) as curr_mgnl_prf_amt_usd 
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'CURRM' then oi_amt else 0 end) as curr_oi_amt_usd   
  
  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then nsales_amt else 0 end) as accu_nsales_amt_krw
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then rnr_sales_amt else 0 end) as accu_rnr_sales_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then gross_sales_amt else 0 end) as accu_gross_sales_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then sales_deduct_amt else 0 end) as accu_sales_deduct_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then mgnl_prf_amt else 0 end) as accu_mgnl_prf_amt_krw  
  ,sum(case when a11.CURRENCY_CD = 'KRW' and a11.currm_accum_type_cd = 'ACCUM' then oi_amt else 0 end) as accu_oi_amt_krw    
  
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then nsales_amt else 0 end) as accu_nsales_amt_usd
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then rnr_sales_amt else 0 end) as accu_rnr_sales_amt_usd  
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then gross_sales_amt else 0 end) as accu_gross_sales_amt_usd 
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then sales_deduct_amt else 0 end) as accu_sales_deduct_amt_usd 
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then mgnl_prf_amt else 0 end) as accu_mgnl_prf_amt_usd
  ,sum(case when a11.CURRENCY_CD = 'USD' and a11.currm_accum_type_cd = 'ACCUM' then oi_amt else 0 end) as accu_oi_amt_usd      
  
  
  FROM   npt_app.nv_dww_con_bep_summ_dw_s a11
  LEFT   OUTER JOIN npt_app.nv_dwd_prft_confm_scenario_h a12
  ON     (a11.acctg_yyyymm = a12.acctg_yyyymm AND a11.div_cd = a12.div_cd AND a11.scenario_type_cd = a12.scenario_type_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_cntry_m a13
  ON     (a11.cntry_rnr_cd = a13.cntry_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_div_leaf_m a14
  ON     (a11.div_cd = a14.div_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_scenario_type_m a15
  ON     (a11.scenario_type_cd = a15.scenario_type_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_subsdr_m a16
  ON     (a11.subsdr_cd = a16.subsdr_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_mgt_org_rnr_m a17
  ON     (a11.sales_subsdr_rnr_cd = a17.mgt_org_cd)
  LEFT   OUTER JOIN npt_app.nv_dwd_zone_m a18
  ON     (a11.zone_rnr_cd = a18.zone_cd)
  WHERE  (a11.scenario_type_cd in ('AC0','PR1') -- , 'PR2', 'PR3', 'PR4')
         AND a11.acctg_yyyymm between &iv_yyyymm and &iv_yyyymm_to
         AND a11.consld_sales_mdl_flag IN ('Y') 
         AND a11.currm_accum_type_cd IN ('CURRM', 'ACCUM') 
         AND a11.vrnc_alc_incl_excl_cd IN ('INCL') 
         AND a11.currency_cd IN ('USD', 'KRW') 
         AND a12.confirm_flag = 'Y')
  GROUP  BY a11.acctg_yyyymm
           ,a11.scenario_type_cd
           ,a15.scenario_type_name
           ,a15.sort_order
           ,a11.div_cd
           ,a14.scrn_dspl_seq
           ,a14.div_shrt_name
           ,a11.sales_subsdr_rnr_cd
           ,a17.mgt_org_shrt_name
           ,a17.sort_order
           ,a11.subsdr_cd
           ,a16.subsdr_shrt_name
           ,a16.sort_order
           ,a11.zone_rnr_cd
           ,a18.zone_name
           ,a18.scrn_dspl_seq
           ,a11.cntry_rnr_cd
           ,a13.cntry_name
           ,a13.sort_order
           ,a11.acctg_yyyymm
           --,a11.currency_cd
           --,a11.currm_accum_type_cd
           ;
