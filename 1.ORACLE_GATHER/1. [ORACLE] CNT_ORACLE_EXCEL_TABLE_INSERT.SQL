INSERT INTO NPT_RS_MGR.TB_RS_EXCEL_UPLD_DATA_D
(
prcs_seq, 
rs_module_cd, 
rs_clsf_id, 
rs_type_cd, 
rs_type_name, 
div_cd, 
base_yyyymmdd, 
cd_desc, 
sort_seq, 
use_flag, 
attribute1_value, 
attribute2_value, 
attribute3_value, 
attribute4_value, 
attribute5_value, 
attribute6_value, 
attribute7_value, 
attribute8_value, 
attribute9_value, 
attribute10_value, 
attribute11_value, 
attribute12_value, 
attribute13_value, 
attribute14_value, 
attribute15_value, 
attribute16_value, 
attribute17_value, 
attribute18_value, 
attribute19_value, 
attribute20_value, 
attribute21_value, 
attribute22_value, 
attribute23_value
)
select  /*+ use_hash(a11 a12 a13 a14 a15 a16 a17 a18 a19 a110 a111 a112 a113 a114 a115 a116 a117 a118 a119 a120 a121 a122 a123 a124 a125 a126 a127 a128 a129 a130 a131 a132 a133 a134 a135 a136 a137 a138 a139 a140) */
'1600'                --prcs_seq
,'ARES'                --rs_module_cd
,'BEP_SMART'           --rs_clsf_id
,'BEP_SMART_CNT'       --rs_type_cd
,'BEP_SMART_CNT'       --rs_type_name
,DECODE(a13.UP_PROD_CD,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')     --div_cd
,a11.ACCTG_YYYYMM                                                 --base_yyyymmdd
,a112.PROD_ENG_NAME                                               --cd_desc
,a111.SCRN_DSPL_SEQ                                               --sort_seq
,'Y'                                                              --use_flag
,  a11.SCENARIO_TYPE_CD           SCENARIO_TYPE_CD,
  DECODE(a13.UP_PROD_CD,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')                     DIV_CD,
  a11.SUBSDR_CD                  SUBSDR_CD,
  a19.SUBSDR_SHRT_NAME           NEW_SUBSDR_SHRT_NAME,
  a11.ZONE_RNR_CD                ZONE_CD,
  a111.ZONE_NAME                 ZONE_NAME,
  a111.SCRN_DSPL_SEQ             SCRN_DSPL_SEQ,
  a11.CNTRY_RNR_CD               CNTRY_RNR_CD,
  a15.CNTRY_NAME                 CNTRY_NAME,
  a11.ACCTG_YYYYMM               BASE_YYYYMM,
  a11.PLN_YYYYMM                 PLAN_YYYYMM,
  a13.UP_PROD_CD                 PROD_CD,
  a112.PROD_ENG_NAME             PROD_ENG_NAME,
  sum(DECODE(a11.CURRENCY_CD,'USD',a11.NSALES_AMT))        NSALES_USD_AMT,
  sum(DECODE(a11.CURRENCY_CD,'KRW',a11.NSALES_AMT))        NSALES_KRW_AMT,
  sum(DECODE(a11.CURRENCY_CD,'USD',a11.SALES_DEDUCT_AMT))  SALES_DEDUCT_USD_AMT,
  sum(DECODE(a11.CURRENCY_CD,'KRW',a11.SALES_DEDUCT_AMT))  SALES_DEDUCT_KRW_AMT,
  sum(DECODE(a11.CURRENCY_CD,'USD',a11.OTH_SALES_AMT))     OTH_SALES_USD_AMT,
  sum(DECODE(a11.CURRENCY_CD,'KRW',a11.OTH_SALES_AMT))     OTH_SALES_KRW_AMT,
  sum(DECODE(a11.CURRENCY_CD,'USD',a11.MGNL_PRF_AMT))      MGNL_PRF_USD_AMT,
  sum(DECODE(a11.CURRENCY_CD,'KRW',a11.MGNL_PRF_AMT))      MGNL_PRF_KRW_AMT,
  sum(DECODE(a11.CURRENCY_CD,'USD',a11.OI_AMT))            COI_USD_AMT,
  sum(DECODE(a11.CURRENCY_CD,'KRW',a11.OI_AMT))            COI_KRW_AMT
from  NPT_APP.NV_DWW_CON_BEP_SUMM_DW_S  a11
  left outer join  NPT_APP.NV_DWD_SUBSDR_MDL_PERIOD_H  a12
    on   (a11.MDL_SFFX_CD = a12.MDL_SFFX_CD and
  a11.SUBSDR_CD = a12.SUBSDR_CD)
  left outer join  NPT_APP.NV_DWD_RPT_PROD4_M  a13
    on   (a12.USR_PROD1_LAST_CD = a13.PROD_CD)
  left outer join  NPT_APP.NV_DWD_PRFT_CONFM_SCENARIO_H  a14
    on   (a11.ACCTG_YYYYMM = a14.ACCTG_YYYYMM and
  a11.DIV_CD = a14.DIV_CD and
  a11.SCENARIO_TYPE_CD = a14.SCENARIO_TYPE_CD)
  left outer join  NPT_APP.NV_DWD_CNTRY_M  a15
    on   (a11.CNTRY_RNR_CD = a15.CNTRY_CD)
  left outer join  NPT_APP.NV_DWD_DIV_LEAF_M  a16
    on   (a11.DIV_CD = a16.DIV_CD)
  left outer join  NPT_APP.NV_DWD_SUBSDR_M  a17
    on   (a11.PRODUCTION_SUBSDR_CD = a17.SUBSDR_CD)
  left outer join  NPT_APP.NV_DWD_SCENARIO_TYPE_M  a18
    on   (a11.SCENARIO_TYPE_CD = a18.SCENARIO_TYPE_CD)
  left outer join  NPT_APP.NV_DWD_SUBSDR_M  a19
    on   (a11.SUBSDR_CD = a19.SUBSDR_CD)
  left outer join  NPT_APP.NV_DWD_MGT_ORG_RNR_M  a110
    on   (a11.SALES_SUBSDR_RNR_CD = a110.MGT_ORG_CD)
  left outer join  NPT_APP.NV_DWD_ZONE_M  a111
    on   (a11.ZONE_RNR_CD = a111.ZONE_CD)
  left outer join  NPT_APP.NV_DWD_RPT_PROD3_M  a112
    on   (a13.UP_PROD_CD = a112.PROD_CD)
where  (a11.ACCTG_YYYYMM between '201301' and '201601'
 and a11.DIV_CD in ('GNT')
 and a11.CONSLD_SALES_MDL_FLAG in ('Y')
 and a11.CURRM_ACCUM_TYPE_CD in ('CURRM')
 and a11.VRNC_ALC_INCL_EXCL_CD in ('INCL')
 and a11.CURRENCY_CD in ('USD','KRW')
 and a14.CONFIRM_FLAG = 'Y')
group by  
  a11.SCENARIO_TYPE_CD 
 ,DECODE(a13.UP_PROD_CD,'GNT_L3_1','GNTCS','GNT_L3_5','GNTHT')           
 ,a11.SUBSDR_CD        
 ,a19.SUBSDR_SHRT_NAME 
 ,a11.ZONE_RNR_CD      
 ,a111.ZONE_NAME       
 ,a111.SCRN_DSPL_SEQ   
 ,a11.CNTRY_RNR_CD     
 ,a15.CNTRY_NAME       
 ,a11.ACCTG_YYYYMM 
 ,a11.PLN_YYYYMM    
 ,a13.UP_PROD_CD       
 ,a112.PROD_ENG_NAME   
