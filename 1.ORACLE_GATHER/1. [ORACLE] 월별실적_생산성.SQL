INSERT INTO npt_rs_mgr.tb_rs_excel_upld_data_d
(
       prcs_seq         
      ,rs_module_cd     
      ,rs_clsf_id       
      ,rs_type_cd       
      ,rs_type_name     
      ,div_cd           
      ,base_yyyymmdd    
      ,cd_desc          
      ,sort_seq         
      ,use_flag         
      ,attribute1_value 
      ,attribute2_value 
      ,attribute3_value 
      ,attribute4_value 
      ,attribute5_value 
      ,attribute6_value 
      ,attribute7_value 
      ,attribute8_value 
      ,attribute9_value 
      ,attribute10_value 
      ,attribute11_value 
)
SELECT '1620'                AS prcs_seq                                 
      ,'ARES'                AS rs_module_cd                             
      ,'BEP_SMART'           AS rs_clsf_id                               
      ,'BEP_SMART_HR'        AS rs_type_cd                               
      ,'BEP_SMART_HR'        AS rs_type_name                             
      ,a.div_cd              AS div_cd                                   
      ,a.base_yyyymm         AS base_yyyymmdd                            
      ,NULL                  AS cd_desc                                  
      ,NULL                  AS sort_seq                                 
      ,'Y'                   AS use_flag                                 
      ,a.base_yyyymm
      ,a.scenario_type_cd
      ,a.kpi_type_cd
      ,a.div_cd
      ,a.subsdr_cd
      ,a.acct_cd
      ,b.acct_nm
      ,SUM(a.currm_krw_amt)  AS currm_krw_amt
      ,SUM(a.currm_usd_amt)  AS currm_usd_amt
      ,SUM(a.accum_krw_amt)  AS accum_krw_amt
      ,SUM(a.accum_usd_amt)  AS accum_usd_amt
FROM   tb_dsm_kpi_div_s a
      ,(
        SELECT acct_cd, acct_desc AS acct_nm
        FROM   tb_dsd_acct_m
        WHERE  acct_gr_cd = 'HR'
        AND    acct_desc IN ('(HR) 원당매출액','(HR) 원당매출액_사내도급제외','(HR) 인당매출액','(HR) 인당매출액_사내도급제외','(HR) 인원수','(HR) 인원수_사내도급제외'
                          ,'(HR) 인원수_FSE','(HR) 인원수_ISE ','(HR) 인원수_임시직 ','(HR) 인원수_사내도급 ','(HR) 인원수_ISE_영업/마케팅','(HR) 인원수_공통인원'
                          ,'(HR) 인건비(HR생산성 기준)','(HR) 인건비_사내도급제외')
      ) b            
WHERE  a.base_yyyymm BETWEEN '201301' AND '201601'
AND    a.acct_cd = b.acct_cd
AND    a.subsdr_cd = 'EEUK'
AND    a.div_cd IN ('GLT','GTT','MST','CNT','DFT','DGT','DMT','CMS')
AND    a.currency_cd = '*'
AND    a.scenario_type_cd = 'AC0'
AND    a.kpi_type_cd in ('HR', 'HR_CNT')
GROUP BY a.base_yyyymm
        ,a.scenario_type_cd
        ,a.kpi_type_cd
        ,a.div_cd
        ,a.subsdr_cd
        ,a.acct_cd
        ,b.acct_nm
;
