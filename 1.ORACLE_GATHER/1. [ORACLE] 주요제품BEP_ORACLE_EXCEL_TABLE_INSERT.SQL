-- 201512
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
           SELECT '1700'                AS prcs_seq                                 
                 ,'ARES'                AS rs_module_cd                             
                 ,'BEP_SMART'           AS rs_clsf_id                               
                 ,'BEP_SMART_PROD_MMGN' AS rs_type_cd                               
                 ,'BEP_SMART_PROD_MMGN' AS rs_type_name                             
                 ,SUBSTR(a.prod_cd,1,3) AS prod_cd                                   
                 ,a.base_yyyymm         AS base_yyyymmdd                            
                 ,a.prod_cd             AS cd_desc                                  
                 ,NULL                  AS sort_seq                                 
                 ,'Y'                   AS use_flag                                 
                 ,a.base_yyyymm
                 ,a.scenario_type_cd
                 ,a.kpi_type_cd
                 ,a.prod_cd
                 ,a.subsdr_cd
                 ,a.acct_cd
                 ,b.acct_nm
                 ,SUM(a.currm_krw_amt)  AS currm_krw_amt
                 ,SUM(a.currm_usd_amt)  AS currm_usd_amt
                 ,SUM(a.accum_krw_amt)  AS accum_krw_amt
                 ,SUM(a.accum_usd_amt)  AS accum_usd_amt
           FROM   tb_dsm_kpi_prod_s a
                 ,(
                   SELECT acct_cd, acct_desc AS acct_nm
                   FROM   tb_dsd_acct_m
                   WHERE  ACCT_GR_CD = 'BEP'
                   AND    acct_cd in ( 'BEP20000000'  -- 순매출
                                      ,'BEP20070000'  -- 가격성판촉비
                                      ,'BEP50000000'  -- 한계이익
                                      ,'BEP50000000%' -- 한계이익율
                                      ,'BEP40010400'  -- 광고선전비금액
                                      ,'BEP40010500'  -- 판매촉진비
                                      ,'BEP60000000'  -- 영업이익
                                      ,'BEP5000SALE'  -- 한계적자매출금액
                                      ,'BEP50000000R' -- 한계적자 모델 매출비중
                                      ,'BEP5000MGNL'  -- 한계적자금액
                                     )
                 ) b            
           WHERE  a.base_yyyymm = '201512'
           AND    a.acct_cd = b.acct_cd
           AND    a.scenario_type_cd = 'AC0'
           AND    a.kpi_type_cd = 'BEP'
           GROUP BY SUBSTR(a.prod_cd,1,3)
                   ,a.base_yyyymm
                   ,a.prod_cd
                   ,a.scenario_type_cd
                   ,a.kpi_type_cd
                   ,a.subsdr_cd
                   ,a.acct_cd
                   ,b.acct_nm
           ;
 