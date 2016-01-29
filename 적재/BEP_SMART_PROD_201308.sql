-- 8883 rows 69.748 sec
-- =       '201308'
-- BEP_SMART_PROD
          
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
                 ,attribute12_value
                 ,attribute13_value
                 ,attribute14_value
                 ,attribute15_value
                 ,attribute16_value
                 ,attribute17_value
                 ,attribute18_value
                 ,attribute19_value
                 ,attribute20_value
                 ,attribute21_value
                 ,attribute22_value
                 ,attribute23_value
                 ,attribute24_value
                 ,attribute25_value
                 ,attribute26_value
                 ,attribute27_value
                 ,attribute28_value
                 ,attribute29_value
                 ,attribute30_value
                 ,attribute31_value
                 ,attribute32_value
                 ,attribute33_value
                 ,attribute34_value
                 ,attribute35_value
                 ,attribute36_value
                 ,attribute37_value
                 ,attribute38_value
                 ,attribute39_value
                 ,attribute40_value
           )
           /*******************************************************/
           /* 주요제품군 Master                                   */
           /*******************************************************/
           WITH v_prod_mst (
              mgt_type_cd
             ,prod_gr_cd
             ,subsdr_cd
             ,prod_cd
             ,prod_lvl_cd
             ,prod_eng_name
             ,prod_kor_name
             ,up_prod_cd
             ,last_lvl_flag
             ,div_cd
             ,enable_flag
             ,sales_qty_incl_flag
             ,sum_excl_flag
             ,sort_order
             ,prod_cd_desc
           )
           AS
           (
             SELECT mgt_type_cd
                   ,prod_gr_cd
                   ,subsdr_cd
                   ,prod_cd
                   ,prod_lvl_cd
                   ,prod_eng_name
                   ,prod_kor_name
                   ,up_prod_cd
                   ,last_lvl_flag
                   ,div_cd
                   ,enable_flag
                   ,sales_qty_incl_flag
                   ,sum_excl_flag
                   ,sort_order
                   ,prod_cd_desc
             FROM   tb_cm_prod_period_h t
             WHERE  t.mgt_type_cd  = 'CM'
             AND    t.acctg_yyyymm = '*'
             AND    t.acctg_week   = '*'
             AND    t.temp_flag    = 'N'
             AND    prod_cd        NOT IN ( '847141' ,'ACAHHAW')-- IMSI
             UNION ALL
             SELECT 'CM'                                                 AS mgt_type_cd
                   ,'PRODUCT_LEVEL'                                      AS prod_gr_cd
                   ,'*'                                                  AS subsdr_cd
                   ,'*'                                                  AS prod_cd
                   ,b.prod_lvl_cd                                        AS prod_lvl_cd
                   ,'*'          ||'(LVL'||b.prod_lvl_cd||')'            AS prod_eng_name
                   ,'*'          ||'(LVL'||b.prod_lvl_cd||')'            AS prod_kor_name
                   ,'*'                                                  AS up_prod_cd
                   ,CASE WHEN b.prod_lvl_cd  = '4' THEN 'Y' ELSE 'N' END AS last_lvl_flag
                   ,'*'                                                  AS div_cd
                   ,'Y'                                                  AS enable_flag
                   ,'N'                                                  AS sales_qty_incl_flag
                   ,NULL                                                 AS sum_excl_flag
                   ,NULL                                                 AS sort_order
                   ,NULL                                                 AS prod_cd_desc
             FROM DUAL
                ,(SELECT '1' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '2' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '3' AS prod_lvl_cd FROM DUAL UNION ALL
                  SELECT '4' AS prod_lvl_cd FROM DUAL  ) b
           ),
           /*******************************************************/
           /* 주요제품군 Levle별 (1,2,3,4)                        */
           /*******************************************************/
           v_prod_m (
               mgt_type_cd
              ,prod_gr_cd
              ,subsdr_cd
              ,prod_cd
              ,prod_lvl_cd
              ,prod_eng_name
              ,prod_kor_name
              ,up_prod_cd
              ,last_lvl_flag
              ,div_cd
              ,enable_flag
              ,sales_qty_incl_flag
              ,sum_excl_flag
              ,sort_order
           )
           AS
           (
              SELECT mgt_type_cd
                    ,prod_gr_cd
                    ,subsdr_cd
                    ,prod_cd
                    ,prod_lvl_cd
                    ,prod_eng_name
                    ,prod_kor_name
                    ,up_prod_cd
                    ,last_lvl_flag
                    ,div_cd
                    ,enable_flag
                    ,sales_qty_incl_flag
                    ,sum_excl_flag
                    ,sort_order
              FROM   v_prod_mst
              WHERE  prod_gr_cd = 'RPT_PROD_GR'
           )
           /*******************************************************/
           /* 주요제품군 조회                    */
           /*******************************************************/
           SELECT /*+ PARALLEL(8) */
                  '1510'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_PROD'                                                     AS rs_type_cd                               
                 ,'BEP_SMART_PROD'                                                     AS rs_type_name                             
                 ,a11.div_cd                                                      AS div_cd                                   
                 ,a11.acctg_yyyymm                                                AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a11.acctg_yyyymm                                                AS base_yyyymm                              
                 ,a11.div_cd                                                      AS div_cd                                   
                 ,a17.scrn_dspl_seq                                               AS div_kor_name                             
                 ,a17.div_shrt_name                                               AS div_shrt_name                            
                 ,a11.subsdr_rnr_cd                                               AS subsdr_cd                                
                 ,a112.mgt_org_shrt_name                                          AS mgt_org_shrt_name                        
                 ,a112.sort_order                                                 AS sort_order                               
                 ,a11.subsdr_cd                                                   AS subsdr_rnr_cd                            
                 ,a111.subsdr_shrt_name                                           AS subsdr_name                              
                 ,a111.sort_order                                                 AS sort1_order                              
                 ,a11.production_subsdr_cd                                        AS subsdr_cd1                               
                 ,a19.subsdr_shrt_name                                            AS subsdr_name0                             
                 ,a19.sort_order                                                  AS sort_order0                              
                 ,a11.scenario_type_cd                                            AS scenario_type_cd                         
                 ,a110.scenario_type_name                                         AS scenario_type_name                       
                 ,a110.sort_order                                                 AS sort_order1                              
                 ,a16.up_prod_cd                                                  AS prod_cd                                  
                 ,a113.prod_kor_name                                              AS prod_kor_name                            
                 ,a113.prod_eng_name                                              AS prod_eng_name                            
                 ,a113.sort_order                                                 AS sort_order2                              
                 ,a15.up_prod_cd                                                  AS prod_cd0                                 
                 ,a16.prod_kor_name                                               AS prod_kor_name0                           
                 ,a16.prod_eng_name                                               AS prod_eng_name0                           
                 ,a16.sort_order                                                  AS sort_order3                              
                 ,a14.up_prod_cd                                                  AS prod_cd1                                 
                 ,a15.prod_kor_name                                               AS prod_kor_name1                           
                 ,a15.prod_eng_name                                               AS prod_eng_name1                           
                 ,a15.sort_order                                                  AS sort_order4                              
                 ,a12.usr_prod1_last_cd                                           AS prod_cd2                                 
                 ,a14.prod_kor_name                                               AS prod_kor_name2                           
                 ,a14.prod_eng_name                                               AS prod_eng_name2                           
                 ,a14.sort_order                                                  AS sort_order5                              
                 ,SUM(a11.sales_qty)                                              AS sales_qty                                
                 ,SUM(a11.nsales_amt)                                             AS nsales_amt                               
                 ,(SUM(a11.fix_cogs_amt) + SUM(a11.var_cogs_amt))                 AS cogs_amt                                 
                 ,SUM(a11.gross_sales_amt)                                        AS gross_sales_amt                          
                 ,SUM(a11.mgnl_prf_amt)                                           AS mgnl_prf_amt                             
                 ,SUM(a11.oi_amt)                                                 AS oi_amt                                   
                 ,SUM(a11.sales_deduct_amt)                                       AS sales_deduct_amt                         
                 ,(SUM(a11.fix_sell_adm_exp_amt) + SUM(a11.var_sell_adm_exp_amt)) AS sell_adm_exp_amt                           
           FROM   npt_app.nv_dww_consld_bep_summ_s  a11                                                                       
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mdl_period_h  a12                                                    
                  ON   a11.mdl_sffx_cd = a12.mdl_sffx_cd                                                                      
                  AND  a11.subsdr_cd   = a12.subsdr_cd                                                                        
                  LEFT OUTER JOIN  npt_dw_mgr.tb_dwd_subsdr_mdl_period_h  a13                                                 
                  ON   a11.acctg_yyyymm = a13.acctg_yyyymm                                                                    
                  AND  a11.mdl_sffx_cd  = a13.mdl_sffx_cd                                                                     
                  AND  a11.subsdr_cd    = a13.subsdr_cd                                                                       
                  LEFT OUTER JOIN  v_prod_m  a14                                                                              
                  ON   a12.usr_prod1_last_cd = a14.prod_cd                                                                    
                  AND  a14.prod_lvl_cd       = '4'                                                                            
                  LEFT OUTER JOIN  v_prod_m  a15                                                                              
                  ON   a14.up_prod_cd   = a15.prod_cd                                                                         
                  AND  a15.prod_lvl_cd  = '3'                                                                                 
                  LEFT OUTER JOIN  v_prod_m  a16                                                                              
                  ON   a15.up_prod_cd  = a16.prod_cd                                                                          
                  AND  a16.prod_lvl_cd = '2'                                                                                  
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a17                                                             
                  ON   a11.div_cd = a17.div_cd                                                                                
                  LEFT OUTER JOIN  npt_app.nv_dwd_02_grd_cd  a18                                                              
                  ON   a13.grd_cd = a18.attribute_cd                                                                          
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a19                                                               
                  ON   a11.production_subsdr_cd = a19.subsdr_cd                                                               
                  LEFT OUTER JOIN  npt_app.nv_dwd_scenario_type_m  a110                                                       
                  ON   a11.scenario_type_cd = a110.scenario_type_cd                                                           
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a111                                                              
                  ON   a11.subsdr_cd = a111.subsdr_cd                                                                         
                  LEFT OUTER JOIN  npt_app.nv_dwd_mgt_org_rnr_m  a112                                                         
                  ON   a11.subsdr_rnr_cd = a112.mgt_org_cd                                                                    
                  LEFT OUTER JOIN  v_prod_m  a113                                                                             
                  ON   a16.up_prod_cd   = a113.prod_cd                                                                        
                  AND  a113.prod_lvl_cd = '1'                                                                                 
           WHERE  a11.acctg_yyyymm =       '201308' -- &IV_YYYYMM    요부분변경                                                               
           AND    a11.scenario_type_cd      = 'AC0'                                                                           
           AND    a11.consld_sales_mdl_flag = 'Y'                                                                             
           AND    a11.currm_accum_type_cd   = 'CURRM'                                                                         
           AND    a11.vrnc_alc_incl_excl_cd = 'INCL'                                                                          
           AND    a11.currency_cd           = 'USD'                                                                           
           AND    a11.mdl_sffx_cd NOT LIKE 'VM-%.CPS'                                                                         
           GROUP BY  a11.acctg_yyyymm                                                                                         
                    ,a11.div_cd                                                                                               
                    ,a17.scrn_dspl_seq                                                                                        
                    ,a17.div_shrt_name                                                                                        
                    ,a11.subsdr_rnr_cd                                                                                        
                    ,a112.mgt_org_shrt_name                                                                                   
                    ,a112.sort_order                                                                                          
                    ,a11.subsdr_cd                                                                                            
                    ,a111.subsdr_shrt_name                                                                                    
                    ,a111.sort_order                                                                                          
                    ,a11.production_subsdr_cd                                                                                 
                    ,a19.subsdr_shrt_name                                                                                     
                    ,a19.sort_order                                                                                           
                    ,a11.scenario_type_cd                                                                                     
                    ,a110.scenario_type_name                                                                                  
                    ,a110.sort_order                                                                                          
                    ,a16.up_prod_cd                                                                                           
                    ,a113.prod_kor_name                                                                                       
                    ,a113.prod_eng_name                                                                                       
                    ,a113.sort_order                                                                                          
                    ,a15.up_prod_cd                                                                                           
                    ,a16.prod_kor_name                                                                                        
                    ,a16.prod_eng_name                                                                                        
                    ,a16.sort_order                                                                                           
                    ,a14.up_prod_cd                                                                                           
                    ,a15.prod_kor_name                                                                                        
                    ,a15.prod_eng_name                                                                                        
                    ,a15.sort_order                                                                                           
                    ,a12.usr_prod1_last_cd                                                                                    
                    ,a14.prod_kor_name                                                                                        
                    ,a14.prod_eng_name                                                                                        
                    ,a14.sort_order                                                                                           
                                                                                          
           ;                                                                                                                  
                                                                                                                              
            
