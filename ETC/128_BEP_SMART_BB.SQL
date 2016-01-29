
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
           )
           -- BB RATIO
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_BB'                                                     AS rs_type_cd                               
                 ,'BEP_SMART_BB'                                                     AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,a.basis_yyyymm                                                  AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a.basis_yyyymm
                 ,a.basis_yyyyww
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            BETWEEN '201301' AND '201512'                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,tb_cm_week_m b
           WHERE  a.basis_yyyyww = REPLACE(b.base_yyyyweek,'W','')
           GROUP BY a.basis_yyyymm
                   ,a.basis_yyyyww
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- BB RATIO W5
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_BB'||'W5'                                               AS rs_type_cd                               
                 ,'BEP_SMART_BB'||'W5'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm                             
                 ,b.w1_week                            
                 ,a.subsdr_cd                          
                 ,a.division_code                      
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (                                    
                   SELECT a.basis_yyyymm               
                         ,a.basis_yyyyww               
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            BETWEEN '201301' AND '201512'                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 5주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    w2.base_yyyy < '2017') b
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           -- 63961
           -- BB RATIO W13
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_BB'||'W13'                                               AS rs_type_cd                               
                 ,'BEP_SMART_BB'||'W13'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            BETWEEN '201301' AND '201512'                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE    w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 13주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    W2.BASE_YYYY < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
           
           UNION ALL
           
           -- 94996
           -- BB RATIO W52
           SELECT '1540'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_BB'||'W52'                                               AS rs_type_cd                               
                 ,'BEP_SMART_BB'||'W52'                                               AS rs_type_name                             
                 ,a.division_code                                                 AS div_cd                                   
                 ,b.yyyymm                                                        AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,b.yyyymm
                 ,b.w1_week
                 ,a.subsdr_cd
                 ,a.division_code
                 ,SUM(DECODE(a.currency_code,'USD',a.new_amt)) + SUM(DECODE(a.currency_code,'USD',a.increase_amt)) + SUM(DECODE(a.currency_code,'USD',a.decrease_amt)) AS award_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.new_amt)) + SUM(DECODE(a.currency_code,'KRW',a.increase_amt)) + SUM(DECODE(a.currency_code,'KRW',a.decrease_amt)) AS award_krw_amt
                 ,SUM(DECODE(a.currency_code,'USD',a.change_amt)) AS sales_usd_amt
                 ,SUM(DECODE(a.currency_code,'KRW',a.change_amt)) AS sales_krw_amt
           FROM   (
                   SELECT a.basis_yyyymm
                         ,a.basis_yyyyww
                         ,a.corporation_code
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                         ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                         ,a.au_code
                         ,a.division_code
                         ,a.currency_code
                         ,SUM(a.new_amt     ) AS new_amt     
                         ,SUM(a.increase_amt) AS increase_amt
                         ,SUM(a.decrease_amt) AS decrease_amt
                         ,SUM(a.change_amt  ) AS change_amt  
                   FROM   tb_i24_b2b_pipeline_balance a
                         ,tb_cm_mgt_org_m o
                   WHERE  a.stage_code              = 'A'
                   AND    a.currency_code           IN ('KRW', 'USD')
                   AND    o.mgt_org_type_cd         (+)= 'IS'
                   AND    o.curr_flag               (+)= 'Y'
                   AND    o.mgt_org_eng_name        (+)= a.corporation_name
                   AND    a.basis_yyyymm            BETWEEN '201301' AND '201512'                   
                   GROUP BY a.basis_yyyymm
                           ,a.basis_yyyyww
                           ,a.corporation_code
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.up_mgt_org_cd, a.corporation_code))
                           ,DECODE(a.corporation_code, 'EEEB', 'EEBN', NVL(o.mgt_org_cd   , a.corporation_code))
                           ,a.au_code
                           ,a.division_code
                           ,a.currency_code
                  ) a
                 ,(SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                         ,REPLACE(w1.base_yyyyweek,'W','') w1_week
                         ,REPLACE(w2.base_yyyyweek,'W','') w2_week
                   FROM   tb_cm_week_m w1
                         ,tb_cm_week_m w2
                   WHERE  w2.start_yyyymmdd BETWEEN TO_CHAR(TO_DATE(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 52주 데이터를 읽어옴 */
                   AND    w1.base_yyyy >= '2013'
                   AND    W2.BASE_YYYY < '2017') B
           WHERE  a.basis_yyyyww = b.w2_week
           GROUP BY b.yyyymm
                   ,b.w1_week
                   ,a.subsdr_cd
                   ,a.division_code
            
                ;
