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
           )
           SELECT '1530'               AS prcs_seq                                 
                 ,'ARES'               AS rs_module_cd                             
                 ,'BEP_SMART'          AS rs_clsf_id                               
                 ,'BEP_SMART_PIPE'          AS rs_type_cd                               
                 ,'BEP_SMART_PIPE'          AS rs_type_name                             
                 ,a11.div_cd           AS div_cd                                   
                 ,a11.base_yyyymm      AS base_yyyymmdd                            
                 ,NULL                 AS cd_desc                                  
                 ,a14.sort_order       AS sort_seq                                 
                 ,'Y'                  AS use_flag                                 
                 ,a11.subsdr_cd        AS subsdr_cd
                 ,a14.subsdr_shrt_name AS new_subsdr_shrt_name
                 ,a14.sort_order       AS sort1_order
                 ,a11.mgt_org_cd       AS mgt_subsdr_cd
                 ,a13.mgt_subsdr_name  AS mgt_subsdr_name
                 ,a11.div_cd           AS div_cd
                 ,a12.div_kor_name     AS div_kor_name
                 ,a12.div_shrt_name    AS div_shrt_name
                 ,a11.base_yyyymm  base_yyyymm
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.chg_amt)) AS sales_usd_amount   --SALES AMT(매출-프로젝트)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.chg_amt)) AS sales_krw_amount
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.bal_amt)) AS backlog_usd_amount --BACKLOG AMOUNT(수주잔고)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.bal_amt)) AS backlog_krw_amount
           FROM   npt_app.nv_dww_b2b_bal_h  a11
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a12
                  ON (a11.div_cd = a12.div_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mgt_m  a13
                  ON (a11.mgt_org_cd = a13.mgt_subsdr_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a14
                  ON (a11.subsdr_cd = a14.subsdr_cd)
           WHERE (a11.base_yyyymm BETWEEN '201301' AND '201512' -- 요부분변경
           AND    a11.pjt_stg_cd IN ('A'))
           GROUP BY a11.subsdr_cd
                   ,a14.subsdr_shrt_name
                   ,a14.sort_order
                   ,a11.mgt_org_cd
                   ,a13.mgt_subsdr_name
                   ,a11.div_cd
                   ,a12.div_kor_name
                   ,a12.div_shrt_name
                   ,a11.base_yyyymm
           UNION ALL
           SELECT '1530'                                                         AS prcs_seq                                 
                 ,'ARES'                                                         AS rs_module_cd                             
                 ,'BEP_SMART'                                                    AS rs_clsf_id                               
                 ,'BEP_SMART_PIPE'                                                    AS rs_type_cd                               
                 ,'BEP_SMART_PIPE'                                                    AS rs_type_name                             
                 ,DECODE(SUBSTR(A11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS div_cd                                   
                 ,a11.base_yyyymm                                                AS base_yyyymmdd                            
                 ,NULL                                                           AS cd_desc                                  
                 ,a14.sort_order                                                 AS sort_seq                                 
                 ,'Y'                                                            AS use_flag                                 
                 ,a11.subsdr_cd                                                  AS subsdr_cd
                 ,a14.subsdr_shrt_name                                           AS new_subsdr_shrt_name
                 ,a14.sort_order                                                 AS sort1_order
                 ,a11.mgt_org_cd                                                 AS mgt_subsdr_cd
                 ,a13.mgt_subsdr_name                                            AS mgt_subsdr_name
                 ,DECODE(SUBSTR(a11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT') AS div_cd
                 ,a12.div_kor_name                                               AS div_kor_name
                 ,a12.div_shrt_name                                              AS div_shrt_name
                 ,a11.base_yyyymm                                                AS base_yyyymm
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.chg_amt))            AS sales_usd_amount        --SALES AMT(매출-프로젝트)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.chg_amt))            AS sales_krw_amount        
                 ,SUM(DECODE(a11.currency_type_cd,'USD',a11.bal_amt))            AS backlog_usd_amount      --BACKLOG AMOUNT(수주잔고)
                 ,SUM(DECODE(a11.currency_type_cd,'KRW',a11.bal_amt))            AS backlog_krw_amount
           FROM   npt_app.nv_dww_b2b_bal_h  a11
                  LEFT OUTER JOIN  npt_app.nv_dwd_div_leaf_m  a12
                  ON (a11.div_cd = a12.div_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_mgt_m  a13
                  ON (a11.mgt_org_cd = a13.mgt_subsdr_cd)
                  LEFT OUTER JOIN  npt_app.nv_dwd_subsdr_m  a14
                  ON (a11.subsdr_cd = a14.subsdr_cd)
           WHERE (a11.base_yyyymm BETWEEN '201301' AND '201512' -- 요부분변경
           AND    a11.div_cd = 'GNT'
           AND    SUBSTR(a11.prod_lvl3_cd,1,2) IN ('CS','HT')
           AND    SUBSTR(a11.prod_lvl4_cd,1,2) IN ('CS','HT')
           AND    a11.mdl_sffx_cd = '*'
           AND    a11.pjt_stg_cd IN ('A'))
           GROUP BY a11.subsdr_cd
                   ,a14.subsdr_shrt_name
                   ,a14.sort_order
                   ,a11.mgt_org_cd
                   ,a13.mgt_subsdr_name
                   ,DECODE(SUBSTR(a11.prod_lvl3_cd,1,2),'CS','GNTCS','HT','GNTHT')
                   ,a12.div_kor_name
                   ,a12.div_shrt_name
                   ,a11.base_yyyymm
          ;
