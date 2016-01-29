

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
           )
           SELECT '1520'                                                          AS prcs_seq                                 
                 ,'ARES'                                                          AS rs_module_cd                             
                 ,'BEP_SMART'                                                     AS rs_clsf_id                               
                 ,'BEP_SMART_ML'                                                     AS rs_type_cd                               
                 ,'BEP_SMART_ML'                                                     AS rs_type_name                             
                 ,a.div_cd                                                        AS div_cd                                   
                 ,b.pln_yyyymm                                                    AS base_yyyymmdd                            
                 ,NULL                                                            AS cd_desc                                  
                 ,NULL                                                            AS sort_seq                                 
                 ,'Y'                                                             AS use_flag                                 
                 ,a.div_cd AS cmpny_cd
                 ,a.div_cd AS rs_div_cd
                 ,DECODE(a.ml_acct_cat_cd, 'NSALES', 'SALE', a.ml_acct_cat_cd) AS kpi_cd
                 ,b.pln_yyyymm AS base_yyyymm
                 ,b.week_no AS base_mmweek
                 ,a.pln_yyyymm AS pln_yyyymm
                 ,CASE WHEN a.pln_yyyymm = b.pln_yyyymm THEN 'PR0'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
                       WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
                  END AS scenario_type_cd
                 ,a.subsdr_cd
                 ,c.subsdr_shrt_name
                 ,SUM(a.krw_amt) AS currm_krw_amt
                 ,SUM(a.usd_amt) AS currm_usd_amt
                 ,TRUNC(a.creation_date)
                 ,'ARES'
                 ,TRUNC(a.last_upd_date)
                 ,'ARES'
           FROM   tb_rfe_ml_upld_rslt_s a
                 ,tb_rfe_ml_week_m      b
                 ,(
                   SELECT DISTINCT s.subsdr_shrt_name, s.subsdr_cd, s.subsdr_kor_name
                   FROM  tb_cm_subsdr_period_h s
                   WHERE s.mgt_type_cd  = 'CM'
                   AND   s.acctg_yyyymm = '*'
                   AND   s.acctg_week   = '*'
                   AND   s.temp_flag    = 'N' ) C
           WHERE  b.pln_yyyymm   BETWEEN '201301' AND '201512' -- 요부분변경
           AND    a.data_type_cd = 'SUBSDR'
           AND    a.pln_yyyyweek = b.pln_yyyyweek
           AND    b.pln_yyyymm   <= a.pln_yyyymm
           AND    a.pln_yyyymm   < TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 4), 'YYYYMM')
           AND    a.krw_amt      <> 0
           AND    c.subsdr_cd    = a.subsdr_cd(+)
           GROUP BY a.div_cd
                   ,a.ml_acct_cat_cd
                   ,b.pln_yyyymm 
                   ,b.week_no
                   ,a.pln_yyyymm 
                   ,CASE WHEN a.pln_yyyymm = b.pln_yyyymm THEN 'PR0'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 1), 'YYYYMM') THEN 'PR1'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 2), 'YYYYMM') THEN 'PR2'
                         WHEN a.pln_yyyymm = TO_CHAR(ADD_MONTHS(TO_DATE(b.pln_yyyymm, 'YYYYMM'), 3), 'YYYYMM') THEN 'PR3'
                    END 
                   ,a.subsdr_cd
                   ,c.subsdr_shrt_name
                   ,TRUNC(a.creation_date)
                   ,TRUNC(a.last_upd_date)
          ;
