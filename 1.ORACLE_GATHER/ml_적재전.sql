        WITH
        with_ml_data AS (
            SELECT  d.subsdr_cd
                  , d.au_cd
                  , d.div_cd
                  
                  , SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PY',d.nsales_amt,0),0)) py_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'RF',d.nsales_amt,0),0)) rf_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PW1',d.nsales_amt,0),0)) pw1_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PW2',d.nsales_amt,0),0)) pw2_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PW3',d.nsales_amt,0),0)) pw3_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PW4',d.nsales_amt,0),0)) pw4_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'PW5',d.nsales_amt,0),0)) pw5_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201512',DECODE(d.src_type_cd,'TW',d.nsales_amt,0),0)) tw_nsales_amt1
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PY',d.nsales_amt,0),0)) py_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'RF',d.nsales_amt,0),0)) rf_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PW1',d.nsales_amt,0),0)) pw1_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PW2',d.nsales_amt,0),0)) pw2_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PW3',d.nsales_amt,0),0)) pw3_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PW4',d.nsales_amt,0),0)) pw4_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'PW5',d.nsales_amt,0),0)) pw5_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201601',DECODE(d.src_type_cd,'TW',d.nsales_amt,0),0)) tw_nsales_amt2
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PY',d.nsales_amt,0),0)) py_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'RF',d.nsales_amt,0),0)) rf_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PW1',d.nsales_amt,0),0)) pw1_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PW2',d.nsales_amt,0),0)) pw2_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PW3',d.nsales_amt,0),0)) pw3_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PW4',d.nsales_amt,0),0)) pw4_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'PW5',d.nsales_amt,0),0)) pw5_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201602',DECODE(d.src_type_cd,'TW',d.nsales_amt,0),0)) tw_nsales_amt3
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PY',d.nsales_amt,0),0)) py_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'RF',d.nsales_amt,0),0)) rf_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PW1',d.nsales_amt,0),0)) pw1_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PW2',d.nsales_amt,0),0)) pw2_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PW3',d.nsales_amt,0),0)) pw3_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PW4',d.nsales_amt,0),0)) pw4_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'PW5',d.nsales_amt,0),0)) pw5_nsales_amt4
, SUM(DECODE(pln_yyyymm,'201603',DECODE(d.src_type_cd,'TW',d.nsales_amt,0),0)) tw_nsales_amt4
            FROM    (
                    
                    SELECT  'PY' src_type_cd
                          , TO_CHAR(ADD_MONTHS(TO_DATE(bep.acctg_yyyymm,'YYYYMM'),12),'YYYYMM')  pln_yyyymm
                          , bep.subsdr_cd
                          , bep.au_cd
                          , bep.div_cd
                          , bep.usd_amt /exr.month_avg_xrate nsales_amt
                    FROM    tb_rfe_ml_monthly_div_bep_s bep   -- BEP
                          , tb_cm_monthly_xrate_m       exr   -- Exchange Rate
                    WHERE   bep.mgt_type_cd = 'APC'
                    AND     bep.acctg_yyyymm  BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE('201512','YYYYMM'),-12),'YYYYMM')  
                                              AND TO_CHAR(ADD_MONTHS(TO_DATE('201603','YYYYMM'),-12),'YYYYMM') 
                    AND     bep.ml_acct_cat_cd = 'NSALES'
                   
                    
                    
                    AND     bep.subsdr_cd   IN (null , 'ENUS'  , 'ENUS'  , 'ENUS'  , 'ENIU'  , 'ENCI'  , 'EEBN'  , 'EEDG'  , 'EEAG'  , 'EEUN'  , 'EESW'  , 'EEUK'  , 'EEIB'  , 'EEES'  , 'EEPT'  , 'EEFS'  , 'EEIH'  , 'EEIS'  , 'EEHS'  , 'EECZ'  , 'EELA'  , 'EEMK'  , 'EEPL'  , 'EERO'  , 'ENMS'  , 'ESCB'  , 'ESPS'  , 'ESCL'  , 'ESPR'  , 'ESAR'  , 'ESSP'  , 'EMGF'  , 'EMGF'  , 'EMTK'  , 'EMLF'  , 'EMLF'  , 'EMDF'  , 'EMGF'  , 'EMDF'  , 'EMEZ'  , 'EFEG'  , 'EFMC'  , 'EMAS'  , 'EFSA'  , 'EMAF'  , 'EMAF'  , 'EHAP'  , 'EAIN'  , 'EAML'  , 'EACM'  , 'EASL'  , 'EATH'  , 'EAVH'  , 'EATT'  , 'EAIL'  , 'ERRZ'  , 'ERRA'  , 'EEUR'  , 'EEAK'  , 'ECCH'  , 'ECHK'  , 'EJJP' ) 
                    
                    -- Exchange Rate
                    AND     exr.mgt_type_cd         = 'CM'
                    AND     exr.period_name         = bep.acctg_yyyymm
                    AND     exr.pln_period_yyyymm   = bep.acctg_yyyymm
                    AND     exr.subsdr_cd           = 'EKHQ'
                    AND     exr.from_currency_cd    = 'USD'
                    AND     exr.to_currency_cd      = 'USD'   -- Fixed
                    AND     exr.usr_cnvt_type_cd    = 'TTM'
                    UNION   ALL
                    
                    SELECT  'RF' src_type_cd
                          , bep.pln_yyyymm
                          , bep.subsdr_cd
                          , bep.au_cd
                          , bep.div_cd
                          , bep.usd_amt /exr.usd_cnvt_rate nsales_amt
                    FROM    tb_rfe_ml_monthly_div_bep_s bep   -- BEP
                          , tb_rf_pln_xrate_d  exr            -- Exchange Rate
                    WHERE   bep.mgt_type_cd = 'RFC'
                    AND     bep.acctg_yyyymm  = '201512'
                    AND     bep.ml_acct_cat_cd = 'NSALES'
                    -- Exchange Rate
                    AND     exr.pln_period_yyyymm       = bep.acctg_yyyymm
                    AND     exr.pln_yyyymm              = bep.pln_yyyymm
                    AND     exr.currency_cd             = 'USD'
                    
                    
                    AND     bep.subsdr_cd   IN (null , 'ENUS'  , 'ENUS'  , 'ENUS'  , 'ENIU'  , 'ENCI'  , 'EEBN'  , 'EEDG'  , 'EEAG'  , 'EEUN'  , 'EESW'  , 'EEUK'  , 'EEIB'  , 'EEES'  , 'EEPT'  , 'EEFS'  , 'EEIH'  , 'EEIS'  , 'EEHS'  , 'EECZ'  , 'EELA'  , 'EEMK'  , 'EEPL'  , 'EERO'  , 'ENMS'  , 'ESCB'  , 'ESPS'  , 'ESCL'  , 'ESPR'  , 'ESAR'  , 'ESSP'  , 'EMGF'  , 'EMGF'  , 'EMTK'  , 'EMLF'  , 'EMLF'  , 'EMDF'  , 'EMGF'  , 'EMDF'  , 'EMEZ'  , 'EFEG'  , 'EFMC'  , 'EMAS'  , 'EFSA'  , 'EMAF'  , 'EMAF'  , 'EHAP'  , 'EAIN'  , 'EAML'  , 'EACM'  , 'EASL'  , 'EATH'  , 'EAVH'  , 'EATT'  , 'EAIL'  , 'ERRZ'  , 'ERRA'  , 'EEUR'  , 'EEAK'  , 'ECCH'  , 'ECHK'  , 'EJJP' ) 
                    
                    UNION   ALL
                    SELECT  
                             DECODE(bep.pln_yyyyweek,'201553','TW','201552','PW1','201551','PW2','201550','PW3','201549','PW4','201548','PW5') src_type_cd
                            
                          , bep.pln_yyyymm
                          , bep.subsdr_cd
                          , bep.au_cd
                          , bep.div_cd
                          , bep.usd_amt / exr.usd_cnvt_rate nsales_amt
                    FROM    tb_rfe_ml_div_bep_s  bep   -- BEP
                          , tb_rf_pln_xrate_d    exr   -- Exchange Rate
                    WHERE   bep.pln_yyyyweek    BETWEEN NVL('201548','201553') AND '201553'
                    AND     bep.pln_yyyymm      BETWEEN NVL('201512','201603')  AND '201603'
                    AND     bep.ml_acct_cat_cd  = 'NSALES'
                    -- Exchange Rate
                    AND     exr.pln_period_yyyymm       = '201512'
                    AND     exr.pln_yyyymm              = bep.pln_yyyymm
                    AND     exr.currency_cd             = 'USD'
                    
                    
                    
                    AND     bep.subsdr_cd   IN (null , 'ENUS'  , 'ENUS'  , 'ENUS'  , 'ENIU'  , 'ENCI'  , 'EEBN'  , 'EEDG'  , 'EEAG'  , 'EEUN'  , 'EESW'  , 'EEUK'  , 'EEIB'  , 'EEES'  , 'EEPT'  , 'EEFS'  , 'EEIH'  , 'EEIS'  , 'EEHS'  , 'EECZ'  , 'EELA'  , 'EEMK'  , 'EEPL'  , 'EERO'  , 'ENMS'  , 'ESCB'  , 'ESPS'  , 'ESCL'  , 'ESPR'  , 'ESAR'  , 'ESSP'  , 'EMGF'  , 'EMGF'  , 'EMTK'  , 'EMLF'  , 'EMLF'  , 'EMDF'  , 'EMGF'  , 'EMDF'  , 'EMEZ'  , 'EFEG'  , 'EFMC'  , 'EMAS'  , 'EFSA'  , 'EMAF'  , 'EMAF'  , 'EHAP'  , 'EAIN'  , 'EAML'  , 'EACM'  , 'EASL'  , 'EATH'  , 'EAVH'  , 'EATT'  , 'EAIL'  , 'ERRZ'  , 'ERRA'  , 'EEUR'  , 'EEAK'  , 'ECCH'  , 'ECHK'  , 'EJJP' ) 
                    ) d
            GROUP   BY
                    d.subsdr_cd
                  , d.au_cd
                  , d.div_cd
        )
        , with_div_info AS (
            SELECT  s.subsdr_cd
                  , s.au_cd
                  , s.regn_cd
                  , s.regn_name
                  , s.regn_seq1
                  , s.regn_seq2
                  , s.zone_cd
                  , s.zone_name
                  , s.zone_seq1
                  , s.zone_seq2
                  , s.subsdr_name
                  , s.subsdr_seq1
                  , s.subsdr_seq2
                  , d.div_path
                  , d.root_div_cd
                  , d.connect_level
                  , d.div_cd
                  , d.div_name
                  , d.up_div_cd
                  , d.div_lvl_cd
                  , d.div_seq1
                  , d.div_seq2
                  , d.div_lvl_num
                  , d.display_flag   -- C20150616_02702
            FROM    (
                    SELECT  NVL(subs.attribute1_value,subs.lkup_cd) subsdr_cd
                          , NVL(subs.attribute2_value,'*')         au_cd
                          , crph.regn_cd                            regn_cd
                          , crph.regn_name                          regn_name
                          , rseq.sort_order                         regn_seq1
                          , crph.scrn_dspl_seq                      regn_seq2
                          , czph.zone_cd                            zone_cd
                          , czph.zone_shrt_name                     zone_name
                          , zseq.sort_order                         zone_seq1
                          , czph.scrn_dspl_seq                      zone_seq2
                          , NVL2(subr.lkup_cd,subs.subsdr_cd||subs.au_cd,csph.subsdr_cd) disp_subsdr_cd
                          , NVL2(subr.lkup_cd,subr.lkup_cd,csph.subsdr_cd) subsdr_rnr_cd
                          , subs.lkup_name                          subsdr_name
                          , subs.sort_order                         subsdr_seq1
                          , csph.sort_order                         subsdr_seq2
                          , csph.bk_currency_cd                     currency_cd
                    FROM    tb_cm_lkup_d           subs -- Most Likely Subsidiary
                          , tb_cm_subsdr_period_h  csph -- Subsidiary
                          , tb_cm_lkup_d           subr -- Subsidiary RnR
                          , tb_cm_regn_period_h    crph -- Region
                          , tb_cm_zone_period_h    czph -- Zone
                          , tb_cm_lkup_d           rseq -- Region Order
                          , tb_cm_lkup_d           zseq -- Zone Order
                    WHERE   
                    -- Most Likely Subsidiary
                            subs.lkup_clss_cd    = 'ML_SUBSDR_CD'
                    AND     subs.div_cd          = '*'
                    AND     subs.subsdr_cd       = '*'
                    AND     subs.au_cd           = '*'
                    AND     subs.use_flag        = 'Y'   -- C20150616_02702
                                                
                                        
                    AND     subs.lkup_cd   IN (null , 'ENUS'  , 'ENUS_BND'  , 'ENUS_OEM'  , 'ENIU'  , 'ENCI'  , 'EEBN'  , 'EEDG'  , 'EEAG'  , 'EEUN'  , 'EESW'  , 'EEUK'  , 'EEIB'  , 'EEES'  , 'EEPT'  , 'EEFS'  , 'EEIH'  , 'EEIS'  , 'EEHS'  , 'EECZ'  , 'EELA'  , 'EEMK'  , 'EEPL'  , 'EERO'  , 'ENMS'  , 'ESCB'  , 'ESPS'  , 'ESCL'  , 'ESPR'  , 'ESAR'  , 'ESSP'  , 'EMGF_IGF'  , 'EMGF_ISB'  , 'EMTK'  , 'EMLF_ILF'  , 'EMLF_IYK'  , 'EMDF_IIR'  , 'EMGF_IRO'  , 'EMDF_ITU'  , 'EMEZ'  , 'EFEG'  , 'EFMC'  , 'EMAS'  , 'EFSA'  , 'EMAF_IAF'  , 'EMAF_IEF'  , 'EHAP'  , 'EAIN'  , 'EAML'  , 'EACM'  , 'EASL'  , 'EATH'  , 'EAVH'  , 'EATT'  , 'EAIL'  , 'ERRZ'  , 'ERRA'  , 'EEUR'  , 'EEAK'  , 'ECCH'  , 'ECHK'  , 'EJJP' ) 
                                                 
                    -- Subsidiary
                    AND     csph.mgt_type_cd    = 'CM'
                    AND     csph.acctg_yyyymm   = '*'
                    AND     csph.acctg_week     = '*'
                    AND     csph.temp_flag      = 'N'
                    AND     csph.subsdr_cd      = NVL(subs.attribute1_value,subs.lkup_cd)
                    -- Region
                    AND     crph.mgt_type_cd    = 'CM'
                    AND     crph.acctg_yyyymm   = '*'
                    AND     crph.acctg_week     = '*'
                    AND     crph.temp_flag      = 'N'
                    AND     crph.regn_cd        = csph.regn_cd
                    -- Zone
                    AND     czph.mgt_type_cd    = 'CM'
                    AND     czph.acctg_yyyymm   = '*'
                    AND     czph.acctg_week     = '*'
                    AND     czph.temp_flag      = 'N'
                    AND     czph.zone_cd        = csph.zone_cd
                    -- Subsidiary RnR
                    AND     subr.lkup_clss_cd(+)= 'ML_INTRNL_SUBSDR_RNR_CD'
                    AND     subr.div_cd      (+)= '*'
                    AND     subr.subsdr_cd   (+)= '*'
                    AND     subr.au_cd       (+)= '*'
                    AND     subr.lkup_cd     (+)= NVL(subs.attribute1_value,subs.lkup_cd)
                    AND     subr.use_flag    (+)= 'Y'   -- C20150616_02702
                    -- Region Order
                    AND     rseq.lkup_clss_cd(+)= 'ML_REGN_CD'
                    AND     rseq.div_cd      (+)= '*'
                    AND     rseq.subsdr_cd   (+)= '*'
                    AND     rseq.au_cd       (+)= '*'
                    AND     rseq.lkup_cd     (+)= crph.regn_cd
                    AND     rseq.use_flag    (+)= 'Y'  -- C20150616_02702
                    -- Zone Order
                    AND     zseq.lkup_clss_cd(+)= 'ML_ZONE_CD'
                    AND     zseq.div_cd      (+)= '*'
                    AND     zseq.subsdr_cd   (+)= '*'
                    AND     zseq.au_cd       (+)= '*'
                    AND     zseq.lkup_cd     (+)= czph.zone_cd
                    AND     zseq.use_flag    (+)= 'Y'    -- C20150616_02702
                    ) s
                  , (
                    SELECT  SYS_CONNECT_BY_PATH(cdph.lkup_cd,'|') div_path
                          , connect_by_root(cdph.lkup_cd) root_div_cd
                          , LEVEL  connect_level
                          , cdph.lkup_cd div_cd
                          , SUBSTR(LPAD(' ',TO_NUMBER(NVL(cdph.attribute4_value,'0'))*4,' '),5,TO_NUMBER(NVL(cdph.attribute4_value,'0'))*4)||cdph.lkup_name   div_name
                          , cdph.attribute2_value up_div_cd
                          , cdph.attribute3_value div_lvl_cd
                          , cdph.sort_order       div_seq1
                          , cdph.sort_order       div_seq2
                          , cdph.attribute4_value div_lvl_num
                          , cdph.attribute7_value display_flag   -- C20150616_02702
                    FROM    tb_cm_lkup_d  cdph
                    WHERE   cdph.lkup_clss_cd       = 'ML_SUBSDR_TYPE_DIV_CD'
                    AND     cdph.div_cd             = '*'
                    AND     cdph.subsdr_cd          = '*'
                    AND     cdph.au_cd              = '*'
                    AND     cdph.use_flag           = 'Y'    -- C20150616_02702
                    START   WITH 
                            cdph.attribute8_value   = 'Y'
                    CONNECT BY NOCYCLE 
                                    cdph.lkup_clss_cd   = PRIOR cdph.lkup_clss_cd
                            AND     cdph.div_cd         = PRIOR cdph.div_cd
                            AND     cdph.subsdr_cd      = PRIOR cdph.subsdr_cd
                            AND     cdph.au_cd          = PRIOR cdph.au_cd
                            AND     cdph.lkup_cd        = PRIOR cdph.attribute2_value
                    ) d
        )
        SELECT  category_name
              , regn_seq1
              , regn_seq2
              , regn_name
              , zone_seq1
              , zone_seq2
              , zone_name
              , subsdr_seq1
              , subsdr_seq2
              , subsdr_name
              , div_seq1
              , div_seq2
              , div_name
              
              , ROUND((py_nsales_amt1)/1, 2) py_nsales_amt1
, ROUND((rf_nsales_amt1)/1, 2) rf_nsales_amt1
, ROUND((pw1_nsales_amt1)/1, 2) pw1_nsales_amt1
, ROUND((pw2_nsales_amt1)/1, 2) pw2_nsales_amt1
, ROUND((pw3_nsales_amt1)/1, 2) pw3_nsales_amt1
, ROUND((pw4_nsales_amt1)/1, 2) pw4_nsales_amt1
, ROUND((pw5_nsales_amt1)/1, 2) pw5_nsales_amt1
, ROUND((tw_nsales_amt1)/1, 2) tw_nsales_amt1
, ROUND(DECODE(py_nsales_amt1,0,0,tw_nsales_amt1/py_nsales_amt1-1)*100,2) vs_py_nsales_amt1
, ROUND(DECODE(rf_nsales_amt1,0,0,tw_nsales_amt1/rf_nsales_amt1)*100,2) vs_rf_nsales_amt1
, ROUND(DECODE(pw1_nsales_amt1,0,0,tw_nsales_amt1/pw1_nsales_amt1-1)*100,2) vs_pw1_nsales_rate1
, ROUND((tw_nsales_amt1)/1 - (pw1_nsales_amt1)/1, 2) vs_pw1_nsales_amt1
, ROUND((py_nsales_amt2)/1, 2) py_nsales_amt2
, ROUND((rf_nsales_amt2)/1, 2) rf_nsales_amt2
, ROUND((pw1_nsales_amt2)/1, 2) pw1_nsales_amt2
, ROUND((pw2_nsales_amt2)/1, 2) pw2_nsales_amt2
, ROUND((pw3_nsales_amt2)/1, 2) pw3_nsales_amt2
, ROUND((pw4_nsales_amt2)/1, 2) pw4_nsales_amt2
, ROUND((pw5_nsales_amt2)/1, 2) pw5_nsales_amt2
, ROUND((tw_nsales_amt2)/1, 2) tw_nsales_amt2
, ROUND(DECODE(py_nsales_amt2,0,0,tw_nsales_amt2/py_nsales_amt2-1)*100,2) vs_py_nsales_amt2
, ROUND(DECODE(rf_nsales_amt2,0,0,tw_nsales_amt2/rf_nsales_amt2)*100,2) vs_rf_nsales_amt2
, ROUND(DECODE(pw1_nsales_amt2,0,0,tw_nsales_amt2/pw1_nsales_amt2-1)*100,2) vs_pw1_nsales_rate2
, ROUND((tw_nsales_amt2)/1 - (pw1_nsales_amt2)/1, 2) vs_pw1_nsales_amt2
, ROUND((py_nsales_amt3)/1, 2) py_nsales_amt3
, ROUND((rf_nsales_amt3)/1, 2) rf_nsales_amt3
, ROUND((pw1_nsales_amt3)/1, 2) pw1_nsales_amt3
, ROUND((pw2_nsales_amt3)/1, 2) pw2_nsales_amt3
, ROUND((pw3_nsales_amt3)/1, 2) pw3_nsales_amt3
, ROUND((pw4_nsales_amt3)/1, 2) pw4_nsales_amt3
, ROUND((pw5_nsales_amt3)/1, 2) pw5_nsales_amt3
, ROUND((tw_nsales_amt3)/1, 2) tw_nsales_amt3
, ROUND(DECODE(py_nsales_amt3,0,0,tw_nsales_amt3/py_nsales_amt3-1)*100,2) vs_py_nsales_amt3
, ROUND(DECODE(rf_nsales_amt3,0,0,tw_nsales_amt3/rf_nsales_amt3)*100,2) vs_rf_nsales_amt3
, ROUND(DECODE(pw1_nsales_amt3,0,0,tw_nsales_amt3/pw1_nsales_amt3-1)*100,2) vs_pw1_nsales_rate3
, ROUND((tw_nsales_amt3)/1 - (pw1_nsales_amt3)/1, 2) vs_pw1_nsales_amt3
, ROUND((py_nsales_amt4)/1, 2) py_nsales_amt4
, ROUND((rf_nsales_amt4)/1, 2) rf_nsales_amt4
, ROUND((pw1_nsales_amt4)/1, 2) pw1_nsales_amt4
, ROUND((pw2_nsales_amt4)/1, 2) pw2_nsales_amt4
, ROUND((pw3_nsales_amt4)/1, 2) pw3_nsales_amt4
, ROUND((pw4_nsales_amt4)/1, 2) pw4_nsales_amt4
, ROUND((pw5_nsales_amt4)/1, 2) pw5_nsales_amt4
, ROUND((tw_nsales_amt4)/1, 2) tw_nsales_amt4
, ROUND(DECODE(py_nsales_amt4,0,0,tw_nsales_amt4/py_nsales_amt4-1)*100,2) vs_py_nsales_amt4
, ROUND(DECODE(rf_nsales_amt4,0,0,tw_nsales_amt4/rf_nsales_amt4)*100,2) vs_rf_nsales_amt4
, ROUND(DECODE(pw1_nsales_amt4,0,0,tw_nsales_amt4/pw1_nsales_amt4-1)*100,2) vs_pw1_nsales_rate4
, ROUND((tw_nsales_amt4)/1 - (pw1_nsales_amt4)/1, 2) vs_pw1_nsales_amt4
        FROM    (
                SELECT  'Net' || CHR(10) || 'Sales'            category_name
                      , regn_seq1
                      , regn_seq2
                      , regn_name
                      , zone_seq1
                      , zone_seq2
                      , zone_name
                      , subsdr_seq1
                      , subsdr_seq2
                      , subsdr_name
                      , di.div_cd
                      , div_seq1
                      , div_seq2
                      , div_name
                      , di.display_flag  -- C20150616_02702
              
                      , NVL(SUM(md.py_nsales_amt1),0) py_nsales_amt1
, NVL(SUM(md.rf_nsales_amt1),0) rf_nsales_amt1
, NVL(SUM(md.pw1_nsales_amt1),0) pw1_nsales_amt1
, NVL(SUM(md.pw2_nsales_amt1),0) pw2_nsales_amt1
, NVL(SUM(md.pw3_nsales_amt1),0) pw3_nsales_amt1
, NVL(SUM(md.pw4_nsales_amt1),0) pw4_nsales_amt1
, NVL(SUM(md.pw5_nsales_amt1),0) pw5_nsales_amt1
, NVL(SUM(md.tw_nsales_amt1),0) tw_nsales_amt1
, NVL(SUM(md.py_nsales_amt2),0) py_nsales_amt2
, NVL(SUM(md.rf_nsales_amt2),0) rf_nsales_amt2
, NVL(SUM(md.pw1_nsales_amt2),0) pw1_nsales_amt2
, NVL(SUM(md.pw2_nsales_amt2),0) pw2_nsales_amt2
, NVL(SUM(md.pw3_nsales_amt2),0) pw3_nsales_amt2
, NVL(SUM(md.pw4_nsales_amt2),0) pw4_nsales_amt2
, NVL(SUM(md.pw5_nsales_amt2),0) pw5_nsales_amt2
, NVL(SUM(md.tw_nsales_amt2),0) tw_nsales_amt2
, NVL(SUM(md.py_nsales_amt3),0) py_nsales_amt3
, NVL(SUM(md.rf_nsales_amt3),0) rf_nsales_amt3
, NVL(SUM(md.pw1_nsales_amt3),0) pw1_nsales_amt3
, NVL(SUM(md.pw2_nsales_amt3),0) pw2_nsales_amt3
, NVL(SUM(md.pw3_nsales_amt3),0) pw3_nsales_amt3
, NVL(SUM(md.pw4_nsales_amt3),0) pw4_nsales_amt3
, NVL(SUM(md.pw5_nsales_amt3),0) pw5_nsales_amt3
, NVL(SUM(md.tw_nsales_amt3),0) tw_nsales_amt3
, NVL(SUM(md.py_nsales_amt4),0) py_nsales_amt4
, NVL(SUM(md.rf_nsales_amt4),0) rf_nsales_amt4
, NVL(SUM(md.pw1_nsales_amt4),0) pw1_nsales_amt4
, NVL(SUM(md.pw2_nsales_amt4),0) pw2_nsales_amt4
, NVL(SUM(md.pw3_nsales_amt4),0) pw3_nsales_amt4
, NVL(SUM(md.pw4_nsales_amt4),0) pw4_nsales_amt4
, NVL(SUM(md.pw5_nsales_amt4),0) pw5_nsales_amt4
, NVL(SUM(md.tw_nsales_amt4),0) tw_nsales_amt4
                      
                FROM    with_ml_data  md
                      , with_div_info di
                WHERE   md.div_cd    (+)= di.root_div_cd
                AND     md.subsdr_cd (+)= di.subsdr_cd
                AND     md.au_cd     (+)= di.au_cd
                GROUP   BY
                        regn_seq1
                      , regn_seq2
                      , regn_name
                      , zone_seq1
                      , zone_seq2
                      , zone_name
                      , subsdr_seq1
                      , subsdr_seq2
                      , subsdr_name
                      , di.div_cd
                      , div_seq1
                      , div_seq2
                      , div_name
                      , di.display_flag -- C20150616_02702
                )
        WHERE   1 = 1
        AND     display_flag = 'Y'   -- C20150616_02702
        
        -- Division
        
         -- CSRXXX
        
        ORDER   BY
                NVL2(regn_seq1,1,9)
              , regn_seq1
              , NVL2(regn_seq2,1,9)
              , regn_seq2
              , regn_name
              , NVL2(zone_seq1,1,9)
              , zone_seq1
              , NVL2(zone_seq2,1,9)
              , zone_seq2
              , zone_name
              , NVL2(subsdr_seq1,1,9)
              , subsdr_seq1
              , NVL2(subsdr_seq2,1,9)
              , subsdr_seq2
              , subsdr_name
              , NVL2(div_seq1,1,9)
              , div_seq1
              , div_name