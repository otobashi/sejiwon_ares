CREATE OR REPLACE PACKAGE NPT_APP.pg_rs_kpi_smart
/***************************************************************************************************/
/* 1.프 로 젝 트 : New Plantopia
/* 2.모       듈 : RS (ARES)
/* 3.프로그램 ID : pg_rs_kpi_smart
/* 4.기       능 : ARES SMART 적재
/*                 1. sp_rs_kpi_bb_ratio_th - BB RATIO(TV사이니지/HOTEL TV)적재
/* 5.입 력 변 수 :
/* 6.Source      :
/* 7.사  용   예 :
/* 8.파 일 위 치 :
/* 9.변 경 이 력 :
/*
/* Version  작성자  소속   일    자   내       용                                             요청자
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 최초작성                                                  mysik
/***************************************************************************************************/


 IS

    cv_module_name CONSTANT VARCHAR2(40) := 'pg_rs_kpi_smart'; --set package name

    -- iv_category : BEP_SMART_BB 만 입력하면 됨.(BEP_SMART_BB / BEP_SMART_BBW5 / BEP_SMART_BBW13 / BEP_SMART_BBW52)
    PROCEDURE sp_rs_kpi_bb_ratio_th(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2);

    PROCEDURE sp_rs_kpi_bb_ratio(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_CNT ( SALE / COI / MGN_PROFIT /SALES_DEDUCTION 등)
    PROCEDURE sp_rs_kpi_cnt_sale(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_ML
    PROCEDURE sp_rs_kpi_most_likely(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_PIPE
    PROCEDURE sp_rs_kpi_pipeline(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_HR
    PROCEDURE sp_rs_kpi_month_hr(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_PROD
    PROCEDURE sp_rs_kpi_prod(iv_yyyymm     IN VARCHAR2
                            ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_SUBSDR
    PROCEDURE sp_rs_kpi_mgn_profit(iv_yyyymm     IN VARCHAR2
                                  ,iv_category   IN VARCHAR2);
                
    -- iv_category : BEP_SMART_PROD_MMGN
    PROCEDURE sp_rs_kpi_prod_mmgn(iv_yyyymm     IN VARCHAR2
                                 ,iv_category   IN VARCHAR2)
                                  
END pg_rs_kpi_smart;
