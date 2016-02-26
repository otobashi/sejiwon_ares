CREATE OR REPLACE PACKAGE NPT_APP.pg_rs_kpi_smart
/***************************************************************************************************/
/* 1.�� �� �� Ʈ : New Plantopia
/* 2.��       �� : RS (ARES)
/* 3.���α׷� ID : pg_rs_kpi_smart
/* 4.��       �� : ARES SMART ����
/*                 1. sp_rs_kpi_bb_ratio_th - BB RATIO(TV���̴���/HOTEL TV)����
/* 5.�� �� �� �� :
/* 6.Source      :
/* 7.��  ��   �� :
/* 8.�� �� �� ġ :
/* 9.�� �� �� �� :
/*
/* Version  �ۼ���  �Ҽ�   ��    ��   ��       ��                                             ��û��
/* -------- ------ ------ ---------- -------------------------------------------------------- ------
/*   1.0     shlee  RS    2016.01.28 �����ۼ�                                                  mysik
/***************************************************************************************************/


 IS

    cv_module_name CONSTANT VARCHAR2(40) := 'pg_rs_kpi_smart'; --set package name

    -- iv_category : BEP_SMART_BB �� �Է��ϸ� ��.(BEP_SMART_BB / BEP_SMART_BBW5 / BEP_SMART_BBW13 / BEP_SMART_BBW52)
    PROCEDURE sp_rs_kpi_bb_ratio_th(iv_yyyymm     IN VARCHAR2
                                   ,iv_category   IN VARCHAR2);

    PROCEDURE sp_rs_kpi_bb_ratio(iv_yyyymm     IN VARCHAR2
                                ,iv_category   IN VARCHAR2);

    -- iv_category : BEP_SMART_CNT ( SALE / COI / MGN_PROFIT /SALES_DEDUCTION ��)
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
