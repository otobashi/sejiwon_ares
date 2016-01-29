CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_KPI_BEP_PROD (
    IN P_YYYYMM	VARCHAR(6),
    IN P_CATEGORY	VARCHAR(30),
    IN P_DIV_YYYYMM	VARCHAR(6) )
  SPECIFIC "SQL140203152810500"
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  /********************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                     */
  /* 2.모       듈 :                                                                          */
  /* 3.프로그램 ID : SP_CD_RES_KPI_BEP_PROD                                                   */
  /*                                                                                          */
  /* 4.설       명 : IPTDW_IFF_CONS_BEP_MODEL_BUYER(모델별 BEP실적)                           */
  /*                 IPTDW_IFF_CONS_BEP_MODEL_BUYER_ROFO(모델별 BEP이동)                      */
  /*                 Product별로 BEP 매출액, 한계이익, 영업이익을 집계하여                    */
  /*                 IPTDW_RES_KPI_PRODUCT에 데이터 생성함                                    */                   
  /*                                                                                          */
  /* 5.입 력 변 수 :                                                                          */
  /*                                                                                          */
  /*                 IN p_yyyymm( 기준월 )                                                    */
  /*                 IN p_category( 시산구분 )                                                */
  /*                 IN p_div_yyyymm( Division기준월 )                                        */
  /* 6.파 일 위 치 :                                                                          */
  /* 7.변 경 이 력 :                                                                          */
  /*                                                                                          */
  /*  version  작성자  일      자  내                 용                                      */
  /*  -------  ------  ----------  --------------------------------------------------------   */
  /*  1.0                                                                                     */
  /*  2.0      syyim   2014.08.06  ARES고도화 - 제품류 매핑 기준을 Weekly dashboard에 맞추어  */
  /*                                            제품류 마스터 테이블을 변경함                 */
  /*  2.1      syyim   2014.10.31  MC본부의 휴대폰, 스마트폰 매출수량 및 ASP를 산출하기 위해  */
  /*                               KPI를 추가함(SALE_QTY, GROSS_SALE)                         */
  /*  2.2      mysik   2015.02.07  상위 조직 Level5 추가                   */
  /********************************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_BEP_PROD';
    DECLARE v_load_start_timestamp       TIMESTAMP     DEFAULT NULL;
    DECLARE v_serial_no                  VARCHAR(30)   DEFAULT NULL;
    DECLARE v_load_progress_status_code  VARCHAR(10)   DEFAULT NULL;
    DECLARE v_target_insert_count        INTEGER       DEFAULT 0;
    DECLARE v_target_update_count        INTEGER       DEFAULT 0;
    DECLARE v_target_delete_count        INTEGER       DEFAULT 0;  
    DECLARE v_source_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_target_table_name          VARCHAR(300)  DEFAULT NULL; 
    DECLARE v_job_notes                  VARCHAR(300)  DEFAULT NULL;    
    DECLARE SQLSTATE                     CHAR(5)       DEFAULT '';
    DECLARE v_basis_yyyymmdd             VARCHAR(8)    DEFAULT NULL;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS EXCEPTION 1 v_job_notes = MESSAGE_TEXT;
        SET v_load_progress_status_code = SQLSTATE;

        ROLLBACK;

        CALL sp_cd_etl_job_logs( v_etl_job_no,
                                 v_basis_yyyymmdd,
                                 v_load_start_timestamp,
                                 v_serial_no,
                                 v_load_progress_status_code,
                                 v_target_insert_count,
                                 v_target_update_count,
                                 v_target_delete_count,
                                 v_source_table_name,
                                 v_target_table_name,
                                 v_job_notes
                               );
        COMMIT;

        SIGNAL SQLSTATE '70001' SET MESSAGE_TEXT = v_job_notes;
    END;

    /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_IFF_CONS_BEP_MODEL_BUYER';
    SET v_target_table_name          = 'IPTDW_RES_KPI_PRODUCT';
    SET v_basis_yyyymmdd             = p_yyyymm;

    /*-----------------------------------
       기준월의 기존 데이터 삭제
    -----------------------------------*/
    
    DELETE 
    FROM   IPTDW.IPTDW_RES_KPI_PRODUCT
    WHERE  BASIS_YYYYMM  = p_yyyymm
    AND    CATEGORY_CODE = p_category
    AND    MANUAL_ADJUST_FLAG = 'N';
  
    GET DIAGNOSTICS  v_target_delete_count = ROW_COUNT;
    
    INSERT 
    INTO  IPTDW.IPTDW_RES_KPI_PRODUCT
   ( BASIS_YYYYMM
    ,SCENARIO_CODE
    ,DIVISION_CODE
    ,MANUAL_ADJUST_FLAG
    ,KPI_TYPE_CODE
    ,CATEGORY_CODE
    ,CATEGORY_DETAIL_CODE
    ,YYYYMM
    ,CURR_MON_KRW_AMOUNT
    ,CURR_MON_USD_AMOUNT
    ,ACCU_KRW_AMOUNT
    ,ACCU_USD_AMOUNT
    ,CREATION_DATE
    ,CREATE_USER_ID
    ,LAST_UPDATE_DATE
    ,LAST_UPDATE_USER_ID )
  WITH temp1 (BASIS_YYYYMM, SCENARIO_TYPE_CODE, GBU_CODE, KPI_TYPE_CODE, CATEGORY_DETAIL_CODE, CURR_MON_KRW_AMOUNT, CURR_MON_USD_AMOUNT, ACCU_KRW_AMOUNT, ACCU_USD_AMOUNT) AS 
   ( 
    /*------------------------------------------------------
          사업부, PRODUCT(Level_Key_Code)별 데이터 생성
    -------------------------------------------------------*/       
     SELECT M.BASIS_YYYYMM,
            M.SCENARIO_TYPE_CODE,
            M.GBU_CODE,
            C.CODE_ID as KPI_TYPE_CODE,
            M.CATEGORY_DETAIL_CODE,            
            sum(CASE C.CODE_ID
                 WHEN 'SALE' THEN
                      v_net_sales_krw_amt
                 WHEN 'MGN_PROFIT' THEN
                      v_mgn_profit_krw_amt
                 WHEN 'COI' THEN
                      v_op_inc_krw_amt
                 WHEN 'SALE_QTY' THEN
                      v_sales_qty_krw
                 WHEN 'GROSS_SALE' THEN
                      v_gross_sales_krw_amt                  
                 ELSE 0 END) as CURR_MON_KRW_AMOUNT,
            sum(CASE C.CODE_ID
                 WHEN 'SALE' THEN
                      v_net_sales_usd_amt
                 WHEN 'MGN_PROFIT' THEN
                      v_mgn_profit_usd_amt
                 WHEN 'COI' THEN
                      v_op_inc_usd_amt
                 WHEN 'SALE_QTY' THEN
                      v_sales_qty_usd
                 WHEN 'GROSS_SALE' THEN
                      v_gross_sales_usd_amt
                 ELSE 0 END) as CURR_MON_USD_AMOUNT,
            sum(CASE C.CODE_ID
                 WHEN 'SALE' THEN
                      v_net_sales_accu_krw_amt
                 WHEN 'MGN_PROFIT' THEN
                      v_mgn_profit_accu_krw_amt
                 WHEN 'COI' THEN
                      v_op_inc_accu_krw_amt
                 WHEN 'SALE_QTY' THEN
                      v_sales_qty_accu_krw
                 WHEN 'GROSS_SALE' THEN
                      v_gross_sales_accu_krw_amt   
                 ELSE 0 END) as ACCU_KRW_AMOUNT,
            sum(CASE C.CODE_ID
                 WHEN 'SALE' THEN
                      v_net_sales_accu_usd_amt
                 WHEN 'MGN_PROFIT' THEN
                      v_mgn_profit_accu_usd_amt
                 WHEN 'COI' THEN
                      v_op_inc_accu_usd_amt
                 WHEN 'SALE_QTY' THEN
                      v_sales_qty_accu_usd
                 WHEN 'GROSS_SALE' THEN
                      v_gross_sales_accu_usd_amt
                 ELSE 0 END) as ACCU_USD_AMOUNT
     FROM(
          SELECT BASIS_YYYYMM,
                 SCENARIO_TYPE_CODE,
                 GBU_CODE,
                 LEVEL_KEY_CODE AS CATEGORY_DETAIL_CODE,               
                 SUM(v_net_sales_krw_amt * PROD_SIGN_CODE) as v_net_sales_krw_amt,
                 SUM(v_net_sales_usd_amt * PROD_SIGN_CODE) as v_net_sales_usd_amt,     
                 SUM(v_mgn_profit_krw_amt * PROD_SIGN_CODE) as v_mgn_profit_krw_amt,
                 SUM(v_mgn_profit_usd_amt * PROD_SIGN_CODE) as v_mgn_profit_usd_amt,
                 SUM(v_op_inc_krw_amt * PROD_SIGN_CODE) as v_op_inc_krw_amt,
                 SUM(v_op_inc_usd_amt * PROD_SIGN_CODE) as v_op_inc_usd_amt,
                 SUM(v_sales_qty_krw * PROD_SIGN_CODE) as v_sales_qty_krw,
                 SUM(v_sales_qty_usd * PROD_SIGN_CODE) as v_sales_qty_usd,
                 SUM(v_gross_sales_krw_amt * PROD_SIGN_CODE) as v_gross_sales_krw_amt,
                 SUM(v_gross_sales_usd_amt * PROD_SIGN_CODE) as v_gross_sales_usd_amt,
                 SUM(v_net_sales_accu_krw_amt * PROD_SIGN_CODE) as v_net_sales_accu_krw_amt,
                 SUM(v_net_sales_accu_usd_amt * PROD_SIGN_CODE) as v_net_sales_accu_usd_amt,     
                 SUM(v_mgn_profit_accu_krw_amt * PROD_SIGN_CODE) as v_mgn_profit_accu_krw_amt,
                 SUM(v_mgn_profit_accu_usd_amt * PROD_SIGN_CODE) as v_mgn_profit_accu_usd_amt,
                 SUM(v_op_inc_accu_krw_amt * PROD_SIGN_CODE) as v_op_inc_accu_krw_amt,
                 SUM(v_op_inc_accu_usd_amt * PROD_SIGN_CODE) as v_op_inc_accu_usd_amt,
                 SUM(v_sales_qty_accu_krw * PROD_SIGN_CODE) as v_sales_qty_accu_krw,
                 SUM(v_sales_qty_accu_usd * PROD_SIGN_CODE) as v_sales_qty_accu_usd,
                 SUM(v_gross_sales_accu_krw_amt * PROD_SIGN_CODE) as v_gross_sales_accu_krw_amt,
                 SUM(v_gross_sales_accu_usd_amt * PROD_SIGN_CODE) as v_gross_sales_accu_usd_amt
          FROM  (SELECT BASIS_YYYYMM,
                        SCENARIO_TYPE_CODE,
                        GBU_CODE,                      
                        PRODUCT_LEVEL4_CODE,
                        MODEL_SUFFIX_CODE,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 NET_SALES_AMT ELSE 0 END) as v_net_sales_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN 
                                 NET_SALES_AMT ELSE 0 END) as v_net_sales_usd_amt,     
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 MARGINAL_PROFIT_AMT ELSE 0 END) as v_mgn_profit_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 MARGINAL_PROFIT_AMT ELSE 0 END) as v_mgn_profit_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 OP_INC_AMT ELSE 0 END) as v_op_inc_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 OP_INC_AMT ELSE 0 END) as v_op_inc_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 SALES_QTY ELSE 0 END) as v_sales_qty_krw,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 SALES_QTY ELSE 0 END) as v_sales_qty_usd,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 GROSS_SALES_AMT ELSE 0 END) as v_gross_sales_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN 
                                 GROSS_SALES_AMT ELSE 0 END) as v_gross_sales_usd_amt,         
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN
                                 NET_SALES_AMT ELSE 0 END) as v_net_sales_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN
                                 NET_SALES_AMT ELSE 0 END) as v_net_sales_accu_usd_amt,     
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 MARGINAL_PROFIT_AMT ELSE 0 END) as v_mgn_profit_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 MARGINAL_PROFIT_AMT ELSE 0 END) as v_mgn_profit_accu_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN
                                 OP_INC_AMT ELSE 0 END) as v_op_inc_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 OP_INC_AMT ELSE 0 END) as v_op_inc_accu_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 SALES_QTY ELSE 0 END) as v_sales_qty_accu_krw,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN
                                 SALES_QTY ELSE 0 END) as v_sales_qty_accu_usd,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 GROSS_SALES_AMT ELSE 0 END) as v_gross_sales_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 GROSS_SALES_AMT ELSE 0 END) as v_gross_sales_accu_usd_amt                                          
                 FROM  IPTDW.IPTDW_IFF_CONS_BEP_MODEL_BUYER
                 WHERE BASIS_YYYYMM = p_yyyymm
                 AND   ALLOCATION_FLAG = 'Y'           -- 매출원가차이배부여부
                 AND   OTHER_SALES_ALLOC_FLAG = 'Y'    -- 기타매출포함여부('N':포함,'Y':제외)
                 AND   CONSLD_SALES_MODEL_FLAG = 'Y'   -- 연결매출만
                 AND   VIEW_CODE IN ('PTD', 'YTD')             
                 AND   CURRENCY_CODE IN ('KRW', 'USD')
                 AND   SCENARIO_TYPE_CODE = 'AC0'
                 GROUP BY BASIS_YYYYMM,
                          SCENARIO_TYPE_CODE,
                          GBU_CODE,                      
                          PRODUCT_LEVEL4_CODE,
                          MODEL_SUFFIX_CODE )  A
          INNER JOIN IPTDW.IPTDW_RES_DIM_DIVISION_PROD_AND_BIZ B
          ON  B.DIVISION_CODE = A.GBU_CODE
          AND B.PRODUCT_LEVEL4_CODE = A.PRODUCT_LEVEL4_CODE
          AND ( B.MAPPING_TYPE_CODE = 'P'
          OR    B.MAPPING_TYPE_CODE = 'M' AND B.MODEL_SUFFIX_CODE = A.MODEL_SUFFIX_CODE )
          GROUP BY BASIS_YYYYMM,
                   SCENARIO_TYPE_CODE,
                   GBU_CODE,
                   LEVEL_KEY_CODE
          UNION ALL
          SELECT BASIS_YYYYMM,
                 SCENARIO_TYPE_CODE,
                 GBU_CODE,
                 LEVEL_KEY_CODE AS CATEGORY_DETAIL_CODE,               
                 SUM(v_net_sales_krw_amt * PROD_SIGN_CODE) as v_net_sales_krw_amt,
                 SUM(v_net_sales_usd_amt * PROD_SIGN_CODE) as v_net_sales_usd_amt,     
                 SUM(v_mgn_profit_krw_amt * PROD_SIGN_CODE) as v_mgn_profit_krw_amt,
                 SUM(v_mgn_profit_usd_amt * PROD_SIGN_CODE) as v_mgn_profit_usd_amt,
                 SUM(v_op_inc_krw_amt * PROD_SIGN_CODE) as v_op_inc_krw_amt,
                 SUM(v_op_inc_usd_amt * PROD_SIGN_CODE) as v_op_inc_usd_amt,
                 SUM(v_sales_qty_krw * PROD_SIGN_CODE) as v_sales_qty_krw,
                 SUM(v_sales_qty_usd * PROD_SIGN_CODE) as v_sales_qty_usd,
                 SUM(v_gross_sales_krw_amt * PROD_SIGN_CODE) as v_gross_sales_krw_amt,
                 SUM(v_gross_sales_usd_amt * PROD_SIGN_CODE) as v_gross_sales_usd_amt,
                 SUM(v_net_sales_accu_krw_amt * PROD_SIGN_CODE) as v_net_sales_accu_krw_amt,
                 SUM(v_net_sales_accu_usd_amt * PROD_SIGN_CODE) as v_net_sales_accu_usd_amt,     
                 SUM(v_mgn_profit_accu_krw_amt * PROD_SIGN_CODE) as v_mgn_profit_accu_krw_amt,
                 SUM(v_mgn_profit_accu_usd_amt * PROD_SIGN_CODE) as v_mgn_profit_accu_usd_amt,
                 SUM(v_op_inc_accu_krw_amt * PROD_SIGN_CODE) as v_op_inc_accu_krw_amt,
                 SUM(v_op_inc_accu_usd_amt * PROD_SIGN_CODE) as v_op_inc_accu_usd_amt,
                 SUM(v_sales_qty_accu_krw * PROD_SIGN_CODE) as v_sales_qty_accu_krw,
                 SUM(v_sales_qty_accu_usd * PROD_SIGN_CODE) as v_sales_qty_accu_usd,
                 SUM(v_gross_sales_accu_krw_amt * PROD_SIGN_CODE) as v_gross_sales_accu_krw_amt,
                 SUM(v_gross_sales_accu_usd_amt * PROD_SIGN_CODE) as v_gross_sales_accu_usd_amt
          FROM  (SELECT BASIS_YYYYMM,
                        SCENARIO_TYPE_CODE,
                        GBU_CODE,                      
                        PRODUCT_LEVEL4_CODE,
                        SALES_MODEL_SUFFIX_CODE,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 NET_SALES ELSE 0 END) as v_net_sales_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN 
                                 NET_SALES ELSE 0 END) as v_net_sales_usd_amt,     
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 MARGINAL_PROFIT ELSE 0 END) as v_mgn_profit_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 MARGINAL_PROFIT ELSE 0 END) as v_mgn_profit_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 OPERATING_INCOME ELSE 0 END) as v_op_inc_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 OPERATING_INCOME ELSE 0 END) as v_op_inc_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 SALES_QTY ELSE 0 END) as v_sales_qty_krw,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN
                                 SALES_QTY ELSE 0 END) as v_sales_qty_usd,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 GROSS_TOTAL_SALES ELSE 0 END) as v_gross_sales_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'PTD' AND CURRENCY_CODE = 'USD' THEN 
                                 GROSS_TOTAL_SALES ELSE 0 END) as v_gross_sales_usd_amt,                                  
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN
                                 NET_SALES ELSE 0 END) as v_net_sales_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN
                                 NET_SALES ELSE 0 END) as v_net_sales_accu_usd_amt,     
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 MARGINAL_PROFIT ELSE 0 END) as v_mgn_profit_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 MARGINAL_PROFIT ELSE 0 END) as v_mgn_profit_accu_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN
                                 OPERATING_INCOME ELSE 0 END) as v_op_inc_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 OPERATING_INCOME ELSE 0 END) as v_op_inc_accu_usd_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 SALES_QTY ELSE 0 END) as v_sales_qty_accu_krw,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN
                                 SALES_QTY ELSE 0 END) as v_sales_qty_accu_usd,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'KRW' THEN 
                                 GROSS_TOTAL_SALES ELSE 0 END) as v_gross_sales_accu_krw_amt,
                        SUM(CASE WHEN VIEW_CODE = 'YTD' AND CURRENCY_CODE = 'USD' THEN 
                                 GROSS_TOTAL_SALES ELSE 0 END) as v_gross_sales_accu_usd_amt                                   
                 FROM  IPTDW.IPTDW_IFF_CONS_BEP_MODEL_BUYER_ROFO
                 WHERE BASIS_YYYYMM = p_yyyymm
                 AND   OTHER_SALES = 'AFTER'    -- 기타매출 제외
                 AND   PROFIT_STEP = 'NORMAL'   -- 시산배부전 기준
                 AND   VIEW_CODE IN ('PTD', 'YTD')
                 AND   CURRENCY_CODE IN ('KRW', 'USD')
                 AND   SCENARIO_TYPE_CODE in ('PR1','PR2','PR3','PR4')              
                 GROUP BY BASIS_YYYYMM,
                          SCENARIO_TYPE_CODE,
                          GBU_CODE,                      
                          PRODUCT_LEVEL4_CODE,
                          SALES_MODEL_SUFFIX_CODE )  A
          INNER JOIN IPTDW.IPTDW_RES_DIM_DIVISION_PROD_AND_BIZ B
          ON  B.DIVISION_CODE = A.GBU_CODE
          AND B.PRODUCT_LEVEL4_CODE = A.PRODUCT_LEVEL4_CODE
          AND ( B.MAPPING_TYPE_CODE = 'P'
          OR    B.MAPPING_TYPE_CODE = 'M' AND B.MODEL_SUFFIX_CODE = A.SALES_MODEL_SUFFIX_CODE )
          GROUP BY BASIS_YYYYMM,
                   SCENARIO_TYPE_CODE,
                   GBU_CODE,
                   LEVEL_KEY_CODE 
       ) M
       LEFT OUTER JOIN IPTDW.IPTDW_RES_DIM_CODES C
       ON   C.CODE_TYPE = 'KPI_TYPE'
       AND  C.CODE_ID in ('SALE','MGN_PROFIT','COI','SALE_QTY','GROSS_SALE') -- 2014.10.31 KPI 추가
       GROUP BY M.BASIS_YYYYMM,
                M.SCENARIO_TYPE_CODE,
                M.GBU_CODE,
                C.CODE_ID,
                M.CATEGORY_DETAIL_CODE
    )
    SELECT temp1.basis_yyyymm,
           temp1.scenario_type_code,
           temp1.gbu_code,
           'N' as manual_adjust_flag,
           temp1.kpi_type_code,
           p_category,
           temp1.category_detail_code,
           CASE temp1.scenario_type_code 
                WHEN 'AC0' THEN temp1.basis_yyyymm
                WHEN 'PR1' THEN to_char(to_date(temp1.basis_yyyymm,'YYYYMM')+ 1 month, 'YYYYMM')
                WHEN 'PR2' THEN to_char(to_date(temp1.basis_yyyymm,'YYYYMM')+ 2 month, 'YYYYMM')
                WHEN 'PR3' THEN to_char(to_date(temp1.basis_yyyymm,'YYYYMM')+ 3 month, 'YYYYMM')
                WHEN 'PR4' THEN to_char(to_date(temp1.basis_yyyymm,'YYYYMM')+ 4 month, 'YYYYMM')
           END,
           temp1.curr_mon_krw_amount,
           temp1.curr_mon_usd_amount,
           temp1.accu_krw_amount,
           temp1.accu_usd_amount,
           current timestamp,
           'ares',
           current timestamp,
           'ares'                   
    FROM temp1;
  
    GET DIAGNOSTICS  v_target_insert_count = ROW_COUNT;
    
    /*--------------------------------------------
       상위 사업부 데이터 생성    
    ---------------------------------------------*/
    INSERT 
    INTO  IPTDW.IPTDW_RES_KPI_PRODUCT
    ( BASIS_YYYYMM
     ,SCENARIO_CODE
     ,DIVISION_CODE
     ,MANUAL_ADJUST_FLAG
     ,KPI_TYPE_CODE
     ,CATEGORY_CODE
     ,CATEGORY_DETAIL_CODE
     ,YYYYMM   
     ,CURR_MON_KRW_AMOUNT
     ,CURR_MON_USD_AMOUNT
     ,ACCU_KRW_AMOUNT
     ,ACCU_USD_AMOUNT
     ,CREATION_DATE
     ,CREATE_USER_ID
     ,LAST_UPDATE_DATE
     ,LAST_UPDATE_USER_ID )
    SELECT  A.BASIS_YYYYMM
           ,A.SCENARIO_CODE
           ,B.ANCESTOR
           ,A.MANUAL_ADJUST_FLAG
           ,A.KPI_TYPE_CODE
           ,A.CATEGORY_CODE
           ,A.CATEGORY_DETAIL_CODE
           ,A.YYYYMM
           ,SUM(A.CURR_MON_KRW_AMOUNT)
           ,SUM(A.CURR_MON_USD_AMOUNT)
           ,SUM(A.ACCU_KRW_AMOUNT)
           ,SUM(A.ACCU_USD_AMOUNT)
           ,CURRENT TIMESTAMP
           ,'ares'
           ,CURRENT TIMESTAMP
           ,'ares'
    FROM IPTDW.IPTDW_RES_KPI_PRODUCT A,
        (   
        
        /* 2015-02-07 Level 5 추가 */ 
		SELECT tree.grand_parent AS ancestor,  
		       CASE COALESCE(tree.grand_grand_grand_child,'***') 
		           WHEN '***' 
		           THEN 
		               CASE COALESCE(tree.grand_grand_child,'***') 
		                   WHEN '***' 
		                   THEN 
		                       CASE COALESCE(tree.grand_child,'***') 
		                           WHEN '***' 
		                           THEN 
		                               CASE COALESCE(tree.child,'***') 
		                                   WHEN '***' 
		                                   THEN tree.parent 
		                                   ELSE tree.child 
		                               END 
		                           ELSE tree.grand_child 
		                       END 
		                   ELSE tree.grand_grand_child 
		               END 
		           ELSE tree.grand_grand_grand_child 
		       END AS leaf_child  
		  FROM 
		       (SELECT HIER1.PARENT_DIVISION_CODE AS GRAND_PARENT, 
		              HIER1.DIVISION_CODE AS PARENT, 
		              HIER2.DIVISION_CODE AS CHILD, 
		              HIER3.DIVISION_CODE AS GRAND_CHILD, 
		              HIER4.DIVISION_CODE AS GRAND_GRAND_CHILD, 
		              /* 2015-02-07 Level 5 추가 */ 
		              HIER5.DIVISION_CODE AS GRAND_GRAND_GRAND_CHILD 
		         FROM 
		              (SELECT c.DIVISION_CODE, 
		                     c.PARENT_DIVISION_CODE 
		                FROM IPTDW_RES_DIM_COMM_DIVISION c, 
		                     IPTDW_RES_DIM_COMM_DIVISION p 
		               WHERE c.BASIS_YYYYMM = p_div_yyyymm 
		                     AND c.BASIS_YYYYMM = p.BASIS_YYYYMM 
		                     AND c.PARENT_DIVISION_CODE = p.DIVISION_CODE 
		                     AND COALESCE(c.PARENT_DIVISION_CODE,'***') <> '***' 
		                     AND nvl(c.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		                     AND nvl(p.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		              ) AS HIER1 
		          LEFT OUTER JOIN 
		              (SELECT c.DIVISION_CODE, 
		                     c.PARENT_DIVISION_CODE 
		                FROM IPTDW_RES_DIM_COMM_DIVISION c, 
		                     IPTDW_RES_DIM_COMM_DIVISION p 
		               WHERE c.BASIS_YYYYMM = p_div_yyyymm 
		                     AND c.BASIS_YYYYMM = p.BASIS_YYYYMM 
		                     AND c.PARENT_DIVISION_CODE = p.DIVISION_CODE 
		                     AND COALESCE(c.PARENT_DIVISION_CODE,'***') <> '***' 
		                     AND nvl(c.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		                     AND nvl(p.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		              ) AS HIER2 
		              ON HIER1.DIVISION_CODE = HIER2.PARENT_DIVISION_CODE 
		          LEFT OUTER JOIN 
		              (SELECT c.DIVISION_CODE, 
		                     c.PARENT_DIVISION_CODE 
		                FROM IPTDW_RES_DIM_COMM_DIVISION c, 
		                     IPTDW_RES_DIM_COMM_DIVISION p 
		               WHERE c.BASIS_YYYYMM = p_div_yyyymm 
		                     AND c.BASIS_YYYYMM = p.BASIS_YYYYMM 
		                     AND c.PARENT_DIVISION_CODE = p.DIVISION_CODE 
		                     AND COALESCE(c.PARENT_DIVISION_CODE,'***') <> '***' 
		                     AND nvl(c.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		                     AND nvl(p.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		              ) AS HIER3 
		              ON HIER2.DIVISION_CODE = HIER3.PARENT_DIVISION_CODE 
		          LEFT OUTER JOIN 
		              (SELECT c.DIVISION_CODE, 
		                     c.PARENT_DIVISION_CODE 
		                FROM IPTDW_RES_DIM_COMM_DIVISION c, 
		                     IPTDW_RES_DIM_COMM_DIVISION p 
		               WHERE c.BASIS_YYYYMM = p_div_yyyymm 
		                     AND c.BASIS_YYYYMM = p.BASIS_YYYYMM 
		                     AND c.PARENT_DIVISION_CODE = p.DIVISION_CODE 
		                     AND COALESCE(c.PARENT_DIVISION_CODE,'***') <> '***' 
		                     AND nvl(c.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		                     AND nvl(p.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		              ) AS HIER4 
		              ON HIER3.DIVISION_CODE = HIER4.PARENT_DIVISION_CODE /* 2015-02-07 Level 5 추가 */ 
		          LEFT OUTER JOIN 
		              (SELECT c.DIVISION_CODE, 
		                     c.PARENT_DIVISION_CODE 
		                FROM IPTDW_RES_DIM_COMM_DIVISION c, 
		                     IPTDW_RES_DIM_COMM_DIVISION p 
		               WHERE c.BASIS_YYYYMM = p_div_yyyymm 
		                     AND c.BASIS_YYYYMM = p.BASIS_YYYYMM 
		                     AND c.PARENT_DIVISION_CODE = p.DIVISION_CODE 
		                     AND COALESCE(c.PARENT_DIVISION_CODE,'***') <> '***' 
		                     AND nvl(c.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		                     AND nvl(p.attribute1,'Y') <> 'N' -- 가상 division 제외. 20130908 
		              ) AS HIER5 
		              ON HIER4.DIVISION_CODE = HIER5.PARENT_DIVISION_CODE 
		       ) AS tree              
         ) B                      
    WHERE A.DIVISION_CODE = B.LEAF_CHILD
    AND   A.BASIS_YYYYMM  = p_yyyymm
    AND   A.CATEGORY_CODE = p_category
    AND   A.MANUAL_ADJUST_FLAG = 'N'
    GROUP BY A.BASIS_YYYYMM
            ,A.SCENARIO_CODE
            ,B.ANCESTOR
            ,A.MANUAL_ADJUST_FLAG
            ,A.KPI_TYPE_CODE
            ,A.CATEGORY_CODE
            ,A.CATEGORY_DETAIL_CODE
            ,A.YYYYMM
    WITH UR;    
   
    SET v_load_progress_status_code        = SQLSTATE;

    COMMIT;
    
    /*--------------------
       ETL JOB LOG 생성
    ---------------------*/
    CALL sp_cd_etl_job_logs( v_etl_job_no,
                             v_basis_yyyymmdd,
                             v_load_start_timestamp,
                             v_serial_no,
                             v_load_progress_status_code,
                             v_target_insert_count,
                             v_target_update_count,
                             v_target_delete_count,
                             v_source_table_name,
                             v_target_table_name,
                             v_job_notes
                           );

    COMMIT;
    
    /*--------------------------------
       ARES JOB MONITORING LOG 생성
    ---------------------------------*/    
    FOR C1 AS
        SELECT DISTINCT
               DIVISION_CODE as v_division_code,
               SUBSTR(SCENARIO_CODE,1,2) as v_scenario_code
        FROM   IPTDW.IPTDW_RES_KPI_PRODUCT
        WHERE  BASIS_YYYYMM  = p_yyyymm
        AND    CATEGORY_CODE = p_category
        AND    MANUAL_ADJUST_FLAG = 'N'
    DO
        CALL sp_cd_res_job_monitor_logs( p_yyyymm, 
                                         p_category, 
                                         v_scenario_code, 
                                         'SYSTEM',
                                         v_division_code,
                                         'ares'
                                        );
    END FOR;

    COMMIT;
    
END