CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_KPI_BEPRANGE_HISTORY (
    IN P_BASIS_YYYYMM	VARCHAR(6),
    IN P_DIVISION_CODE	VARCHAR(8),
    IN P_DIV_YYYYMM	VARCHAR(6) )
  SPECIFIC "SQL130628101617100"
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  /****************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                     */
  /* 2.모       듈 :                                                          */
  /* 3.프로그램 ID : SP_CD_RES_KPI_BEPRANGE_HISTORY                           */
  /*                                                                          */
  /* 4.설       명 : IPTDW_RES_KPI_PRODUCT에서 사업부별로 한계이익 구간별     */
  /*                 데이터를 집계하여 Result Set으로 return함                */
  /*                                                                          */
  /* 5.입 력 변 수 :                                                          */
  /*                                                                          */
  /*                 IN p_basis_yyyymm( 기준월 )                              */
  /*                 IN p_division_code( 사업부 )                             */
  /*                 IN p_div_yyyymm( 조직기준월 )                            */
  /* 6.파 일 위 치 :                                                          */
  /* 7.변 경 이 력 :                                                          */
  /*                                                                          */
  /*  version  작성자  일      자  내                 용                      */
  /*  -------  ------  ----------  ------------------------------------------ */
  /*  1.0                                                                     */
  /*  1.1      syyim   2014.09.03  ARES 고도화 - 저수익 기준 풋노트 쿼리 추가 */
  /****************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_BEPRANGE_HISTORY';
    DECLARE v_load_start_timestamp       TIMESTAMP     DEFAULT NULL;
    DECLARE v_serial_no                  VARCHAR(30)   DEFAULT NULL;
    DECLARE v_load_progress_status_code  VARCHAR(10)   DEFAULT NULL;
    DECLARE v_target_insert_count        INTEGER       DEFAULT 0;
    DECLARE v_target_update_count        INTEGER       DEFAULT 0;
    DECLARE v_target_delete_count        INTEGER       DEFAULT 0;  
    DECLARE v_source_table_name          VARCHAR(300)  DEFAULT NULL;
    DECLARE v_target_table_name          VARCHAR(300)  DEFAULT NULL; 
    DECLARE v_job_notes                  VARCHAR(300)  DEFAULT NULL;
    DECLARE v_basis_yyyymmdd             VARCHAR(8)    DEFAULT NULL;
    DECLARE SQLSTATE                     CHAR(5)       DEFAULT '';
    
    DECLARE C1 CURSOR WITH HOLD WITH RETURN FOR -- CURSOR WITH RETURN FOR(프로시저 간 결과 세트를 호출할 때)
      select  B.display_name,
              A.row_index,
              A.yyyymm,
              A.amount
      from (
          -- 1.시산매출
          select division_code,
                 '11.시산매출' AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount           
          from  IPTDW.IPTDW_RES_KPI_DIVISION
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                             and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm  = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_code = 'TB'
          and   kpi_type_code = 'SALE'
          group by division_code,
                   yyyymm
          union all
          -- 1-1.전월이동매출
          select division_code,
                 '11.전월이동매출' AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount
          from  IPTDW.IPTDW_RES_KPI_DIVISION
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                             and to_char(to_date(p_basis_yyyymm, 'YYYYMM') - 1 month, 'YYYYMM') 
          and  (scenario_code = 'PR1'
          or    basis_yyyymm  = to_char(to_date(p_basis_yyyymm, 'YYYYMM') - 1 month, 'YYYYMM') 
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_code = 'TB'
          and   kpi_type_code = 'SALE'
          group by division_code,
                   yyyymm
          union all
          -- 2.OH 실적 고정비, 한계이익, FUND
          select division_code,
                 '12.'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount           
          from  IPTDW.IPTDW_RES_KPI_DIVISION
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                             and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm  = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_code = 'OH_MANUAL'
          and   kpi_type_code in ('OH_F_COST','OH_MGN_PROFIT','OH_FUND')
          group by division_code,                   
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '12.'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount           
          from  IPTDW.IPTDW_RES_KPI_DIVISION A
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                             and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm  = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_code = 'OH'
          and   kpi_type_code in ('OH_F_COST','OH_MGN_PROFIT','OH_FUND')
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_DIVISION
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    category_code = 'OH_MANUAL'
                            and    kpi_type_code = A.KPI_TYPE_CODE)
          group by division_code,                   
                   kpi_type_code,
                   yyyymm
          union all
          -- 3.OH 전월이동 한계이익, FUND
          select division_code,
                 '13.전월이동_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount           
          from  IPTDW.IPTDW_RES_KPI_DIVISION
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                             and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month,'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm  = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month,'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_code = 'OH_MANUAL'
          and   kpi_type_code in ('OH_MGN_PROFIT','OH_FUND')
          group by division_code,                   
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '13.전월이동_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount           
          from  IPTDW.IPTDW_RES_KPI_DIVISION A
          where basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                             and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month,'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm  = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month,'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_code = 'OH'
          and   kpi_type_code in ('OH_MGN_PROFIT','OH_FUND')
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_DIVISION
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    category_code = 'OH_MANUAL'
                            and    kpi_type_code = A.KPI_TYPE_CODE)
          group by division_code,                   
                   kpi_type_code,
                   yyyymm
          union all
          -- 4.BEP_RANGE 매출
          select division_code,
                 '14.BEP_RANGE_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          --and   category_detail_code like 'MARGINAL%'
          and   category_detail_code = 'MARGINAL_PF_TOT'
          and   kpi_type_code in ( 'SALE' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '14.BEP_RANGE_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code like 'MARGINAL%'
          --and    category_detail_code = 'MARGINAL_PF_TOT'
          and   kpi_type_code in ( 'SALE'  )
          and   category_code = 'BEP_RANGE'
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and   category_detail_code = 'MARGINAL_PF_TOT'
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    kpi_type_code in ( 'SALE'  )
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 4.BEP_RANGE 모델수
          select division_code,
                 '14.BEP_RANGE_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code like 'MARGINAL%'
          and   kpi_type_code in ( 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '14.BEP_RANGE_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code like 'MARGINAL%'
          and   kpi_type_code in ( 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code like 'MARGINAL%'
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    kpi_type_code in ( 'MODEL_COUNT' )
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all          
          -- 5.BEP_RANGE 전월이동 매출
          select division_code,
                 '15.BEP_RANGE_전월이동_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                              and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_detail_code like 'MARGINAL%'
          and   kpi_type_code in ( 'SALE' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'          
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '15.BEP_RANGE_전월이동_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                              and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_detail_code like 'MARGINAL%'
          and   kpi_type_code in ( 'SALE' )
          and   category_code = 'BEP_RANGE' 
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code like 'MARGINAL%'
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 6.한계이익 30%이상
          select division_code,
                 '16.한계이익 30%이상_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_30'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'          
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '16.한계이익 30%이상_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_30'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 7.한계이익 20% ~ 30%
          select division_code,
                 '17.한계이익 20%~30%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_20_30'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '17.한계이익 20%~30%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_20_30'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )       
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all         
          -- 8.한계이익 10% ~ 20%
          select division_code,
                 '18.한계이익 10%~20%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_10_20'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'  
          and   manual_adjust_flag = 'Y'          
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '18.한계이익 10%~20%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_10_20'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'   
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )   
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 9.한계이익 5% ~ 10%
          select division_code,
                 '19.한계이익 5%~10%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_5_10'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'          
          and   manual_adjust_flag = 'Y'          
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '19.한계이익 5%~10%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_5_10'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'            
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 10.한계이익 0% ~ 5%
          select division_code,
                 '20.한계이익 0%~5%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_0_5'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'        
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '20.한계이익 0%~5%_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_0_5'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and   category_code = 'BEP_RANGE'  
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 11.한계이익(-)
          select division_code,
                 '21.한계이익(-)_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_(-)'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT', 'MGN_PROFIT' )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'        
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '21.한계이익(-)_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   category_detail_code = 'MARGINAL_PF_(-)'
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT', 'MGN_PROFIT' )
          and   category_code = 'BEP_RANGE' 
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 12.BEP전월이동 한계이익 0%미만 매출액
          select division_code,
                 '22.BEP전월이동_한계이익(-)_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                              and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_detail_code  = 'MARGINAL_PF_(-)'
          and   kpi_type_code = 'SALE'
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'        
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select division_code,
                 '22.BEP전월이동_한계이익(-)_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 3 year, 'YYYY')||'12'
                              and to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and  (scenario_code = 'PR1'
          or    basis_yyyymm = to_char(to_date(p_basis_yyyymm, 'YYYYMM')- 1 month, 'YYYYMM')
          and   scenario_code in ('PR2', 'PR3', 'PR4'))
          and   category_detail_code  = 'MARGINAL_PF_(-)'
          and   kpi_type_code = 'SALE'
          and   category_code = 'BEP_RANGE' 
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all 
          -- 13.저수익비중
          select A.division_code,
                 '23.저수익_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          left outer join IPTDW.IPTDW_RES_DIM_DIVISION_CODE_MAPPING B
          on     A.DIVISION_CODE = B.DIVISION_CODE
          and    B.CODE_TYPE = 'LOW_PROFIT_RANGE'
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and ( B.CODE_ID is null          and category_detail_code = 'MARGINAL_PF_(-)'
          or    B.CODE_ID = 'MGN_20%_BELOW' and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5', 'MARGINAL_PF_5_10', 'MARGINAL_PF_10_20')
          or    B.CODE_ID = 'MGN_10%_BELOW' and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5', 'MARGINAL_PF_5_10')
          or    B.CODE_ID = 'MGN_5%_BELOW'  and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5') 
          or    B.CODE_ID = 'COI_-15%_BELOW' and category_detail_code = 'COI_-15'
          )
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'
          group by A.division_code,
                   kpi_type_code,
                   yyyymm
          union all
          select A.division_code,
                 '23.저수익_'||kpi_type_code AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT A
          left outer join IPTDW.IPTDW_RES_DIM_DIVISION_CODE_MAPPING B
          on     A.DIVISION_CODE = B.DIVISION_CODE
          and    B.CODE_TYPE = 'LOW_PROFIT_RANGE'
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and p_basis_yyyymm
          and  (scenario_code = 'AC0'
          or    basis_yyyymm = p_basis_yyyymm
          and   scenario_code in ('PR1', 'PR2', 'PR3', 'PR4'))
          and   kpi_type_code in ( 'SALE', 'MODEL_COUNT' )
          and ( B.CODE_ID is null          and category_detail_code = 'MARGINAL_PF_(-)'
          or    B.CODE_ID = 'MGN_20%_BELOW' and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5', 'MARGINAL_PF_5_10', 'MARGINAL_PF_10_20')
          or    B.CODE_ID = 'MGN_10%_BELOW' and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5', 'MARGINAL_PF_5_10')
          or    B.CODE_ID = 'MGN_5%_BELOW'  and category_detail_code in ('MARGINAL_PF_(-)', 'MARGINAL_PF_0_5') 
          or    B.CODE_ID = 'COI_-15%_BELOW' and category_detail_code = 'COI_-15'
          )
          and   category_code = 'BEP_RANGE'
          and   not exists (select *
                            from   IPTDW.IPTDW_RES_KPI_PRODUCT
                            where  basis_yyyymm  = A.BASIS_YYYYMM
                            and    scenario_code = A.SCENARIO_CODE
                            and    division_code = A.DIVISION_CODE
                            and    kpi_type_code = A.KPI_TYPE_CODE
                            and    category_detail_code = A.CATEGORY_DETAIL_CODE
                            and    category_code = 'BEP_RANGE_MANUAL'
                            and    manual_adjust_flag = 'Y' )
          group by A.division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 14.목표한계적자비중
          select division_code,
                 '24.목표한계적자비중' AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and to_char(to_date(p_basis_yyyymm,'YYYYMM') + 4 month, 'YYYYMM')
          and   scenario_code = 'PB0'
          and   category_detail_code = 'MARGINAL_PF_(-)'
          and   kpi_type_code = 'SALE_MINUS_GOAL(%)'
          and   category_code = 'BEP_RANGE_MANUAL'   
          and   manual_adjust_flag = 'Y'          
          group by division_code,
                   kpi_type_code,
                   yyyymm
          union all
          -- 15. 목표한계적자금액
          select division_code,
                 '25. 목표한계적자금액' AS row_index,
                 yyyymm,
                 sum(curr_mon_krw_amount) as amount 
          from   IPTDW.IPTDW_RES_KPI_PRODUCT
          where  basis_yyyymm between to_char(to_date(substr(p_basis_yyyymm,1,4), 'YYYY')- 2 year, 'YYYY')||'01'
                              and to_char(to_date(p_basis_yyyymm,'YYYYMM') + 4 month, 'YYYYMM')
          and   scenario_code = 'PB0'
          and   category_detail_code = 'MARGINAL_PF_(-)'
          and   kpi_type_code = 'MGN_PROFIT_MINUS_GOAL'
          and   category_code = 'BEP_RANGE_MANUAL'
          and   manual_adjust_flag = 'Y'         
          group by division_code,
                   kpi_type_code,
                   yyyymm                    
      ) AS A
      INNER JOIN IPTDW.IPTDW_RES_DIM_COMM_DIVISION B
      ON     A.division_code = B.division_code
      and    B.division_code = p_division_code
      and    B.basis_yyyymm  = p_div_yyyymm
      --order by A.row_index,
      --         A.yyyymm
      union all
      SELECT B.display_name,
             nvl('저수익기준 : '
                 ||case when substr(code_id,1,3) = 'MGN' then '한계이익 ' else '영업이익 ' end
                 ||replace(substr(code_id,5), '_BELOW', ' 이하'), '저수익기준 : 한계이익 0% 이하') as row_index,
             null,
             null
      FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION B
      LEFT OUTER JOIN IPTDW.IPTDW_RES_DIM_DIVISION_CODE_MAPPING A
      on     A.code_type     = 'LOW_PROFIT_RANGE'
      and    A.division_code = B.division_code
      where  B.division_code = p_division_code
      and    B.basis_yyyymm  = p_div_yyyymm      
      with ur;
          
    OPEN C1;

   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_PRODUCT';
    SET v_basis_yyyymmdd             = p_basis_yyyymm;
    SET v_load_progress_status_code  = SQLSTATE;

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
END