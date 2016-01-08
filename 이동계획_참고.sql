CREATE OR REPLACE PROCEDURE SP_CD_RES_KPI_ROLLING_PLAN
(
  IN p_basis_yyyymm  VARCHAR(6)
)
LANGUAGE SQL
DYNAMIC RESULT SETS 1
BEGIN
  /*************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                  */
  /* 2.모       듈 :                                                       */
  /* 3.프로그램 ID : SP_CD_RES_KPI_ROLLING_PLAN                            */
  /*                                                                       */
  /* 4.설       명 : IPTDW.IPTDW_RES_KPI_DIVISION에서                      */
  /*                 매출, 영업이익에 대한 실적 및 이동계획을 집계하여     */
  /*                 Result Set으로 return함                               */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                                                                       */
  /*                 IN p_basis_yyyymm( 기준월 )                           */
  /*                                                                       */
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0                                                                  */
  /*  1.1      czdog   2015.06.22  GUB, MST, EX_MST(MC제외)                */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_ROLLING_PLAN';
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
 	  SELECT CASE WHEN B.NO = 1 THEN A.DIVISION_CODE ELSE 'EX_MST' END DIVISION_CODE
	        ,A.KPI_TYPE_CODE
	        ,A.YYYYMM
	        ,SUM(CASE WHEN B.NO = 1 THEN A.curr_amount ELSE CASE WHEN A.DIVISION_CODE = 'GBU' THEN A.curr_amount ELSE A.curr_amount * (-1) END END) curr_amount
	  FROM   (
		      -- 이동 PR1을 기준월로 하여 전전년 1월실적 ~ 당월 실적
		      select division_code,
		             kpi_type_code,
		             yyyymm,
		             sum(CURR_MON_KRW_AMOUNT) AS curr_amount
		      from  IPTDW.IPTDW_RES_KPI_DIVISION
		      where basis_yyyymm  between substr(to_char(to_date(p_basis_yyyymm,'YYYYMM')- 35 month, 'YYYYMM'),1,4)||'01'
		                          and p_basis_yyyymm
		      and   scenario_code = 'AC0'
		      and   category_code = 'TB'
		      and   kpi_type_code in ('SALE', 'COI')
		      and   division_code in ('GBU', 'MST')
		      group by division_code,
		               kpi_type_code,
		               yyyymm
		      union all
		      -- 당월 이동
		      select division_code,
		             kpi_type_code,
		             yyyymm,
		             sum(CURR_MON_KRW_AMOUNT) AS curr_amount
		      from  IPTDW.IPTDW_RES_KPI_DIVISION
		      where basis_yyyymm  = p_basis_yyyymm
		      and   scenario_code in ('PR1','PR2','PR3','PR4')
		      and   category_code = 'TB'
		      and   kpi_type_code in ('SALE', 'COI')
		      and   division_code in ('GBU', 'MST')
		      group by division_code,
		               kpi_type_code,
		               yyyymm
		      union all
		      -- 전월 이동
		      select division_code,
		             '전월이동_'||kpi_type_code as kpi_type_code,
		             yyyymm,
		             sum(CURR_MON_KRW_AMOUNT) AS curr_amount
		      from  IPTDW.IPTDW_RES_KPI_DIVISION
		      where basis_yyyymm  = to_char(to_date(p_basis_yyyymm,'YYYYMM')- 1 month, 'YYYYMM')
		      and   scenario_code in ('PR2','PR3','PR4')
		      and   category_code = 'TB'
		      and   kpi_type_code in ('SALE', 'COI')
		      and   division_code in ('GBU', 'MST')
		      group by division_code,
		               kpi_type_code,
		               yyyymm
		      union all
		      -- 전전월 이동
		      select division_code,
		             '전전월이동_'||kpi_type_code as kpi_type_code,
		             yyyymm,
		             sum(CURR_MON_KRW_AMOUNT) AS curr_amount
		      from  IPTDW.IPTDW_RES_KPI_DIVISION
		      where basis_yyyymm  = to_char(to_date(p_basis_yyyymm,'YYYYMM')- 2 month, 'YYYYMM')
		      and   scenario_code in ('PR3','PR4')
		      and   category_code = 'TB'
		      and   kpi_type_code in ('SALE', 'COI')
		      and   division_code in ('GBU', 'MST')
		      group by division_code,
		               kpi_type_code,
		               yyyymm
	         ) A
	         JOIN  
	         (
	          SELECT 1 AS NO FROM SYSIBM.SYSDUMMY1 UNION ALL
	          SELECT 2 AS NO FROM SYSIBM.SYSDUMMY1 
	         ) B
	         ON 1=1
      GROUP BY CASE WHEN B.NO = 1 THEN A.DIVISION_CODE ELSE 'EX_MST' END 
	          ,A.KPI_TYPE_CODE
	          ,A.YYYYMM
      with ur;
          
    OPEN C1;

   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_DIVISION';
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