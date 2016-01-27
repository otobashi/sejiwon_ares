CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2C_MAJOR_PROD (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD VARCHAR(8)
     --IN P_DIVISION VARCHAR(3),
     --IN P_CURRENCY VARCHAR(3)
     )
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
  /*******************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                        */
  /* 2.모       듈 :                                                             */
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2C_MAJOR_PROD                                */
  /*                                                                             */
  /* 4.설       명 : SMART B2C/B2B 매출/영업이익 추이를 Result Set으로 return함  */
  /*                                                                             */
  /* 5.입 력 변 수 :                                                             */
  /*                                                                             */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                 */
  /*                 IN P_SUBSDR_CD( 법인 )                                      */
  /*                 IN P_CODE_TYPE( B2C/B2B구분 )                               */
  /* 6.파 일 위 치 :                                                             */
  /* 7.변 경 이 력 :                                                             */
  /*                                                                             */
  /*  version  작성자  일      자  내                 용  요   청   자           */
  /*  -------  ------  ----------  ---------------------  ------------           */
  /*  1.0      shlee   2016.01.07  최초 작성              신명식c                */
  /*******************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2C_MAJOR_PROD';
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

    DECLARE C1 CURSOR WITH HOLD WITH RETURN FOR

-- 1.법인합계
      SELECT '1.법인합계'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.BASE_YYYYMM        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND '201601'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = 'EKHQ'
      GROUP BY A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

      UNION ALL

-- 2.사업부합계
      SELECT B.DISP_NM            AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.BASE_YYYYMM        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT DIVISION_CODE AS DIV_CD
                   ,CASE WHEN DIVISION_CODE = 'GLT' THEN 'HE LCD TV'
                         WHEN DIVISION_CODE = 'MST' THEN 'MC 단말'
                         WHEN DIVISION_CODE = 'CNT' THEN 'H&A 냉장고'
                         ELSE DISPLAY_NAME END AS DISP_NM
             FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION
             WHERE  USE_FLAG = 'Y'
             AND    BASIS_YYYYMM = '201601'
             AND    DIVISION_CODE IN ('GLT','MST','CNT','DFT','DGT')
            ) B
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND '201601'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.DIV_CD IN ('GLT','MST','CNT','DFT','DGT')
      AND   A.DIV_CD = B.DIV_CD
      GROUP BY B.DISP_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

      UNION ALL

-- 3 LEVEL2
      SELECT B.PROD_KOR_NM        AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.BASE_YYYYMM        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT ATTRIBUTE1    AS PROD_CD
                   ,DISPLAY_NAME1 AS PROD_KOR_NM
                   ,DISPLAY_NAME2 AS PROD_ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER 
             WHERE  CODE_TYPE = 'SMR_PROD_MST'
             AND    USE_FLAG  = 'Y'
             AND    ATTRIBUTE2= 'B2C_DIV'
            ) B
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND '201601'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.DIV_CD IN ('GLT','MST','CNT','DFT','DGT')
      AND   A.ATTRIBUTE3_VALUE = B.PROD_CD
      GROUP BY B.PROD_KOR_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD


      UNION ALL

-- 4 LEVEL3
      SELECT B.PROD_KOR_NM        AS COL_INDEX         
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,A.BASE_YYYYMM        AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'1'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT ATTRIBUTE1    AS PROD_CD
                   ,DISPLAY_NAME1 AS PROD_KOR_NM
                   ,DISPLAY_NAME2 AS PROD_ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER 
             WHERE  CODE_TYPE = 'SMR_PROD_MST'
             AND    USE_FLAG  = 'Y'
             AND    ATTRIBUTE2= 'B2C_DIV'
            ) B
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR('201601',1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND '201601'
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_PROD'
      AND   A.KPI_CD in ('SALE', 'COI')
      AND   A.SUBSDR_CD = 'EKHQ'
      AND   A.DIV_CD IN ('GLT','MST','CNT','DFT','DGT')
      AND   A.ATTRIBUTE4_VALUE = B.PROD_CD
      GROUP BY B.PROD_KOR_NM
              ,A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

    WITH UR;

    OPEN C1;
   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_SUBSDR_CNTRY';
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