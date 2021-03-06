CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2B_PIPELINE (
     IN P_BASIS_YYYYMM VARCHAR(6)
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
  /************************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                         */
  /* 2.모       듈 :                                                                              */
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2B_PIPELINE                                                   */
  /* 4.설       명 : SMART 월별Trend(파이프라인)을 Result Set으로 return함                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.12  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2B_PIPELINE';
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

-- 1.법인전체
      SELECT '법인전체'           AS COL_INDEX
            ,CASE WHEN A.KPI_CD = 'BLSN'     THEN '수주잔고'
                  WHEN A.KPI_CD = 'SALE'     THEN '매출(전체)'
                  WHEN A.KPI_CD = 'SALE_PRJ' THEN '매출(프로젝트)'
                  ELSE A.KPI_CD END            AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('BLSN','SALE_PRJ','SALE')
      AND    A.CAT_CD IN ('BEP_SMART_PIPE','BEP_SMART_DIV')
      GROUP BY CASE WHEN A.KPI_CD = 'BLSN'     THEN '수주잔고'
                    WHEN A.KPI_CD = 'SALE'     THEN '매출(전체)'
                    WHEN A.KPI_CD = 'SALE_PRJ' THEN '매출(프로젝트)'
                    ELSE A.KPI_CD END
              ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END
     
      UNION ALL

-- 2.각 사업부별
      SELECT C.KOR_NM AS COL_INDEX
            ,CASE WHEN A.KPI_CD = 'BLSN'     THEN '수주잔고'
                  WHEN A.KPI_CD = 'SALE'     THEN '매출(전체)'
                  WHEN A.KPI_CD = 'SALE_PRJ' THEN '매출(프로젝트)'
                  ELSE A.KPI_CD END            AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
            ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2B_DIV') C
      WHERE  A.DIV_CD = C.DIV_CD
      AND    A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('BLSN','SALE_PRJ','SALE')
      AND    A.CAT_CD IN ('BEP_SMART_PIPE','BEP_SMART_DIV')
      GROUP BY C.KOR_NM
              ,CASE WHEN A.KPI_CD = 'BLSN'     THEN '수주잔고'
                    WHEN A.KPI_CD = 'SALE'     THEN '매출(전체)'
                    WHEN A.KPI_CD = 'SALE_PRJ' THEN '매출(프로젝트)'
                    ELSE A.KPI_CD END
              ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END

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