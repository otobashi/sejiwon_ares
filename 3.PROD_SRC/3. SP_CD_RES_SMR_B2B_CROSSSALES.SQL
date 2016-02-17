CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2B_CROSSSALES (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD    VARCHAR(8)     
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2B_CROSSSALES                                                 */
  /* 4.설       명 : SMART 월별Trend(CROSSSALES)을 Result Set으로 return함                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인코드 )                                                   */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.02.02  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2B_CROSSSALES';
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

-- 1.법인/사업부
      SELECT '법인전체'           AS COL_INDEX
            ,A.KPI_CD             AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,0 AS DISP_SEQ
            ,'합계'  AS KOR_NM
            ,'TOTAL' AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('SALE')
      AND    A.CAT_CD IN ('BEP_SMART_DIV')
      AND    A.DIV_CD = 'GBU'
      GROUP BY A.KPI_CD
              ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END
          
      UNION ALL

-- 2.B2B Only
      SELECT 'B2B Only'           AS COL_INDEX
            ,A.KPI_CD             AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,0 AS DISP_SEQ
            ,'B2B Only'  AS KOR_NM
            ,'B2B Only'  AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
            ,(
              SELECT CODE_ID AS DIV_CD
                    ,ATTRIBUTE1 AS DIV_KOR_NM
                    ,ATTRIBUTE2 AS DIV_ENG_NM
              FROM   IPTDW.IPTDW_RES_DIM_CODES
              WHERE  CODE_TYPE = 'B2B_DIV'      
             ) C
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('SALE','CROSS_BORDER','CROSS_BU')
      AND    A.CAT_CD IN ('BEP_SMART_DIV','BEP_SMART_CROSS')
      AND    A.DIV_CD = C.DIV_CD
      GROUP BY A.KPI_CD
              ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END

      UNION ALL

-- 3.사업부
      SELECT A.DIV_CD             AS COL_INDEX
            ,A.KPI_CD             AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,0 AS DISP_SEQ
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
            ,(
              -- 사업부
              SELECT 'DIV' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.DIVISION_CODE     AS CODE
                    ,A.DISPLAY_NAME      AS KOR_NM
                    ,A.DIVISION_NAME     AS ENG_NM
                    ,A.COMPANY_CODE      AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
              WHERE  A.BASIS_YYYYMM = '201601'
              AND    A.USE_FLAG = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD    = Z.CODE
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('SALE')
      AND    A.CAT_CD IN ('BEP_SMART_DIV')
      GROUP BY A.DIV_CD
              ,A.KPI_CD
              ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END

      UNION ALL

-- 4.제품별
      SELECT A.DIV_CD            AS COL_INDEX
            ,A.KPI_CD            AS KPI_CD
            ,CASE WHEN B.SEQ = 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN B.SEQ = 2 THEN A.BASE_YYYYMM END        AS BASE_YYYYMM
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,0 AS DISP_SEQ
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 AS SEQ FROM SYSIBM.SYSDUMMY1) B
            ,(SELECT DIVISION_CODE AS DIV_CD
                    ,DISPLAY_NAME1 AS KOR_NM
                    ,DISPLAY_NAME2 AS ENG_NM
                    ,DISPLAY_ORDER_SEQ AS DISP_SEQ
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
              WHERE  CODE_TYPE = 'PIPELINE'
              AND    DIVISION_CODE IN ('GNTCS','GNTHT')
              AND    USE_FLAG = 'Y' ) Z
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD    = Z.DIV_CD
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.KPI_CD IN ('SALE')
      AND    A.CAT_CD IN ('BEP_SMART_GNT')
      GROUP BY A.DIV_CD
              ,A.KPI_CD
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