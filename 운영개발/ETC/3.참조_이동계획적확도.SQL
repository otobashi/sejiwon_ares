CREATE OR REPLACE PROCEDURE SP_CD_RES_KPI_SUMM_DIV_MP_HIS
(
  IN P_BASIS_YYYYMM  VARCHAR(6)
)
LANGUAGE SQL
DYNAMIC RESULT SETS 1
BEGIN
  /************************************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                                         */
  /* 2.모       듈 :                                                                              */
  /* 3.프로그램 ID : SP_CD_RES_KPI_SUMM_DIV_MP_HIS                                                */
  /* 4.설       명 : 참조. 이동계획 적확도                                                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.02.02  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_SUMM_DIV_MP_HIS';
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

    SELECT  A.DISP_SEQ
           ,A.DIV_CD
           ,C.DISP_NM
           ,A.BASE_YYYYMM
           ,A.KPI_CD
           ,A.CURR_KRW_AMT
           ,A.CURR_USD_AMT
           ,A.ACCU_KRW_AMT
           ,A.ACCU_USD_AMT
    FROM   (
            SELECT C.DISPLAY_ORDER AS DISP_SEQ
                  ,DECODE (A.DIV_CD,'ALL','GBU',A.DIV_CD) AS DIV_CD
                  ,CASE A.SCENARIO_TYPE_CD||A.BASE_YYYYMM
                        WHEN 'AC0'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 2 month, 'YYYYMM') THEN '전전월'
                        WHEN 'PR1'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 3 month, 'YYYYMM') THEN '전전월M-1월'
                        WHEN 'PR2'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 4 month, 'YYYYMM') THEN '전전월M-2월'
                        WHEN 'PR3'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 5 month, 'YYYYMM') THEN '전전월M-3월'
                        WHEN 'AC0'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 1 month, 'YYYYMM') THEN '전월'
                        WHEN 'PR1'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 2 month, 'YYYYMM') THEN '전월M-1월'
                        WHEN 'PR2'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 3 month, 'YYYYMM') THEN '전월M-2월'
                        WHEN 'PR3'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 4 month, 'YYYYMM') THEN '전월M-3월'
                        WHEN 'AC0'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 0 month, 'YYYYMM') THEN '당월'
                        WHEN 'PR1'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 1 month, 'YYYYMM') THEN '당월M-1월'
                        WHEN 'PR2'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 2 month, 'YYYYMM') THEN '당월M-2월'
                        WHEN 'PR3'||to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 3 month, 'YYYYMM') THEN '당월M-3월' END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN '41000000' THEN '매출'
                        WHEN '549999PL' THEN '영업이익' END AS KPI_CD
                  ,SUM(A.CURRM_KRW_AMT) AS CURR_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT) AS CURR_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT) AS ACCU_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT) AS ACCU_USD_AMT
            FROM  IPTDW.IPTDW_RES_KPI_DIV_S A
                 ,IPTDW.IPTDW_RES_DISPLAY_MASTER C
            WHERE C.TYPE_CODE = 'RPT_DIV_PR_ALL'
            AND   C.DISPLAY_CODE = DECODE (A.DIV_CD,'ALL','GBU',A.DIV_CD)
            AND   A.BASE_YYYYMM BETWEEN to_char(to_date(P_BASIS_YYYYMM, 'YYYYMM')- 5 month, 'YYYYMM') AND P_BASIS_YYYYMM
            AND   A.SCENARIO_TYPE_CD IN ('AC0','PR1','PR2','PR3')
            AND   A.SUBSDR_CD = 'ALL'
            AND   A.ACCT_CD IN ( '41000000', '549999PL' )
            GROUP BY C.DISPLAY_ORDER
                    ,DECODE (A.DIV_CD,'ALL','GBU',A.DIV_CD)
                    ,A.SCENARIO_TYPE_CD||A.BASE_YYYYMM
                    ,CASE A.ACCT_CD
                        WHEN '41000000' THEN '매출'
                        WHEN '549999PL' THEN '영업이익' END
          ) A
        ,(SELECT C.DISPLAY_NAME AS DISP_NM
                ,C.DISPLAY_CODE AS DISP_CD
          FROM   IPTDW.IPTDW_RES_DISPLAY_MASTER C
          WHERE  C.TYPE_CODE = 'RPT_DIV_PR_ALL') C
    WHERE  A.BASE_YYYYMM IS NOT NULL
    AND    C.DISP_CD = DECODE (A.DIV_CD,'ALL','GBU',A.DIV_CD)
    ORDER BY A.DISP_SEQ
    
    WITH UR;

    OPEN C1;

   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_DIV_S';
    SET v_basis_yyyymmdd             = P_BASIS_YYYYMM;
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