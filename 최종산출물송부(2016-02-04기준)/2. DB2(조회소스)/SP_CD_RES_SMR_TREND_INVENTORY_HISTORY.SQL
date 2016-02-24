CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_INVENTORY_HISTORY ( 
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD   VARCHAR(8),
     IN P_B2B_B2C      VARCHAR(3),
     IN P_DIV_YYYYMM VARCHAR(6)
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
  /*************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                  */
  /* 2.모       듈 :                                                       */
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_INVENTORY_HISTORY                 */
  /*                                                                       */
  /* 4.설       명 : SMART 재고 추이를 Result Set으로 return함             */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                                                                       */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                           */
  /*                 IN P_SUBSDR_CD ( 법인 )                               */
  /*                 IN P_B2B_B2C( B2B/B2C )                               */
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      KIM.S.K 2016.01.04  최초 작성                               */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_INVENTORY_HISTORY';
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

    SELECT Z.COL_INDEX
          ,Z.DIV_CD
          ,P_B2B_B2C                  AS B2B_B2C
          ,SUBSTR(Z.BASIS_YYYYMM,1,4) AS BASIS_YYYY
          ,Z.BASIS_YYYYMM
          ,Z.KPI_CD
          ,SUM(Z.AMOUNT)              AS AMOUNT
          ,MAX(Z.DIV_NAME_KO)         AS DIV_NAME_KO
          ,MAX(Z.DIV_NAME_EN)         AS DIV_NAME_EN
    FROM   (
            -- 1.전체_매출
            SELECT  '1.전체_매출'                  AS COL_INDEX
                   ,'1.전체_매출'                  AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,''                             AS DIV_NAME_KO
                   ,''                             AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                  /*INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)*/
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'BEP_SMART_DIV'
            AND     A.KPI_CD           IN ('SALE')
            AND     A.DIV_CD            = 'GBU'
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 2.전체_재고일수
            SELECT  '2.전체_재고일수'              AS COL_INDEX
                   ,'2.전체_재고일수'              AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,''                             AS DIV_NAME_KO
                   ,''                             AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
          --AND     A.DIV_CD            = (CASE WHEN P_B2B_B2C IN ('B2B','B2C') THEN P_B2B_B2C ELSE 'GBU' END)
            AND     A.DIV_CD            = 'GBU'
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV_DIO', 'INV_DIO_R', 'INV_DIO_MTL')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 3.전체_재고금액
            SELECT  '3.전체_재고금액'              AS COL_INDEX
                   ,'3.전체_재고금액'              AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,''                             AS DIV_NAME_KO
                   ,''                             AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                  /*INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)*/
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV', 'INV_WH', 'INV_SEA')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 4.전체_장기재고금액
            SELECT  '4.전체_장기재고금액'          AS COL_INDEX
                   ,'4.전체_장기재고금액'          AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,''                             AS DIV_NAME_KO
                   ,''                             AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                  /*INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)*/
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV_LONG_TERM', 'INV_LONG_TERM_MTL','INV_LONG_TERM_AGING', 'INV_LONG_TERM_3_6', 'INV_LONG_TERM_7_12', 'INV_LONG_TERM_13_Over', 
                                           'INV_LONG_TERM_MTL_3_6', 'INV_LONG_TERM_MTL_7_12', 'INV_LONG_TERM_MTL_13_Over')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 5.재고과다/재고부족_재고일수
            SELECT  '5.재고과다/재고부족_재고일수' AS COL_INDEX
                   ,A.DIV_CD                       AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,MAX(C2.KOR_NM)                 AS DIV_NAME_KO
                   ,MAX(C2.ENG_NM)                 AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                    INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)
                    LEFT OUTER JOIN ( -- 사업부
                                      SELECT 'DIV' AS CODE_TYPE
                                            ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                            ,A.DIVISION_CODE     AS CODE
                                            ,A.DISPLAY_NAME      AS KOR_NM
                                            ,A.DIVISION_NAME     AS ENG_NM
                                            ,A.COMPANY_CODE      AS REF_CD
                                      FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                      WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                      AND    A.USE_FLAG = 'Y'
                                    ) C2 ON (A.DIV_CD = C2.CODE)
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV_DIO')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 6.재고과다/재고부족_재고금액
            SELECT  '6.재고과다/재고부족_재고금액' AS COL_INDEX
                   ,A.DIV_CD                       AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,MAX(C2.KOR_NM)                 AS DIV_NAME_KO
                   ,MAX(C2.ENG_NM)                 AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                    INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)
                    LEFT OUTER JOIN ( -- 사업부
                                      SELECT 'DIV' AS CODE_TYPE
                                            ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                            ,A.DIVISION_CODE     AS CODE
                                            ,A.DISPLAY_NAME      AS KOR_NM
                                            ,A.DIVISION_NAME     AS ENG_NM
                                            ,A.COMPANY_CODE      AS REF_CD
                                      FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                      WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                      AND    A.USE_FLAG = 'Y'
                                    ) C2 ON (A.DIV_CD = C2.CODE)
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 7.장기재고율_장기재고금액
            SELECT  '7.장기재고율_장기재고금액'    AS COL_INDEX
                   ,A.DIV_CD                       AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,MAX(C2.KOR_NM)                 AS DIV_NAME_KO
                   ,MAX(C2.ENG_NM)                 AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                    INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)
                    LEFT OUTER JOIN ( -- 사업부
                                      SELECT 'DIV' AS CODE_TYPE
                                            ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                            ,A.DIVISION_CODE     AS CODE
                                            ,A.DISPLAY_NAME      AS KOR_NM
                                            ,A.DIVISION_NAME     AS ENG_NM
                                            ,A.COMPANY_CODE      AS REF_CD
                                      FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                      WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                      AND    A.USE_FLAG = 'Y'
                                    ) C2 ON (A.DIV_CD = C2.CODE)
            WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'SMART_INV'
            AND     A.KPI_CD           IN ('INV_LONG_TERM')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
            UNION ALL
            -- 7.장기재고율_매출
            SELECT  '7.장기재고율_매출'            AS COL_INDEX
                   ,A.DIV_CD                       AS DIV_CD
                   ,A.BASE_YYYYMM                  AS BASIS_YYYYMM
                   ,A.KPI_CD                       AS KPI_CD
                   ,SUM(A.CURRM_USD_AMT)           AS AMOUNT
                   ,MAX(C2.KOR_NM)                 AS DIV_NAME_KO
                   ,MAX(C2.ENG_NM)                 AS DIV_NAME_EN
            FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                    INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                            ON (B.CODE_TYPE = (CASE WHEN P_B2B_B2C = 'B2B' THEN 'B2B_DIV' WHEN P_B2B_B2C = 'B2C' THEN 'B2C_DIV' ELSE B.CODE_TYPE END)
                           AND  A.DIV_CD = B.CODE_ID)
                    LEFT OUTER JOIN ( -- 사업부
                                      SELECT 'DIV' AS CODE_TYPE
                                            ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                            ,A.DIVISION_CODE     AS CODE
                                            ,A.DISPLAY_NAME      AS KOR_NM
                                            ,A.DIVISION_NAME     AS ENG_NM
                                            ,A.COMPANY_CODE      AS REF_CD
                                      FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                      WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                      AND    A.USE_FLAG = 'Y'
                                    ) C2 ON (A.DIV_CD = C2.CODE)
            WHERE   A.BASE_YYYYMM       = TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
            AND     A.SUBSDR_CD         = P_SUBSDR_CD
            AND     A.SCENARIO_TYPE_CD  = 'AC0'
            AND     A.CAT_CD            = 'BEP_SMART_DIV'
            AND     A.KPI_CD           IN ('SALE')
            GROUP BY A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
           ) Z
    GROUP BY Z.COL_INDEX, Z.DIV_CD, SUBSTR(Z.BASIS_YYYYMM,1,4), Z.BASIS_YYYYMM, Z.KPI_CD
    ORDER BY Z.COL_INDEX, Z.DIV_CD, SUBSTR(Z.BASIS_YYYYMM,1,4), Z.BASIS_YYYYMM, Z.KPI_CD
    WITH UR ;    
      
    
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