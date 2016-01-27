CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_MGN_PROFIT (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD    VARCHAR(8),
     IN P_DIV_CD       VARCHAR(8)
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_MGN_PROFIT                                                     */
  /* 4.설       명 : 유첨. 제품별 경영 실적 - DIVISION                                            */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인 )                                                       */
  /*                 IN P_DIV_CD( DIVISION CODE )                                                 */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.20  최초 작성                                                      */
  /*  1.1      shlee   2016.01.27  BEP 추가 - 한글/영문 추가작업필요없음.                         */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_MGN_PROFIT';
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

-- 1.매출/영업이익
      SELECT CASE A.KPI_CD
                  WHEN 'SALE' THEN '매출'
                  WHEN 'COI'  THEN '영업이익' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE A.KPI_CD
                    WHEN 'SALE' THEN '매출'
                    WHEN 'COI'  THEN '영업이익' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL
      
-- 2. 저수익 모델매출 / 저수익구간
      SELECT CASE C.SEQ
                  WHEN 1 THEN '저수익모델매출'
                  WHEN 2 THEN '저수익구간'||D.SUB_CAT_NM END AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) C
            ,(
              SELECT 'MARGINAL_PF_(-)'  AS SUB_CAT_CD, '0%미만' AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_0_5'  AS SUB_CAT_CD, '0%~5%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_5_10' AS SUB_CAT_CD, '5%~10%' AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE C.SEQ
                  WHEN 1 THEN '저수익모델매출'
                  WHEN 2 THEN '저수익구간'||D.SUB_CAT_NM END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL
      
-- 3. 저수익 모델수 / 저수익구간모델수
      SELECT CASE C.SEQ
                  WHEN 1 THEN '저수익모델수'
                  WHEN 2 THEN '저수익구간모델수'||D.SUB_CAT_NM END AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) C
            ,(
              SELECT 'MARGINAL_PF_(-)'  AS SUB_CAT_CD, '0%미만' AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_0_5'  AS SUB_CAT_CD, '0%~5%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_5_10' AS SUB_CAT_CD, '5%~10%' AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MODEL_COUNT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE C.SEQ
                  WHEN 1 THEN '저수익모델수'
                  WHEN 2 THEN '저수익구간모델수'||D.SUB_CAT_NM END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 4. 구간대별매출
      SELECT '구간대별'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_10_20'  AS SUB_CAT_CD, '10%~20%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_20_30'  AS SUB_CAT_CD, '20%~30%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_30'     AS SUB_CAT_CD, '30%이상'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY '구간대별'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 5. 구간대별모델수
      SELECT '구간대별모델수'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_10_20'  AS SUB_CAT_CD, '10%~20%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_20_30'  AS SUB_CAT_CD, '20%~30%'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'MARGINAL_PF_30'     AS SUB_CAT_CD, '30%이상'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MODEL_COUNT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY '구간대별모델수'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 6. 한계적자금액
      SELECT '한계'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자금액'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY'한계'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 6-1. 한계적자매출
      SELECT '한계'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자매출'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY'한계'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 7. 적자모델수
      SELECT D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자모델수'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MODEL_COUNT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 8. 한계이익
      SELECT '한계이익'  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUB_CAT_CD LIKE 'COI%'
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL
-- 9. BEP매출
      SELECT 'BEP매출'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('BEP_F_COST')
      AND    A.MANUAL_ADJ_FLAG = 'N'
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL
-- 10. 유효모델수
      SELECT '유효모델수'  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MODEL_COUNT')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
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