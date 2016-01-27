CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_ACTUAL_DIV (
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_ACTUAL_DIV                                                     */
  /* 4.설       명 : 유첨. 제품별 경영 실적                                                       */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인 )                                                       */
  /*                 IN P_DIV_CD( DIVISION CODE )                                                 */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.26  최초 작성                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_ACTUAL_DIV';
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


-- 1.매출/영업이익/가격성판촉비
      SELECT A.DIV_CD AS DIV_CD
            ,'ALL' AS PROD_CD
            ,CASE A.KPI_CD
                  WHEN 'SALE' THEN '매출'
                  WHEN 'COI'  THEN '영업이익'
                  WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
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
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI','SALES_DEDUCTION')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.DIV_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,CASE A.KPI_CD
                    WHEN 'SALE' THEN '매출'
                    WHEN 'COI'  THEN '영업이익'
                    WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 2. 전년대비
      SELECT A.DIV_CD AS DIV_CD
            ,'ALL' AS PROD_CD
            ,'전년대비'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
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
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),1,4)||'12'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.DIV_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END
              ,A.KPI_CD

      UNION ALL

-- 3. 광고선전비
      SELECT A.DIV_CD AS DIV_CD
            ,'ALL' AS PROD_CD
            ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
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
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_OH'
      AND    A.KPI_CD in ('OH101000')
      AND    A.SUB_CAT_CD = 'FC'
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.DIV_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 4. 한계적자금액/한계이익
      SELECT A.DIV_CD AS DIV_CD
            ,'ALL' AS PROD_CD
            ,CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자금액' END AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY           
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-10_-5'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15_-10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-5_0'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_0_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1
             ) D
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
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.DIV_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자금액' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 5. 한계적자매출
      SELECT A.DIV_CD AS DIV_CD
            ,'ALL' AS PROD_CD
            ,'한계'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자매출'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
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
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.DIV_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,'한계'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 요기서부터 제품별
-- 6.매출/영업이익/가격성판촉비
      SELECT A.DIV_CD AS DIV_CD
            ,A.SUB_CAT_CD AS PROD_CD
            ,CASE A.KPI_CD
                  WHEN 'SALE' THEN '매출'
                  WHEN 'COI'  THEN '영업이익'
                  WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD'
      AND    A.KPI_CD in ('SALE', 'COI','SALES_DEDUCTION')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,CASE A.KPI_CD
                    WHEN 'SALE' THEN '매출'
                    WHEN 'COI'  THEN '영업이익'
                    WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 7. 전년대비
      SELECT A.DIV_CD AS DIV_CD
            ,A.SUB_CAT_CD AS DIV_CD
            ,'전년대비'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),1,4)||'12'
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END
              ,A.KPI_CD

      UNION ALL

-- 8. 광고선전비
      SELECT A.DIV_CD AS DIV_CD
            ,A.SUB_CAT_CD AS DIV_CD
            ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_OH'
      AND    A.KPI_CD in ('OH101000')
      AND    A.SUB_CAT_CD = 'FC'
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 9. 한계이익
      SELECT A.DIV_CD AS DIV_CD
            ,A.SUB_CAT_CD AS DIV_CD
            , '한계이익' AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY           
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,'한계이익'
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 10. 한계적자금액
      SELECT A.DIV_CD
            ,A.SUB_CAT_CD
            ,'한계'||D.SUB_CAT_NM  AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
            ,MIN(Z.KOR_NM) AS KOR_NM
            ,MIN(Z.ENG_NM) AS ENG_NM
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT 'MARGINAL_PF_(-)'     AS SUB_CAT_CD, '적자금액'  AS SUB_CAT_NM FROM SYSIBM.SYSDUMMY1 
             ) D
            ,(
              -- 제품
              SELECT 'PROD' AS CODE_TYPE
                    ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                    ,A.ATTRIBUTE1          AS CODE
                    ,A.DISPLAY_NAME1       AS KOR_NM
                    ,A.DISPLAY_NAME2       AS ENG_NM
                    ,A.ATTRIBUTE1          AS REF_CD
              FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
              WHERE  A.CODE_TYPE = 'SMR_PROD_MST'
              AND    A.USE_FLAG  = 'Y'
             ) Z
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_PROD_MMGN'
--      AND    A.KPI_CD in ('SALE')
      AND    A.KPI_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      AND    A.SUB_CAT_CD = Z.CODE
      GROUP BY A.DIV_CD
              ,A.SUB_CAT_CD
              ,'한계'||D.SUB_CAT_NM 
              ,A.SUBSDR_CD
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