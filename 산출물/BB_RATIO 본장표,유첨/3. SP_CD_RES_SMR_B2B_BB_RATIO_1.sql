CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_B2B_BBR_1 (
     IN P_BASIS_YYYYMMDD VARCHAR(8),
     IN P_SUBSDR_CD      VARCHAR(8)
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_B2B_BBR_1                                                   */
  /* 4.설       명 : SMART 주차별Trend(BB RATIO)을 Result Set으로 return함                        */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMMDD( 기준일 )                                                */
  /*                 IN P_SUBSDR_CD( 법인코드 )                                                   */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.14  최초 작성                                                      */
  /*  1.1      shlee   2016.01.15  IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER로 정리                 */
  /*  1.2      shlee   2016.01.19  SUBSDR_CD 구분추가                                             */
  /*  1.3      shlee   2016.01.22  CURRM_USD_AMT 로 변경                                          */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_B2B_BBR_1';
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

    -- 전체1. W5 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 36 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체1. W5 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 43 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체1. W5 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 50 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체1. W5 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 57 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체1. W5 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W05' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 64 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    UNION ALL
    -- 전체2. W13 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 92 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체2. W13 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 99 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체2. W13 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 106 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체2. W13 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 113 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체2. W13 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W13' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 120 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    UNION ALL
    -- 전체3. W52 가장최근
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME < P_BASIS_YYYYMMDD
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 365 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체3. W52 가장최근 - 1
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 7 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 372 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체3. W52 가장최근 - 2
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 14 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 379 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체3. W52 가장최근 - 3
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 21 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 386 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW
    -- 전체3. W52 가장최근 - 4
    UNION ALL
    SELECT MAX(C.BASE_YYYYWW) AS BASE_YYYYWW
          ,'ALL' AS LEV1
          ,'ALL' AS LEV2
          ,'법인전체' AS KOR_NM
          ,'TOTAL'    AS ENG_NM
          ,'W52' AS KPI_CD
          ,CASE COALESCE(SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END),0)
                WHEN 0 THEN 0
                ELSE SUM(CASE A.KPI_CD WHEN 'AWARD' THEN A.CURRM_USD_AMT END) / SUM(CASE A.KPI_CD WHEN 'SALES' THEN A.CURRM_USD_AMT END) END  AS BB_RATIO
    FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
          ,IPTDW.IPTDW_RES_DIM_MST_DIV_PROD_MAPPING B     
          ,(SELECT DISTINCT A.CODE_ID AS BASE_YYYYWW
            FROM   IPTDW.IPTDW_RES_DIM_CODES A
                  ,IPTDW.IPTDW_RES_DIM_CODES B
            WHERE  A.CODE_TYPE = 'SMART_WEEK'
            AND    A.CODE_TYPE = B.CODE_TYPE
            AND    A.CODE_NAME <  TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 28 DAY, 'YYYYMMDD')
            AND    B.CODE_NAME >= TO_CHAR(to_date(P_BASIS_YYYYMMDD, 'YYYYMMDD') - 393 DAY, 'YYYYMMDD')
            AND    A.CODE_ID   >= B.CODE_ID ) C
    WHERE  A.DIV_CD = B.DIVISION_CODE
    AND    A.CAT_CD = 'BEP_SMART_BB'
    AND    A.SUBSDR_CD = P_SUBSDR_CD
    AND    A.APPLY_YYYYMM = C.BASE_YYYYWW

    WITH UR;
    
    OPEN C1;
   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_SUBSDR_CNTRY';
    SET v_basis_yyyymmdd             = P_BASIS_YYYYMMDD;
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