CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_SALE_COI_HISTORY (
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD    VARCHAR(8),
     IN P_DIV_YYYYMM VARCHAR(6)
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
  /*************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                  */
  /* 2.모       듈 :                                                       */
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_SALE_COI_HISTORY                  */
  /*                                                                       */
  /* 4.설       명 : SMART 매출 추이를 Result Set으로 return함             */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                                                                       */
  /*                 IN P_BASIS_YYYYMM ( 기준월 )                          */
  /*                 IN P_SUBSDR_CD ( 법인 )                               */
  /*                 IN P_DIVISION ( 사업부 )                              */
  /*                 IN P_CURRENCY ( 통화 )                                */
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      mysik   2015.12.07  최초 작성                               */
  /*  1.1      KIM.S.K 2016.01.18  - 당월실적(OLED 추가)                   */
  /*                               - Most likely 로직추가                  */
  /*  1.2      KIM.S.K 2016.01.22  - 전월이동,전전월이동 로직추가          */
  /*  1.3      S.H.LEE 2016.02.16  - ML처리 변경                           */
  /*************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_SALE_COI_HISTORY';
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
          ,Z.SUBSDR_CD
          ,MAX(Z.SUBSDR_SHRT_NAME) AS SUBSDR_SHRT_NAME
          ,Z.DIV_CD
          ,Z.BASIS_YYYYMM
          ,Z.KPI_CD
          ,SUM(Z.AMOUNT)           AS AMOUNT
          ,MAX(Z.DIV_NAME_KO)      AS DIV_NAME_KO
          ,MAX(Z.DIV_NAME_EN)      AS DIV_NAME_EN
          ,SUBSTR(Z.BASIS_YYYYMM,1, 4) AS YYYY
    FROM (

          SELECT  '1.전년실적'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
                   LEFT OUTER JOIN ( -- 사업부
                                     SELECT 'DIV' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.DIVISION_CODE     AS CODE
                                           ,A.DISPLAY_NAME      AS KOR_NM
                                           ,A.DIVISION_NAME     AS ENG_NM
                                           ,A.COMPANY_CODE      AS REF_CD
                                     FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                     WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                     --AND    A.USE_FLAG = 'Y'
                                   ) C2 ON (A.DIV_CD = C2.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '2.당월실적'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
                   LEFT OUTER JOIN ( -- 사업부
                                     SELECT 'DIV' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.DIVISION_CODE     AS CODE
                                           ,A.DISPLAY_NAME      AS KOR_NM
                                           ,A.DIVISION_NAME     AS ENG_NM
                                           ,A.COMPANY_CODE      AS REF_CD
                                     FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                     WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                     --AND    A.USE_FLAG = 'Y'
                                   ) C2 ON (A.DIV_CD = C2.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           -- 2.당월실적(OLED 추가)
           SELECT  '2.당월실적'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'OLED'               AS DIV_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'(OLED)'             AS DIV_NAME_KO
                  ,'(OLED)'             AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_PROD'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ATTRIBUTE3_VALUE  = 'GLT_L2_1'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD


           UNION ALL
           -- 2.1.전년누계실적
           SELECT  '2.1.전년누계실적'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,P_BASIS_YYYYMM       AS BASE_YYYYMM -- A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
                   LEFT OUTER JOIN ( -- 사업부
                                     SELECT 'DIV' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.DIVISION_CODE     AS CODE
                                           ,A.DISPLAY_NAME      AS KOR_NM
                                           ,A.DIVISION_NAME     AS ENG_NM
                                           ,A.COMPANY_CODE      AS REF_CD
                                     FROM   IPTDW.IPTDW_RES_DIM_COMM_DIVISION A
                                     WHERE  A.BASIS_YYYYMM = P_DIV_YYYYMM
                                     --AND    A.USE_FLAG = 'Y'
                                   ) C2 ON (A.DIV_CD = C2.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 1 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 year, 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           -- 2.1.전년누계실적
           SELECT  '2.1.전년누계실적'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'OLED'               AS DIV_CD
                  ,P_BASIS_YYYYMM       AS BASE_YYYYMM -- A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'(OLED)'             AS DIV_NAME_KO
                  ,'(OLED)'             AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 1 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 year, 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_PROD'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ATTRIBUTE3_VALUE  = 'GLT_L2_1'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '3.전월이동'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)

           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR1'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD,  A.APPLY_YYYYMM, KPI_CD
           UNION ALL
           SELECT  '3.전전월이동'       AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR2'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           AND     A.DIV_CD            = 'GBU'
           GROUP BY SUBSDR_CD,  A.APPLY_YYYYMM, KPI_CD
           UNION ALL
           SELECT  '4.월말매출집중도' AS COL_INDEX
                  ,A.SUBSDR_CD        AS SUBSDR_CD
                  ,C1.CODE            AS SUBSDR_SHRT_NAME
                  ,'GBU'              AS DIV_CD
                  ,A.BASIS_YYYYMM     AS BASE_YYYYMM
                  ,A.KPI_TYPE_CD      AS KPI_CD
                  ,A.ACT_RATE         AS AMOUNT
                  ,''                 AS DIV_NAME_KO
                  ,''                 AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_WEEKLY_IF A
                   LEFT OUTER JOIN IPTDW.IPTDW_RES_DIM_CORPORATION_MAPPING B
                                ON (B.CORPORATION_CODE = CASE WHEN A.SUBSDR_CD = 'ENUS_ALL' THEN 'ENUS' ELSE A.SUBSDR_CD END)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASIS_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM
           AND     A.KPI_TYPE_CD        = '2_SC7D' -- IN ('1_SAAP','2_SC7D','5_MRGN','1_SAAP_YR','2_SC7D_YR') -- '3_BBRT, 4_BLSN'  DIV에서만 사용함.
           AND     A.SUBSDR_CD         <> 'ENUS'
           /* B2C */
           UNION ALL
           SELECT  '4.B2C_당월실적'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL        AS DIV_NAME_KO
                  ,NULL        AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND  A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)

           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '5.B2C_전월이동'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  --,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH , 'YYYYMM') AS BASE_YYYYMM
                  ,A.APPLY_YYYYMM       AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL        AS DIV_NAME_KO
                  ,NULL        AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)

           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR1' -- 'MP'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           --GROUP BY A.SUBSDR_CD, TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH , 'YYYYMM'), A.KPI_CD
           GROUP BY A.SUBSDR_CD, A.APPLY_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '5.B2C_전전월이동'   AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  --,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH , 'YYYYMM') AS BASE_YYYYMM
                  ,A.APPLY_YYYYMM       AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR2' -- 'MP'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           --GROUP BY A.SUBSDR_CD, A.DIV_CD, TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH , 'YYYYMM'), A.KPI_CD
           GROUP BY A.SUBSDR_CD, A.APPLY_YYYYMM, A.KPI_CD
           /* B2C */
           UNION ALL
           SELECT  '4.B2B_당월실적'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS KPI_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '5.B2B_전월이동'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                      ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)

           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR1'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.APPLY_YYYYMM, A.KPI_CD
           UNION ALL
           SELECT  '5.B2B_전전월이동'   AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM       AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,NULL       AS DIV_NAME_KO
                  ,NULL       AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                      ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)

           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH , 'YYYYMM')
           AND     A.SCENARIO_TYPE_CD  = 'PR2'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD,  A.APPLY_YYYYMM, A.KPI_CD
           UNION ALL
/*
           -- GBU 합산
           SELECT  'Most Likely W'||A.ZONE_RNR_CD  AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM       AS BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
                   LEFT OUTER JOIN ( -- 법인코드/명 가져오기 전체공통
                                     SELECT 'CORP' AS CODE_TYPE
                                           ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                           ,A.ATTRIBUTE1        AS CODE
                                           ,A.DISPLAY_NAME1     AS KOR_NM
                                           ,A.DISPLAY_NAME2     AS ENG_NM
                                           ,A.CORPORATION_CODE  AS REF_CD
                                     FROM IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                     WHERE CODE_TYPE = 'SMART_SUBSDR_DISP'
                                     AND   A.ATTRIBUTE2  IS NULL
                                   ) C1 ON (A.SUBSDR_CD = C1.CODE)
           WHERE  A.BASE_YYYYMM       = TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 3 MONTH, 'YYYYMM')
           AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3')
           AND    A.CAT_CD            = 'BEP_SMART_ML'
           AND    A.KPI_CD           IN ('SALE', 'COI')
           GROUP BY A.ZONE_RNR_CD, A.SUBSDR_CD, A.APPLY_YYYYMM, A.KPI_CD
*/
/*
           -- GBU 합산 -- ML처리 변경 SHLEE 2016.02.16
           SELECT  'Most Likely W'||A.ZONE_RNR_CD  AS COL_INDEX
                  ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
           WHERE  A.BASE_YYYYMM       BETWEEN TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 3 MONTH, 'YYYYMM') AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
           AND    A.SCENARIO_TYPE_CD IN ('PR1')
           AND    A.CAT_CD            = 'BEP_SMART_ML'
           AND    A.KPI_CD           IN ( 'SALE','COI')
--           AND    A.SUBSDR_CD = 'EHAP'
           GROUP BY A.ZONE_RNR_CD
                   ,A.SUBSDR_CD
                   ,A.APPLY_YYYYMM
                   ,A.KPI_CD
*/
-- 2016.02.21
-- 7.Most Likely
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
      UNION ALL
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
      UNION ALL
      SELECT 'Most Likely W'||C.SEQ         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
                  ,A.SUBSDR_CD         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
            ,A.APPLY_YYYYMM       AS APPLY_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
--            ,C.THU                AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
           ,(SELECT CODE_ID DIV_CD
                   ,CODE_NAME
                   ,ATTRIBUTE1 KOR_NM
                   ,ATTRIBUTE2 ENG_NM
             FROM   IPTDW.IPTDW_RES_DIM_CODES
             WHERE  CODE_TYPE = 'B2C_DIV'
            ) B
           ,(SELECT A.WEEK_NO
                   ,A.BASE_YYYYMM
                   ,A.THU
                   ,ROWNUMBER() OVER() AS SEQ
                   ,A.START_YMD
             FROM   (SELECT A.ATTRIBUTE1 AS WEEK_NO
                           ,A.ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(A.DESCRIPTION,5,2)||'/'||SUBSTR(A.DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                           ,A.ATTRIBUTE3 AS START_YMD
                     FROM   IPTDW.IPTDW_RES_DIM_CODES A
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                    ) A
                   ,(SELECT MAX(SEQ) AS FROM_SEQ, MAX(SEQ) - 3 AS END_SEQ
                     FROM  (
                     SELECT ATTRIBUTE1 AS WEEK_NO
                           ,ATTRIBUTE2 AS BASE_YYYYMM
                           ,SUBSTR(DESCRIPTION,5,2)||'/'||SUBSTR(DESCRIPTION,7,2) AS THU
                           ,ROWNUMBER() OVER() AS SEQ
                     FROM   IPTDW.IPTDW_RES_DIM_CODES
                     WHERE  CODE_TYPE = 'SMART_WEEK'
                     AND    TO_CHAR(to_date(TO_CHAR(to_date(P_BASIS_YYYYMM, 'YYYYMM') - 0 MONTH, 'YYYYMM')||'30', 'YYYYMMDD') - 1 day, 'YYYYMMDD') >= ATTRIBUTE3
                     )
                    ) B
             WHERE A.SEQ >= B.END_SEQ
            ) C
           ,(SELECT ATTRIBUTE1 AS SUBSDR_CD
                   ,ATTRIBUTE4 AS AU_CD
             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
             WHERE  CODE_TYPE = 'SMART_SUBSDR_DISP') AU
      WHERE  A.BASE_YYYYMM = C.BASE_YYYYMM
      AND    A.SCENARIO_TYPE_CD IN ('PR0')
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE', 'COI')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.SUBSDR_CD = AU.SUBSDR_CD
      AND    A.AU_CD     = AU.AU_CD
      AND    A.DIV_CD = B.DIV_CD
      AND    A.ATTRIBUTE1_VALUE = C.START_YMD
--      AND    A.ZONE_RNR_CD = C.WEEK_NO
      GROUP BY C.SEQ
              ,A.SUBSDR_CD
              ,A.APPLY_YYYYMM
              ,A.KPI_CD
              ,C.THU
         ) Z
    WHERE Z.SUBSDR_CD = P_SUBSDR_CD
    GROUP BY Z.COL_INDEX,
             Z.SUBSDR_CD,
             Z.DIV_CD,
             Z.BASIS_YYYYMM,
             Z.KPI_CD,
             SUBSTR(Z.BASIS_YYYYMM,1, 4)
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