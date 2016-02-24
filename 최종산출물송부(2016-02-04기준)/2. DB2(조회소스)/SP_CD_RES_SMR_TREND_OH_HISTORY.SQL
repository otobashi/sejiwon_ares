CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_OH_HISTORY ( 
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD VARCHAR(8),
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_OH_HISTORY                        */
  /*                                                                       */
  /* 4.설       명 : SMART Overhead 추이를 Result Set으로 return함         */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                                                                       */
  /*                 IN p_basis_yyyymm( 기준월 )                           */
  /*                 IN p_division( 사업부 )                               */
  /*                 IN p_currency( 통화 )                                 */
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      mysik   2015.12.18  최초 작성                               */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_OH_HISTORY';
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
          ,MAX(Z.SUBSDR_SHRT_NAME)    AS SUBSDR_SHRT_NAME
          ,Z.DIV_CD
          ,SUBSTR(Z.BASIS_YYYYMM,1,4) AS BASIS_YYYY
          ,Z.BASIS_YYYYMM
          ,Z.KPI_CD
          ,SUM(Z.AMOUNT)              AS AMOUNT  
          ,MAX(Z.DIV_NAME_KO)         AS DIV_NAME_KO
          ,MAX(Z.DIV_NAME_EN)         AS DIV_NAME_EN
    FROM (
           /* 법인전체*/         
           SELECT  '1.전년실적_법인전체_SALE' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE')
           AND     A.ZONE_RNR_CD <> 'ZZZ'
           AND     A.DIV_CD = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* 법인전체_OH*/         
           SELECT  '2.전년실적_법인전체_OH' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD            AS KPI_CD -- 'OH_COST'
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
                                     AND    A.USE_FLAG = 'Y'
                                   ) C2 ON (A.DIV_CD = C2.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST' -- OH_COST
           --AND     A.DIV_CD = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           SELECT  '1.당월실적_법인전체_SALE' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,''                   AS DIV_NAME_KO
                  ,''                   AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE')
           AND     A.ZONE_RNR_CD <> 'ZZZ'
           AND     A.DIV_CD = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL
           /* 법인전체_OH*/         
           SELECT  '2.당월실적_법인전체_OH' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
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
                                     AND    A.USE_FLAG = 'Y'
                                   ) C2 ON (A.DIV_CD = C2.CODE)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST' -- OH_COST
           --AND     A.DIV_CD = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           /**************/   
           /* B2C (전년) */
           /**************/
           /* B2C 실적 - 매출/ 가격성 판촉*/   
           SELECT  '1.전년실적_B2C' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD   
           UNION ALL
           /* B2C 실적 - OH_COST */   
           SELECT  '1.전년실적_B2C' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD            AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           /* B2C 실적 - OH변동비 */   
           SELECT  '1.전년실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD          AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_V_COST'
           --AND     A.SUB_CAT_CD = 'VC'
           --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           /* B2C 실적 - OH고정비 */   
           SELECT  '1.전년실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD          AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_F_COST'
           --AND     A.SUB_CAT_CD = 'FC'
           --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.KPI_CD, A.BASE_YYYYMM
           UNION ALL

           /**************/   
           /* B2C (당월) */
           /**************/
           /* B2C 실적 - 매출/ 가격성 판촉*/   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD   
           UNION ALL
           /* B2C 실적 - OH_COST */   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           /* B2C 실적 - OH변동비 */   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_V_COST'
           --AND     A.SUB_CAT_CD = 'VC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.KPI_CD, A.BASE_YYYYMM
           UNION ALL
           /* B2C 실적 - OH판촉비,운반비,SVC */   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           --AND     A.KPI_CD IN ( 'OH101000','OH201000','OH202000') -- 판촉비, 운반비, SVC
           AND     A.KPI_CD IN ('OH_V_AD_PROMOTION','OH_V_TRANS', 'OH_V_SVC')
           --AND     A.SUB_CAT_CD = 'VC'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD            
           UNION ALL
           /* B2C 실적 - OH고정비 */   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD             AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_F_COST'
           --AND     A.SUB_CAT_CD = 'FC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* B2C 실적 - OH인건비,광고선전비,지급수수료 */   
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD 
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           --AND     A.KPI_CD IN ( 'OH103000','OH101000','OH203000') -- 인건비,광고선전비,지급수수료
           --AND     A.SUB_CAT_CD = 'FC'
           AND     A.KPI_CD IN ('OH_F_LABOR' , 'OH_F_AD_PROMOTION' , 'OH_F_COMMISSION')
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM,A.KPI_CD
     
           UNION ALL

           /**************/   
           /* B2B (전년) */
           /**************/
           /* B2B 실적 - 매출/ 가격성 판촉*/   
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE', 'SALES_DEDUCTION')
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD   
           UNION ALL
           /* B2B 실적 - OH_COST */   
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM,A.KPI_CD
           UNION ALL
           /* B2B 실적 - OH변동비 */   
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_V_COST'
           --AND     A.SUB_CAT_CD = 'VC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* B2B 실적 - OH고정비 */   
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_F_COST'
           --AND     A.SUB_CAT_CD = 'FC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL

           /**************/   
           /* B2B (당월) */
           /**************/
           /* B2B 실적 - 매출/ 가격성 판촉*/   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_DIV'
           AND     A.KPI_CD IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD   
           UNION ALL
           /* B2B 실적 - OH_COST */   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_COST'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* B2B 실적 - OH변동비 */   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD          AS KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_V_COST'
           --AND     A.SUB_CAT_CD = 'VC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* B2B 실적 - OH판촉비,운반비,SVC */   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD IN ( 'OH_V_AD_PROMOTION','OH_V_TRANS','OH_V_SVC') -- 판촉비, 운반비, SVC
           --AND     A.SUB_CAT_CD = 'VC'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM,A.KPI_CD
                             
           UNION ALL
           /* B2B 실적 - OH고정비 */   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD = 'OH_F_COST'
           --AND     A.SUB_CAT_CD = 'FC'
         --AND     A.ZONE_RNR_CD <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM, A.KPI_CD
           UNION ALL
           /* B2B 실적 - OH인건비,광고선전비,지급수수료 */   
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,(CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END) AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.KOR_NM END)) AS DIV_NAME_KO
                  ,MAX((CASE WHEN CAL.CAL_DIV = 'GBU' THEN '' ELSE C2.ENG_NM END)) AS DIV_NAME_EN
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
                  ,(SELECT 'GBU' AS CAL_DIV FROM SYSIBM.SYSDUMMY1 UNION ALL
                    SELECT 'DIV' AS CAL_DIV FROM SYSIBM.SYSDUMMY1
                   ) CAL
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD = 'BEP_SMART_OH'
           AND     A.KPI_CD IN ( 'OH_F_LABOR','OH_F_AD_PROMOTION','OH_F_COMMISSION') -- 인건비,광고선전비,지급수수료
           --AND     A.SUB_CAT_CD = 'FC'
           GROUP BY A.SUBSDR_CD, (CASE WHEN CAL.CAL_DIV = 'GBU' THEN 'GBU' ELSE A.DIV_CD END), A.BASE_YYYYMM,A.KPI_CD
      
        ) Z
    WHERE Z.SUBSDR_CD = P_SUBSDR_CD
    GROUP BY Z.COL_INDEX, 
             Z.SUBSDR_CD,
             Z.DIV_CD, 
             SUBSTR(Z.BASIS_YYYYMM,1,4),
             Z.BASIS_YYYYMM, 
             Z.KPI_CD 
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