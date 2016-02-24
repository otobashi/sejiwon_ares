CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_SALES_DEDUCT_HISTORY ( 
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_SALES_DEDUCT_HISTORY              */
  /*                                                                       */
  /* 4.설       명 : SMART 가격성판촉 추이를 Result Set으로 return함       */
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
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_SALES_DEDUCT_HISTORY';
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
           /* 법인전체 */         
           SELECT  '1.전년실적_법인전체' AS COL_INDEX
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           --AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL                
           SELECT  '2.당월실적_법인전체' AS COL_INDEX
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM  
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           --AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL
           -- 2.당월실적_법인전체(OLED 추가)
           SELECT  '2.당월실적_법인전체' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                 ,'OLED'                AS DIV_CD
                  ,BASE_YYYYMM AS BASIS_YYYYMM
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
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ATTRIBUTE3_VALUE  = 'GLT_L2_1'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL
           /*******************************************************************************/              
           /* B2C 실적 */ 
           /*******************************************************************************/       
           SELECT  '1.전년실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD        
           UNION ALL
           -- 전년실적_B2C (GBU 전사)
           SELECT  '1.전년실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD,  A.BASE_YYYYMM, A.KPI_CD        
           UNION ALL   
           -- 2.당월실적_B2C        
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD    
           UNION ALL
           -- 2.당월실적_B2C (GBU 전사)       
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD    
           UNION ALL        
           /*******************************************************************************/              
           /* B2B 실적 */ 
           /*******************************************************************************/           
           --  1.전년실적_B2B
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL                                
           --  1.전년실적_B2B (GBU 전사)
           SELECT  '1.전년실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD,  A.BASE_YYYYMM, A.KPI_CD       
           -- 2.당월실적_B2B
           UNION ALL     
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD      
           -- 2.당월실적_B2B (GBU 전사)
           UNION ALL     
           SELECT  '2.당월실적_B2B'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'SALES_DEDUCTION')
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD                
           
           /*******************************************************************************/
           /* 가격성 판촉 : 'TRADE TERM','NON TRADE TERM_SELLIN','NON TRADE TERM_SELLOUT' */
           /*******************************************************************************/       
           UNION ALL
           SELECT  '2.당월실적_법인전체' AS COL_INDEX
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM  
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_TRADE'
           AND     A.KPI_CD           IN ('TRDTM','NTRDTM_SELLIN','NTRDTM_SELLOUT')  
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL           
           -- 개별 DIV
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_TRADE'
           AND     A.KPI_CD           IN ('TRDTM','NTRDTM_SELLIN','NTRDTM_SELLOUT')  
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD               
           UNION ALL

           -- GBU
           SELECT  '2.당월실적_B2C'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
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
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_TRADE'
           AND     A.KPI_CD           IN ('TRDTM','NTRDTM_SELLIN','NTRDTM_SELLOUT')  
           AND     A.ZONE_RNR_CD      <> 'ZZZ'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD               
           UNION ALL     
           -- 개별 DIVI      
           SELECT  '2.당월실적_B2B' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
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
            WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
            AND    A.SCENARIO_TYPE_CD  = 'AC0'
            AND    A.CAT_CD            = 'BEP_SMART_TRADE'
            AND    A.KPI_CD           IN ('TRDTM','NTRDTM_SELLIN','NTRDTM_SELLOUT')  
            AND    A.ZONE_RNR_CD      <> 'ZZZ'
            GROUP BY A.SUBSDR_CD, A.DIV_CD, A.BASE_YYYYMM, A.KPI_CD    
            
            UNION ALL
            -- GBU
           SELECT  '2.당월실적_B2B' AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'GBU'                AS DIV_CD
                  ,A.BASE_YYYYMM        AS BASIS_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,MAX(C2.KOR_NM)       AS DIV_NAME_KO
                  ,MAX(C2.ENG_NM)       AS DIV_NAME_EN
            FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
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
            WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 4 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
            AND    A.SCENARIO_TYPE_CD  = 'AC0'
            AND    A.CAT_CD            = 'BEP_SMART_TRADE'
            AND    A.KPI_CD           IN ('TRDTM','NTRDTM_SELLIN','NTRDTM_SELLOUT')  
            AND    A.ZONE_RNR_CD      <> 'ZZZ'
            GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD                              
                         
           ) Z
    WHERE Z.SUBSDR_CD = P_SUBSDR_CD
    GROUP BY Z.COL_INDEX, 
             Z.SUBSDR_CD,
             Z.DIV_CD, 
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