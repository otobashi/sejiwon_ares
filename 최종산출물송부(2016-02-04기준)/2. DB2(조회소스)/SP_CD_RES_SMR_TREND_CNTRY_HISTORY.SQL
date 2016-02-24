CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_CNTRY_HISTORY ( 
     IN P_BASIS_YYYYMM VARCHAR(6),
     IN P_SUBSDR_CD VARCHAR(8)
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_CNTRY_HISTORY                     */
  /*                                                                       */
  /* 4.설       명 : SMART 국가별 매출/영업이익 추이를 Result Set으로 return함 */
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
  /*  1.0      mysik   2015.12.07  최초 작성                               */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_CNTRY_HISTORY';
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
    

    /* 법인내 관할국 매출 순위 4개 */
    WITH V_CNTRY_RNK_B2C AS ( 
         SELECT  --'2.B2C_국가별' AS col_index,
                 A.SUBSDR_CD,
                 A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
                 A.CNTRY_CD,
                 A.BASE_YYYYMM,
                 A.KPI_CD,
                 SUM(A.CURRM_USD_AMT) AS AMOUNT,
                 SMALLINT(ROW_NUMBER() OVER(PARTITION BY A.SUBSDR_CD,A.BASE_YYYYMM,A.KPI_CD ORDER BY A.BASE_YYYYMM, A.KPI_CD, SUM(A.ACCU_USD_AMT) DESC)) AS SORT_KEY
          FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                 INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                         ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
          WHERE  1=1 
          AND    A.SCENARIO_TYPE_CD = 'AC0'
          AND    A.CAT_CD           = 'BEP_SMART_DIV'
          AND    A.KPI_CD          IN ('SALE')
          AND    A.DIV_CD          <> 'GBU'
          AND    A.ZONE_RNR_CD     <> 'ZZZ'
          GROUP BY A.SUBSDR_CD, A.ATTRIBUTE1_VALUE, A.CNTRY_CD, A.BASE_YYYYMM, A.KPI_CD ),

          V_CNTRY_RNK_B2B AS ( 
          SELECT  --'2.B2C_국가별' AS col_index,
                  A.SUBSDR_CD,
                  A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
                  A.CNTRY_CD,
                  A.BASE_YYYYMM,
                  A.KPI_CD,
                  SUM(A.CURRM_USD_AMT) AS AMOUNT,
                  SMALLINT(ROW_NUMBER() OVER(PARTITION BY A.SUBSDR_CD,A.BASE_YYYYMM,A.KPI_CD  ORDER BY A.BASE_YYYYMM, A.KPI_CD, SUM(A.ACCU_USD_AMT) DESC)) AS SORT_KEY
          FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                  INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                          ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
          WHERE   1=1 
          AND     A.SCENARIO_TYPE_CD = 'AC0'
          AND     A.CAT_CD           = 'BEP_SMART_DIV'
          AND     A.KPI_CD          IN ('SALE')
          AND     A.DIV_CD          <> 'GBU'
          AND     A.ZONE_RNR_CD     <> 'ZZZ'
          GROUP BY A.SUBSDR_CD, A.ATTRIBUTE1_VALUE, A.CNTRY_CD, A.BASE_YYYYMM, A.KPI_CD )               

    SELECT Z.COL_INDEX 
          ,Z.SUBSDR_CD
          ,MAX(Z.SUBSDR_SHRT_NAME) AS SUBSDR_SHRT_NAME
          ,Z.CNTRY_CD
          ,Z.BASE_YYYYMM        AS BASIS_YYYYMM
          ,Z.KPI_CD
          ,SUM(Z.AMOUNT)        AS AMOUNT
          ,Z.SORT_KEY
          ,MAX(Z.CNTRY_NAME_KO) AS CNTRY_NAME_KO
          ,MAX(Z.CNTRY_NAME_EN) AS CNTRY_NAME_EN
    FROM (
           -- 1.법인전체
           SELECT  '1.법인전체'         AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'*'                  AS CNTRY_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'0'                  AS SORT_KEY
                  ,''                   AS CNTRY_NAME_KO
                  ,''                   AS CNTRY_NAME_EN
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
           AND     A.SCENARIO_TYPE_CD  = 'AC0'
           AND     A.CAT_CD            = 'BEP_SMART_DIV'
           AND     A.KPI_CD           IN ('SALE', 'COI')
           AND     A.DIV_CD            = 'GBU'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD  
           UNION ALL   
           -- 2.B2C_국가별
           SELECT  '2.B2C_국가별'       AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.CNTRY_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,C.SORT_KEY
                  ,MAX(C3.KOR_NM)       AS CNTRY_NAME_KO
                  ,MAX(C3.ENG_NM)       AS CNTRY_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
                   INNER JOIN  V_CNTRY_RNK_B2C C
                           ON (C.SUBSDR_CD   = A.SUBSDR_CD 
                          AND  C.CNTRY_CD    = A.CNTRY_CD
                          AND  C.BASE_YYYYMM = A.BASE_YYYYMM
                        --AND  C.KPI_CD      = A.KPI_CD
                          AND  C.SORT_KEY   IN ( '1','2','3','4'))
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
                   LEFT OUTER JOIN (SELECT 'CNTRY'             AS CODE_TYPE
                                          ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                          ,A.DIVISION_CODE     AS CNTRY_CD
                                          ,A.DISPLAY_NAME1     AS KOR_NM
                                          ,A.DISPLAY_NAME2     AS ENG_NM
                                          ,A.CORPORATION_CODE  AS REF_CD                        
                                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                    WHERE  A.CODE_TYPE = 'SMART_CNTRY_NAME'
                                   ) C3
                                ON (A.CNTRY_CD = C3.CNTRY_CD)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD           = 'BEP_SMART_DIV'
           AND     A.KPI_CD          IN ('SALE', 'COI')
           AND     A.DIV_CD          <> 'GBU'
           GROUP BY A.SUBSDR_CD, A.CNTRY_CD, A.BASE_YYYYMM, A.KPI_CD, C.SORT_KEY
           UNION ALL
           -- 3.B2C_기타국가
           SELECT  '3.B2C_기타국가'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'*'                  AS CNTRY_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'900'                AS SORT_KEY
                  ,''                   AS CNTRY_NAME_KO
                  ,''                   AS CNTRY_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2C_DIV' AND A.DIV_CD = B.CODE_ID)
                   INNER JOIN V_CNTRY_RNK_B2C C
                           ON (C.SUBSDR_CD   = A.SUBSDR_CD
                          AND  C.CNTRY_CD    = A.CNTRY_CD
                          AND  C.BASE_YYYYMM = A.BASE_YYYYMM
                        --AND  C.KPI_CD      = A.KPI_CD
                          AND  C.SORT_KEY NOT IN ( '1','2','3','4'))
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
           AND     A.CAT_CD           = 'BEP_SMART_DIV'
           AND     A.KPI_CD          IN ('SALE', 'COI')
           AND     A.DIV_CD          <> 'GBU'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD
           UNION ALL               
           -- 4.B2B_국가별
           SELECT  '4.B2B_국가별'       AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,A.CNTRY_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,C.SORT_KEY
                  ,MAX(C3.KOR_NM)       AS CNTRY_NAME_KO
                  ,MAX(C3.ENG_NM)       AS CNTRY_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
                   INNER JOIN V_CNTRY_RNK_B2C C
                           ON (C.SUBSDR_CD   = A.SUBSDR_CD
                          AND  C.CNTRY_CD    = A.CNTRY_CD
                          AND  C.BASE_YYYYMM = A.BASE_YYYYMM
                        --AND  C.KPI_CD      = A.KPI_CD
                          AND  C.SORT_KEY   IN ( '1','2','3','4'))
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
                   LEFT OUTER JOIN (SELECT 'CNTRY'             AS CODE_TYPE
                                          ,A.DISPLAY_ORDER_SEQ AS DISP_SEQ
                                          ,A.DIVISION_CODE     AS CNTRY_CD
                                          ,A.DISPLAY_NAME1     AS KOR_NM
                                          ,A.DISPLAY_NAME2     AS ENG_NM
                                          ,A.CORPORATION_CODE  AS REF_CD                        
                                    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER A
                                    WHERE  A.CODE_TYPE = 'SMART_CNTRY_NAME'
                                   ) C3
                                ON (A.CNTRY_CD = C3.CNTRY_CD)
           WHERE   A.BASE_YYYYMM BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 3 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
           AND     A.SCENARIO_TYPE_CD = 'AC0'
           AND     A.CAT_CD           = 'BEP_SMART_DIV'
           AND     A.KPI_CD          IN ('SALE', 'COI')
           AND     A.DIV_CD          <> 'GBU'
           GROUP BY A.SUBSDR_CD, A.CNTRY_CD, A.BASE_YYYYMM, A.KPI_CD, C.SORT_KEY
           UNION ALL
           -- 5.B2B_기타국가
           SELECT  '5.B2B_기타국가'     AS COL_INDEX
                  ,A.SUBSDR_CD
                  ,MAX(C1.CODE)         AS SUBSDR_SHRT_NAME
                  ,'*'                  AS CNTRY_CD
                  ,A.BASE_YYYYMM
                  ,A.KPI_CD
                  ,SUM(A.CURRM_USD_AMT) AS AMOUNT
                  ,'900'                AS SORT_KEY
                  ,''                   AS CNTRY_NAME_KO
                  ,''                   AS CNTRY_NAME_EN
           FROM    IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
                   INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
                           ON (B.CODE_TYPE = 'B2B_DIV' AND A.DIV_CD = B.CODE_ID)
                   INNER JOIN V_CNTRY_RNK_B2C C
                           ON (C.SUBSDR_CD   = A.SUBSDR_CD
                          AND  C.CNTRY_CD    = A.CNTRY_CD
                          AND  C.BASE_YYYYMM = A.BASE_YYYYMM
                        --AND  C.KPI_CD      = A.KPI_CD
                          AND  C.SORT_KEY NOT IN ( '1','2','3','4'))
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
           AND     A.CAT_CD           = 'BEP_SMART_DIV'
           AND     A.KPI_CD          IN ('SALE', 'COI')
           AND     A.DIV_CD          <> 'GBU'
           GROUP BY A.SUBSDR_CD, A.BASE_YYYYMM, A.KPI_CD
          ) Z
    WHERE  Z.SUBSDR_CD = P_SUBSDR_CD
    GROUP BY Z.COL_INDEX,
             Z.SUBSDR_CD,
             Z.CNTRY_CD, 
             Z.BASE_YYYYMM, 
             Z.KPI_CD,
             Z.SORT_KEY
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