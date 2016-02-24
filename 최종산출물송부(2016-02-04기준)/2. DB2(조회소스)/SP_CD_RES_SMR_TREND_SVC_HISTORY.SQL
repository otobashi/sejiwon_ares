CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_SVC_HISTORY ( 
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
  /*************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                  */
  /* 2.모       듈 :                                                       */
  /* 3.프로그램 ID : SP_CD_RES_SMR_TREND_SVC_HISTORY                       */
  /*                                                                       */
  /* 4.설       명 : SMART SVC 추이를 Result Set으로 return함              */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                                                                       */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                           */
  /*                 IN P_SUBSDR_CD ( 법인 )                               */
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      KIM.S.K 2016.01.26  최초 작성                               */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_TREND_SVC_HISTORY';
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

    WITH V_SMART_SVC
    AS ( SELECT YYYYMMDD    AS BASIS_PERIOD
               ,CODE_ID     AS KPI_TYPE_CODE
               ,ATTRIBUTE1  AS SUBSIDIARY_CODE
               ,'GBU'       AS DIVISION_CODE
               ,ATTRIBUTE5  AS ATTRIBUTE5      /* SVC_CALL : Answer Speed, SVC_RTAT : RTAT, SVC_NET_COST : Curr. Amt (USD) */
               ,ATTRIBUTE6  AS ATTRIBUTE6      /* SVC_CALL : Waiting Time, SVC_RTAT : Total Repair Lead Time */
               ,ATTRIBUTE7  AS ATTRIBUTE7      /* SVC_CALL : Response Call, SVC_RTAT : Total Repair */
               ,ATTRIBUTE8  AS ATTRIBUTE8      /* SVC_NET_COST : Net SVC Categoy */
               ,ATTRIBUTE9  AS ATTRIBUTE9      /* SVC_NET_COST : Net SVC Categoy Name */
               ,ATTRIBUTE10 AS ATTRIBUTE10     /* SVC_NET_COST : Net SVC Detail */
               ,ATTRIBUTE11 AS ATTRIBUTE11     /* SVC_NET_COST : Net SVC Detail Name */
         FROM   IPTDW.IPTDW_RES_EXCEL_UPLOAD_DATA
         WHERE  SEQ         = '1560'
         AND    MODULE_CODE = 'ARES'
         AND    CODE_TYPE   = 'SMART_SVC'
         AND    CODE_ID    IN ('SVC_CALL', 'SVC_RTAT', 'SVC_NET_COST')
         AND    YYYYMMDD    BETWEEN TO_CHAR(TO_DATE(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), 'YYYYMM') 
       )   
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
    FROM  (
           -- 1.Answer Speed
           SELECT  '1.Answer Speed'   AS COL_INDEX
                  ,A.SUBSIDIARY_CODE  AS SUBSDR_CD
                  ,MAX(C1.CODE)       AS SUBSDR_SHRT_NAME
                  ,A.DIVISION_CODE    AS DIV_CD
                  ,A.BASIS_PERIOD     AS BASIS_YYYYMM
                  ,A.KPI_TYPE_CODE    AS KPI_CD
                  ,SUM(A.ATTRIBUTE5)  AS AMOUNT
                  ,''                 AS DIV_NAME_KO
                  ,''                 AS DIV_NAME_EN
           FROM    V_SMART_SVC A
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
                                   ) C1 ON (A.SUBSIDIARY_CODE = C1.CODE)
           WHERE   A.KPI_TYPE_CODE = 'SVC_CALL'
           GROUP BY A.SUBSIDIARY_CODE, A.DIVISION_CODE, A.BASIS_PERIOD, A.KPI_TYPE_CODE
           UNION ALL
           SELECT  '2.수리소요일수'   AS COL_INDEX
                  ,A.SUBSIDIARY_CODE  AS SUBSDR_CD
                  ,MAX(C1.CODE)       AS SUBSDR_SHRT_NAME
                  ,A.DIVISION_CODE    AS DIV_CD
                  ,A.BASIS_PERIOD     AS BASIS_YYYYMM
                  ,A.KPI_TYPE_CODE    AS KPI_CD
                  ,DOUBLE(DECODE(SUM(ATTRIBUTE7),0,0,SUM(ATTRIBUTE6)/SUM(ATTRIBUTE7))) AS AMOUNT
                  ,''                 AS DIV_NAME_KO
                  ,''                 AS DIV_NAME_EN
           FROM    V_SMART_SVC A
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
                                   ) C1 ON (A.SUBSIDIARY_CODE = C1.CODE)
           WHERE   A.KPI_TYPE_CODE = 'SVC_RTAT'
           GROUP BY A.SUBSIDIARY_CODE, A.DIVISION_CODE, A.BASIS_PERIOD, A.KPI_TYPE_CODE
           UNION ALL
           SELECT  '3.Net SVC 비용-전체' AS COL_INDEX
                  ,A.SUBSIDIARY_CODE     AS SUBSDR_CD
                  ,MAX(C1.CODE)       AS SUBSDR_SHRT_NAME
                  ,A.DIVISION_CODE     AS DIV_CD
                  ,A.BASIS_PERIOD      AS BASIS_YYYYMM
                  ,A.KPI_TYPE_CODE     AS KPI_CD
                  ,SUM(A.ATTRIBUTE5)   AS AMOUNT
                  ,''                  AS DIV_NAME_KO
                  ,''                  AS DIV_NAME_EN
           FROM    V_SMART_SVC A
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
                                   ) C1 ON (A.SUBSIDIARY_CODE = C1.CODE)
           WHERE   A.KPI_TYPE_CODE = 'SVC_NET_COST'
           GROUP BY A.SUBSIDIARY_CODE, A.DIVISION_CODE, A.BASIS_PERIOD, A.KPI_TYPE_CODE
           UNION ALL
           SELECT  '4.Net SVC 비용-'||A.ATTRIBUTE9  AS COL_INDEX
                  ,A.SUBSIDIARY_CODE  AS SUBSDR_CD
                  ,MAX(C1.CODE)       AS SUBSDR_SHRT_NAME
                  ,A.DIVISION_CODE    AS DIV_CD
                  ,A.BASIS_PERIOD     AS BASIS_YYYYMM
                  ,A.KPI_TYPE_CODE    AS KPI_CD
                  ,SUM(A.ATTRIBUTE5)  AS AMOUNT
                  ,''                 AS DIV_NAME_KO
                  ,''                 AS DIV_NAME_EN
           FROM    V_SMART_SVC A
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
                                   ) C1 ON (A.SUBSIDIARY_CODE = C1.CODE)
           WHERE   A.KPI_TYPE_CODE = 'SVC_NET_COST'
           GROUP BY A.SUBSIDIARY_CODE, A.DIVISION_CODE, A.BASIS_PERIOD, A.KPI_TYPE_CODE, A.ATTRIBUTE9
          ) Z
    WHERE Z.SUBSDR_CD = P_SUBSDR_CD
    GROUP BY Z.COL_INDEX, Z.SUBSDR_CD, Z.DIV_CD, SUBSTR(Z.BASIS_YYYYMM,1,4), Z.BASIS_YYYYMM, Z.KPI_CD
    ORDER BY Z.COL_INDEX, Z.SUBSDR_CD, Z.DIV_CD, SUBSTR(Z.BASIS_YYYYMM,1,4), Z.BASIS_YYYYMM, Z.KPI_CD
    WITH UR ;    
      
    
    OPEN C1;
   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_EXCEL_UPLOAD_DATA';
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