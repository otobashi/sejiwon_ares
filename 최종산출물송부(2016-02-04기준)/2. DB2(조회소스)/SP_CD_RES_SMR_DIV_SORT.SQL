CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_DIV_SORT
(
  IN P_CODE_TYPE VARCHAR(30),
  IN P_SUBSDR_CD VARCHAR(8)
)
LANGUAGE SQL
DYNAMIC RESULT SETS 1
BEGIN
  /*************************************************************************/
  /* 1.프 로 젝 트 : ARES                                                  */
  /* 2.모       듈 :                                                       */
  /* 3.프로그램 ID :   SP_CD_RES_SMR_DIV_SORT                              */
  /*                                                                       */
  /* 4.설       명 : 법인별 제품기준별 표시                                */
  /*                                                                       */
  /* 5.입 력 변 수 :                                                       */
  /*                 P_CODE_TYPE( 기준년월 )                               */
  /*                 'SMART_B2B_Sale_COI'                                  */
  /*                 'SMART_B2C_Sale_COI'                                  */  
  /*                 'SMART_B2B_Sales_Deduction'                           */
  /*                 'SMART_B2C_Sales_Deduction'                           */  
  /* 6.파 일 위 치 :                                                       */
  /* 7.변 경 이 력 :                                                       */
  /*                                                                       */
  /*  version  작성자  일      자  내                 용  요   청   자     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      KIM.S.K 2016.02.03  최초 작성                               */
  /*                                                                       */
  /*************************************************************************/ 
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_DIV_SORT';
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
    
    SELECT DISPLAY_ORDER_SEQ AS SEQ
          ,DIVISION_CODE     AS CODE_ID
          ,DISPLAY_NAME1     AS CODE_NAME_KO
          ,DISPLAY_NAME2     AS CODE_NAME_EN
          ,ATTRIBUTE2        AS B2B_B2C
          ,CORPORATION_CODE  AS CORP_CODE
    FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
    WHERE  (CODE_TYPE, CORPORATION_CODE) IN (SELECT CODE_TYPE, MAX(CORPORATION_CODE)
                                             FROM   IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
                                             WHERE  CODE_TYPE         = P_CODE_TYPE
                                             AND    CORPORATION_CODE IN ('*', P_SUBSDR_CD)
                                             GROUP BY CODE_TYPE
                                            )
    ORDER BY DISPLAY_ORDER_SEQ
    WITH UR; 
 
    OPEN C1;

   /* LOG 변수 RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_DIM_CORP_DISPLAY_MASTER';
    --SET v_basis_yyyymmdd             = p_basis_yyyymm;
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