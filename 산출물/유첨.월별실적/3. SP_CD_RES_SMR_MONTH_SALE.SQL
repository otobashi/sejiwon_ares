CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_MONTH_SALE (
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
  /* 3.프로그램 ID : SP_CD_RES_SMR_MONTH_SALE                                                     */
  /* 4.설       명 : 유첨. 제품별 경영 실적 - 1.월별실적                                          */
  /* 5.입 력 변 수 :                                                                              */
  /*                 IN P_BASIS_YYYYMM( 기준월 )                                                  */
  /*                 IN P_SUBSDR_CD( 법인 )                                                       */
  /*                 IN P_DIV_CD( DIVISION CODE )                                                 */
  /* 6.파 일 위 치 :                                                                              */
  /* 7.변 경 이 력 :                                                                              */
  /*  version  작성자  일      자  내                 용                             요   청   자 */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.01.25  최초 작성                                                      */
  /*  1.1      shlee   2016.01.27  한글/영문 추가작업필요없음.                                    */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(40)   DEFAULT 'SP_CD_RES_SMR_MONTH_SALE';
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
      SELECT CASE A.KPI_CD
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
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE', 'COI','SALES_DEDUCTION')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE A.KPI_CD
                    WHEN 'SALE' THEN '매출'
                    WHEN 'COI'  THEN '영업이익'
                    WHEN 'SALES_DEDUCTION' THEN '가격성판촉비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 2.MostLikely대비(W-1)
      SELECT 'MostLikely대비(W-1)'         AS COL_INDEX
            ,A.SUBSDR_CD   AS SUBSDR_CD
            ,A.BASE_YYYYMM AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE  A.BASE_YYYYMM BETWEEN TO_CHAR(ADD_MONTHS(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), -2), 'YYYYMM') AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'PR0'
      AND    A.ZONE_RNR_CD = '1'
      AND    A.CAT_CD = 'BEP_SMART_ML'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,A.BASE_YYYYMM
              ,A.KPI_CD

      UNION ALL

-- 3. 전월이동대비
      SELECT '전월이동'         AS COL_INDEX
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
      AND    A.SCENARIO_TYPE_CD = 'MP'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 4. 전년대비
      SELECT '전년대비'         AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END AS BASE_YYYYMM
            ,A.KPI_CD             AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),1,4)||'12'
      AND    A.SCENARIO_TYPE_CD = 'MP'
      AND    A.CAT_CD = 'BEP_SMART_DIV'
      AND    A.KPI_CD in ('SALE')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)
                  WHEN 2 THEN SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), +12), 'YYYYMM'),1,4)||SUBSTR(TO_CHAR(ADD_MONTHS(TO_DATE(A.BASE_YYYYMM, 'YYYYMM'), -12), 'YYYYMM'),5,2) END
              ,A.KPI_CD

      UNION ALL

-- 5. 광고선전비/판매촉진비
      SELECT CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END         AS COL_INDEX
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
      AND    A.CAT_CD = 'BEP_SMART_OH'
      AND    A.KPI_CD in ('OH101000')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE A.SUB_CAT_CD
                  WHEN 'FC' THEN '광고선전비'
                  WHEN 'VC' THEN '판매촉진비' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL
            
-- 6. 저수익 매출
      SELECT '저수익매출' AS COL_INDEX
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
      GROUP BY A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 7. 한계적자매출/한계이익
      SELECT CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자매출' END AS COL_INDEX
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
              SELECT 'MARGINAL_PF_(-)'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-10_-5'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-15_-10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_-5_0'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_0_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 'COI_10'  AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_SUBSDR'
      AND    A.KPI_CD in ('MGN_PROFIT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE SUBSTR(D.SUB_CAT_CD,1,3)
                  WHEN 'COI' THEN '한계이익'
                  ELSE '한계적자매출' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 8.재고일수/역산법/장기재고
      SELECT CASE A.KPI_CD
                    WHEN 'INV_DIO' THEN '재고일수'
                    WHEN 'INV_DIO_R' THEN '역산법'
                    WHEN 'INV_LONG_TERM' THEN '장기재고' END         AS COL_INDEX
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
      AND    A.CAT_CD = 'SMART_INV'
      AND    A.KPI_CD IN ('INV_DIO', 'INV_DIO_R', 'INV_LONG_TERM')
      AND    A.SUBSDR_CD = P_SUBSDR_CD
      AND    A.DIV_CD = P_DIV_CD
      GROUP BY CASE A.KPI_CD
                    WHEN 'INV_DIO' THEN '재고일수'
                    WHEN 'INV_DIO_R' THEN '역산법'
                    WHEN 'INV_LONG_TERM' THEN '장기재고' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,A.KPI_CD

      UNION ALL

-- 9.생산성
      SELECT CASE D.SUB_CAT_CD
                  WHEN '10600000'       THEN '(HR) 원당매출액'              
                  WHEN '10600000EXC01A' THEN '(HR) 원당매출액_사내도급제외' 
                  WHEN '10600010'       THEN '(HR) 인당매출액'              
                  WHEN '10600010EXC01A' THEN '(HR) 인당매출액_사내도급제외' 
                  WHEN '10600020'       THEN '(HR) 인원수'                  
                  WHEN '10600020CMMN'   THEN '(HR) 인원수_공통인원'         
                  WHEN '10600020CONT'   THEN '(HR) 인원수_사내도급'         
                  WHEN '10600020EXC01A' THEN '(HR) 인원수_사내도급제외'     
                  WHEN '10600020FSE'    THEN '(HR) 인원수_FSE'              
                  WHEN '10600020IMKT'   THEN '(HR) 인원수_ISE_영업/마케팅'  
                  WHEN '10600020ISE'    THEN '(HR) 인원수_ISE'              
                  WHEN '10600020TEMP'   THEN '(HR) 인원수_임시직'           
                  WHEN '10600050'       THEN '(HR) 인건비(HR생산성 기준)'   
                  WHEN '10600050EXC01A' THEN '(HR) 인건비_사내도급제외' END AS COL_INDEX
            ,A.SUBSDR_CD          AS SUBSDR_CD
            ,CASE B.SEQ 
                  WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                  WHEN 2 THEN A.BASE_YYYYMM END AS BASE_YYYYMM
            ,D.SUB_CAT_CD         AS KPI_CD
            ,SUM(A.CURRM_USD_AMT) AS AMOUNT
            ,'0'                  AS SORT_KEY           
      FROM   IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
            ,(
              SELECT 1 AS SEQ FROM SYSIBM.SYSDUMMY1 UNION ALL
              SELECT 2 FROM SYSIBM.SYSDUMMY1
             ) B
            ,(
              SELECT '10600000'       AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 원당매출액'              AS HR_NM
              SELECT '10600000EXC01A' AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 원당매출액_사내도급제외' AS HR_NM
              SELECT '10600010'       AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인당매출액'              AS HR_NM
              SELECT '10600010EXC01A' AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인당매출액_사내도급제외' AS HR_NM
              SELECT '10600020'       AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수'                  AS HR_NM
              SELECT '10600020CMMN'   AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_공통인원'         AS HR_NM
              SELECT '10600020CONT'   AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_사내도급'         AS HR_NM
              SELECT '10600020EXC01A' AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_사내도급제외'     AS HR_NM
              SELECT '10600020FSE'    AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_FSE'              AS HR_NM
              SELECT '10600020IMKT'   AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_ISE_영업/마케팅'  AS HR_NM
              SELECT '10600020ISE'    AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_ISE'              AS HR_NM
              SELECT '10600020TEMP'   AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인원수_임시직'           AS HR_NM
              SELECT '10600050'       AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1 UNION ALL -- , '(HR) 인건비(HR생산성 기준)'   AS HR_NM
              SELECT '10600050EXC01A' AS SUB_CAT_CD FROM SYSIBM.SYSDUMMY1           -- , '(HR) 인건비_사내도급제외'     AS HR_NM
             ) D
      WHERE  A.BASE_YYYYMM BETWEEN '201301' AND P_BASIS_YYYYMM
      AND    A.SCENARIO_TYPE_CD = 'AC0'
      AND    A.CAT_CD = 'BEP_SMART_HR'
      AND    A.KPI_CD in ('HR','HR_CNT')
      AND    A.SUB_CAT_CD = D.SUB_CAT_CD
      AND    A.SUBSDR_CD  = P_SUBSDR_CD
      AND    A.DIV_CD     = P_DIV_CD
      GROUP BY CASE D.SUB_CAT_CD
                    WHEN '10600000'       THEN '(HR) 원당매출액'              
                    WHEN '10600000EXC01A' THEN '(HR) 원당매출액_사내도급제외' 
                    WHEN '10600010'       THEN '(HR) 인당매출액'              
                    WHEN '10600010EXC01A' THEN '(HR) 인당매출액_사내도급제외' 
                    WHEN '10600020'       THEN '(HR) 인원수'                  
                    WHEN '10600020CMMN'   THEN '(HR) 인원수_공통인원'         
                    WHEN '10600020CONT'   THEN '(HR) 인원수_사내도급'         
                    WHEN '10600020EXC01A' THEN '(HR) 인원수_사내도급제외'     
                    WHEN '10600020FSE'    THEN '(HR) 인원수_FSE'              
                    WHEN '10600020IMKT'   THEN '(HR) 인원수_ISE_영업/마케팅'  
                    WHEN '10600020ISE'    THEN '(HR) 인원수_ISE'              
                    WHEN '10600020TEMP'   THEN '(HR) 인원수_임시직'           
                    WHEN '10600050'       THEN '(HR) 인건비(HR생산성 기준)'   
                    WHEN '10600050EXC01A' THEN '(HR) 인건비_사내도급제외' END
              ,A.SUBSDR_CD
              ,CASE B.SEQ 
                    WHEN 1 THEN SUBSTR(A.BASE_YYYYMM,1,4)
                    WHEN 2 THEN A.BASE_YYYYMM END
              ,D.SUB_CAT_CD

  
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