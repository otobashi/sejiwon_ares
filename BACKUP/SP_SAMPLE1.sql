CREATE OR REPLACE PROCEDURE ETL_IDW.SP_CD_RES_SMR_TREND_OH_HISTORY ( 
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
  /* 1.�� �� �� Ʈ : ARES                                                  */
  /* 2.��       �� :                                                       */
  /* 3.���α׷� ID : SP_CD_RES_SMR_TREND_OH_HISTORY                 */
  /*                                                                       */
  /* 4.��       �� : SMART Overhead ���̸� Result Set���� return��            */
  /*                                                                       */
  /* 5.�� �� �� �� :                                                       */
  /*                                                                       */
  /*                 IN p_basis_yyyymm( ���ؿ� )                           */
  /*                 IN p_division( ����� )                               */
  /*                 IN p_currency( ��ȭ )                                 */
  /* 6.�� �� �� ġ :                                                       */
  /* 7.�� �� �� �� :                                                       */
  /*                                                                       */
  /*  version  �ۼ���  ��      ��  ��                 ��  ��   û   ��     */
  /*  -------  ------  ----------  ---------------------  ------------     */
  /*  1.0      mysik   2015.12.18  ���� �ۼ�                               */
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
    


    SELECT Z.col_index, 
           Z.SUBSDR_CD,
           Z.SUBSDR_SHRT_NAME, 
           Z.DIV_CD, 
           Z.BASIS_YYYYMM, 
           Z.KPI_CD,           
           SUM(Z.AMOUNT) AS AMOUNT  
           
    FROM (

/* ������ü*/         
     SELECT  '1.�������_������ü_SALE' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE')
      and   a.zone_rnr_cd <> 'ZZZ'
      and   a.div_cd = 'GBU'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
               
     UNION ALL


/* ������ü_OH*/         
     SELECT  '2.�������_������ü_OH' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000' -- OH_COST
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD  
      
      UNION ALL
      
     SELECT  '1.�������_������ü_SALE' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             BASE_YYYYMM AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE')
      and   a.zone_rnr_cd <> 'ZZZ'
      and   a.div_cd = 'GBU'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD  
               
     UNION ALL


/* ������ü_OH*/         
     SELECT  '2.�������_������ü_OH' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000' -- OH_COST
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD        



     UNION ALL
     
/**************/   
/* B2C (����) */
/**************/

     /* B2C ���� - ����/ ���ݼ� ����*/   
     
     SELECT  '1.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD   

     UNION ALL

     /* B2C ���� - OH_COST */   
     
     SELECT  '1.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   

     UNION ALL
     /* B2C ���� - OH������ */   
     
     SELECT  '1.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_V_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'VC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD  

     
     UNION ALL
     /* B2C ���� - OH������ */   
     
     SELECT  '1.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_F_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'FC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   
               
               
               
     UNION ALL                                        

/**************/   
/* B2C (���) */
/**************/

     /* B2C ���� - ����/ ���ݼ� ����*/   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD   

     UNION ALL

     /* B2C ���� - OH_COST */   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   

     UNION ALL
     /* B2C ���� - OH������ */   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_V_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'VC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD  

     UNION ALL
     
     
     /* B2C ���� - OH���˺�,��ݺ�,SVC */   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             CASE WHEN A.KPI_CD = 'OH101000' THEN 'OH_V_AD_PROMOTION' 
                  WHEN A.KPI_CD = 'OH201000' THEN 'OH_V_TRANS' 
                  WHEN A.KPI_CD = 'OH202000' THEN 'OH_V_SVC'
                  ELSE '*'
             END AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD IN ( 'OH101000','OH201000','OH202000') -- ���˺�, ��ݺ�, SVC
      AND    SUB_CAT_CD = 'VC'
  
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM ,
               CASE WHEN A.KPI_CD = 'OH101000' THEN 'OH_V_AD_PROMOTION' 
                  WHEN A.KPI_CD = 'OH201000' THEN 'OH_V_TRANS' 
                  WHEN A.KPI_CD = 'OH202000' THEN 'OH_V_SVC'
                  ELSE '*'
               END             

     
     UNION ALL
     /* B2C ���� - OH������ */   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_F_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'FC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   
               

     UNION ALL
     
     /* B2C ���� - OH�ΰǺ�,��������,���޼����� */   
     
     SELECT  '2.�������_B2C' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
	           CASE WHEN A.KPI_CD = 'OH103000' THEN 'OH_F_LABOR' 
	                WHEN A.KPI_CD = 'OH101000' THEN 'OH_F_AD_PROMOTION' 
	                WHEN A.KPI_CD = 'OH203000' THEN 'OH_F_COMMISSION'
	                ELSE '*'
	           END  AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2C_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD IN ( 'OH103000','OH101000','OH203000') -- �ΰǺ�,��������,���޼�����
      AND    SUB_CAT_CD = 'FC'
  
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM ,
               CASE WHEN A.KPI_CD = 'OH103000' THEN 'OH_F_LABOR' 
                    WHEN A.KPI_CD = 'OH101000' THEN 'OH_F_AD_PROMOTION' 
                    WHEN A.KPI_CD = 'OH203000' THEN 'OH_F_COMMISSION'
                    ELSE '*'
               END       
                
                  

     UNION ALL
     
/**************/   
/* B2B (����) */
/**************/

     /* B2B ���� - ����/ ���ݼ� ����*/   
     
     SELECT  '1.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD   

     UNION ALL

     /* B2B ���� - OH_COST */   
     
     SELECT  '1.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   

     UNION ALL
     /* B2B ���� - OH������ */   
     
     SELECT  '1.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_V_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'VC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD  

     
     UNION ALL
     /* B2B ���� - OH������ */   
     
     SELECT  '1.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             TO_CHAR(TO_DATE(BASE_YYYYMM, 'YYYYMM') + 1 YEAR , 'YYYYMM') AS BASIS_YYYYMM ,
             'OH_F_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 YEAR , 'YYYYMM') 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'FC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   
               
               
               
     UNION ALL                                        

/**************/   
/* B2B (���) */
/**************/

     /* B2B ���� - ����/ ���ݼ� ����*/   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             A.KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_DIV'
      AND   A.KPI_CD in ('SALE', 'SALES_DEDUCTION')
      and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM,
               A.KPI_CD   

     UNION ALL

     /* B2B ���� - OH_COST */   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   

     UNION ALL
     /* B2B ���� - OH������ */   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_V_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'VC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD  

     UNION ALL
     
     
     /* B2B ���� - OH���˺�,��ݺ�,SVC */   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             CASE WHEN A.KPI_CD = 'OH101000' THEN 'OH_V_AD_PROMOTION' 
                  WHEN A.KPI_CD = 'OH201000' THEN 'OH_V_TRANS' 
                  WHEN A.KPI_CD = 'OH202000' THEN 'OH_V_SVC'
                  ELSE '*'
             END AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD IN ( 'OH101000','OH201000','OH202000') -- ���˺�, ��ݺ�, SVC
      AND    SUB_CAT_CD = 'VC'
  
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM ,
               CASE WHEN A.KPI_CD = 'OH101000' THEN 'OH_V_AD_PROMOTION' 
                  WHEN A.KPI_CD = 'OH201000' THEN 'OH_V_TRANS' 
                  WHEN A.KPI_CD = 'OH202000' THEN 'OH_V_SVC'
                  ELSE '*'
               END             

     
     UNION ALL
     /* B2B ���� - OH������ */   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
             'OH_F_COST' AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD = 'OH000000'
      AND    SUB_CAT_CD = 'FC'
      --and   a.zone_rnr_cd <> 'ZZZ'
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM
               --A.KPI_CD   
               

     UNION ALL
     
     /* B2B ���� - OH�ΰǺ�,��������,���޼����� */   
     
     SELECT  '2.�������_B2B' AS col_index,
             A.SUBSDR_CD,
             A.ATTRIBUTE1_VALUE AS SUBSDR_SHRT_NAME,
             A.DIV_CD,
             A.BASE_YYYYMM AS BASIS_YYYYMM ,
	           CASE WHEN A.KPI_CD = 'OH103000' THEN 'OH_F_LABOR' 
	                WHEN A.KPI_CD = 'OH101000' THEN 'OH_F_AD_PROMOTION' 
	                WHEN A.KPI_CD = 'OH203000' THEN 'OH_F_COMMISSION'
	                ELSE '*'
	           END  AS KPI_CD,
             sum(A.CURRM_USD_AMT) AS amount
      FROM  IPTDW.IPTDW_RES_KPI_SUBSDR_CNTRY A
      INNER JOIN IPTDW.IPTDW_RES_DIM_CODES B
         ON B.CODE_TYPE = 'B2B_DIV'
        AND A.DIV_CD = B.CODE_ID
      WHERE A.BASE_YYYYMM BETWEEN TO_CHAR(to_date(SUBSTR(P_BASIS_YYYYMM,1, 4), 'YYYY') - 2 YEAR, 'YYYY')||'01' AND P_BASIS_YYYYMM 
      AND   A.SCENARIO_TYPE_CD = 'AC0'
      AND   A.CAT_CD = 'BEP_SMART_OH'
      AND   A.KPI_CD IN ( 'OH103000','OH101000','OH203000') -- �ΰǺ�,��������,���޼�����
      AND    SUB_CAT_CD = 'FC'
  
      
      GROUP BY A.SUBSDR_CD,
               A.ATTRIBUTE1_VALUE,
               A.DIV_CD,
               A.BASE_YYYYMM ,
               CASE WHEN A.KPI_CD = 'OH103000' THEN 'OH_F_LABOR' 
                    WHEN A.KPI_CD = 'OH101000' THEN 'OH_F_AD_PROMOTION' 
                    WHEN A.KPI_CD = 'OH203000' THEN 'OH_F_COMMISSION'
                    ELSE '*'
               END       
     ) Z
    WHERE Z.SUBSDR_SHRT_NAME = P_SUBSDR_CD
    GROUP BY Z.col_index,
           Z.col_index, 
           Z.SUBSDR_CD,
           Z.SUBSDR_SHRT_NAME, 
           Z.DIV_CD, 
           Z.BASIS_YYYYMM, 
           Z.KPI_CD 
      WITH UR ;            
 
      
    
    OPEN C1;
   /* LOG ���� RESET */
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