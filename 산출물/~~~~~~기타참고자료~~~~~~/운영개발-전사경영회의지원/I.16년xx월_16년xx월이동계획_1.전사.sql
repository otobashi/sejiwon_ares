CREATE OR REPLACE PROCEDURE SP_CD_RES_KPI_MOVEPLAN_B2B_HIS
(
  IN P_BASIS_YYYYMM  VARCHAR(6)
)
  DYNAMIC RESULT SETS 1
  LANGUAGE SQL

BEGIN
  /************************************************************************************************/
  /* 1.�� �� �� Ʈ : ARES                                                                         */
  /* 2.��       �� :                                                                              */
  /* 3.���α׷� ID : SP_CD_RES_KPI_MOVEPLAN_B2B_HIS                                               */
  /* 4.��       �� :                                                                              */
  /* 5.�� �� �� �� :                                                                              */
  /*                 IN P_BASIS_YYYYMM( ���ؿ� )                                                  */
  /* 6.�� �� �� ġ :                                                                              */
  /* 7.�� �� �� �� :                                                                              */
  /*  version  �ۼ���  ��      ��  ��                 ��                             ��   û   �� */
  /*  -------  ------  ----------  ------------------------------------------------  ------------ */
  /*  1.0      shlee   2016.02.04  ���� �ۼ�                                                      */
  /************************************************************************************************/
    DECLARE v_etl_job_no                 VARCHAR(30)   DEFAULT 'SP_CD_RES_KPI_MOVEPLAN_B2B_HIS';
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

    DECLARE C1 CURSOR WITH HOLD WITH RETURN FOR -- CURSOR WITH RETURN FOR(���ν��� �� ��� ��Ʈ�� ȣ���� ��)
    
    -- ��ü����
    SELECT '��ü����'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '����'
                WHEN '549999PL' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
--    WHERE  A.BASE_YYYYMM    = P_BASIS_YYYYMM
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('AC0')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    -- ����̵�
    SELECT '����̵�'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '����'
                WHEN '549999PL' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
--    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
    WHERE  A.BASE_YYYYMM    = P_BASIS_YYYYMM
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    -- �����̵�
    SELECT '�����̵�'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '����'
                WHEN '549999PL' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
--    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
    WHERE  A.BASE_YYYYMM    = TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 1 MONTH, 'YYYYMM')
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('PR2','PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD
    UNION ALL
    -- �������̵�
    SELECT '�������̵�'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN '41000000' THEN '����'
                WHEN '549999PL' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' END AS SCENARIO_TYPE_CD
          ,'ALL' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
--    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
    WHERE  A.BASE_YYYYMM    = TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 2 MONTH, 'YYYYMM')
    AND    A.DIV_CD         = 'ALL'
    AND    A.SUBSDR_CD      = 'ALL'
    AND    A.LDGR_TYPE_CD   = '1'
    AND    A.SUBSDR_TYPE_CD = 'S'
    AND    A.KPI_TYPE_CD    = 'TB'
    AND    A.ACCT_CD        IN ('41000000','549999PL')
    AND    A.SCENARIO_TYPE_CD IN ('PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL

    -- M/L
    SELECT 'ML'         AS COL_INDEX
          ,CASE A.SCENARIO_CODE
                WHEN 'AC0' THEN A.BASIS_YYYYMM
                WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASIS_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.KPI_TYPE_CODE
                WHEN 'SALE' THEN '����'
                WHEN 'COI'  THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_CODE
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' END AS SCENARIO_TYPE_CD
          ,'ML' AS B2B_TYPE
          ,A.WEEK_INPUT AS WEEK_INPUT
          ,SUM(A.CURR_MON_KRW_AMOUNT) AS CURRM_KRW_AMT
          ,SUM(A.CURR_MON_USD_AMOUNT) AS CURRM_USD_AMT
          ,NULL AS ACCUM_KRW_AMT
          ,NULL AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_ADJ_MOST_LIKELY A
--    WHERE  A.BASIS_YYYYMM   BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
    WHERE  A.BASIS_YYYYMM   = P_BASIS_YYYYMM
    AND    A.DIVISION_CODE  = 'GBU'
    AND    A.KPI_TYPE_CODE  IN ('SALE','COI')
    AND    A.SCENARIO_CODE  IN ('AC0','PR1','PR2','PR3','PR4')
    GROUP BY A.BASIS_YYYYMM
            ,A.KPI_TYPE_CODE
            ,A.SCENARIO_CODE
            ,A.WEEK_INPUT

    UNION ALL
    
    -- B2B����
    SELECT 'B2B����'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN 'BEP20000B2B' THEN '����'
                WHEN 'BEP60000B2B' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' 
                END AS SCENARIO_TYPE_CD
          ,'B2B' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
    WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
--    WHERE  A.BASE_YYYYMM     = P_BASIS_YYYYMM
    AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
    AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
    AND    A.DATA_DELIMT_CD  = 'DIV'
    AND    A.PROD_CD         = 'ALL'
    AND    A.SUBSDR_CD       = 'ALL'
    AND    A.KPI_TYPE_CD     = 'B2B'
    AND    A.SUMM_FLAG       = 'Y'
    AND    A.SCENARIO_TYPE_CD IN ('AC0')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    
    -- B2B��ȹ
    SELECT 'B2B����'         AS COL_INDEX
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN A.BASE_YYYYMM
                WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                END AS BASE_YYYYMM
          ,CASE A.ACCT_CD
                WHEN 'BEP20000B2B' THEN '����'
                WHEN 'BEP60000B2B' THEN '��������' END AS ACCT_CD
          ,CASE A.SCENARIO_TYPE_CD
                WHEN 'AC0' THEN '����'
                ELSE '��ȹ' 
                END AS SCENARIO_TYPE_CD
          ,'B2B' AS B2B_TYPE
          ,'ALL' AS WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
    FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
--    WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
    WHERE  A.BASE_YYYYMM     = P_BASIS_YYYYMM
    AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
    AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
    AND    A.DATA_DELIMT_CD  = 'DIV'
    AND    A.PROD_CD         = 'ALL'
    AND    A.SUBSDR_CD       = 'ALL'
    AND    A.KPI_TYPE_CD     = 'B2B'
    AND    A.SUMM_FLAG       = 'Y'
    AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
    GROUP BY A.BASE_YYYYMM
            ,A.ACCT_CD
            ,A.SCENARIO_TYPE_CD

    UNION ALL
    -- B2C
    SELECT 'B2C����'         AS COL_INDEX
          ,A.BASE_YYYYMM
          ,A.ACCT_CD
          ,A.SCENARIO_TYPE_CD
          ,A.B2B_TYPE
          ,A.WEEK_INPUT
          ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
          ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
          ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
          ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT      
    FROM   (
            -- ��ü����
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN '41000000' THEN '����'
                        WHEN '549999PL' THEN '��������' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '����'
                        ELSE '��ȹ' END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
            WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
        --    WHERE  A.BASE_YYYYMM    = P_BASIS_YYYYMM
            AND    A.DIV_CD         = 'ALL'
            AND    A.SUBSDR_CD      = 'ALL'
            AND    A.LDGR_TYPE_CD   = '1'
            AND    A.SUBSDR_TYPE_CD = 'S'
            AND    A.KPI_TYPE_CD    = 'TB'
            AND    A.ACCT_CD        IN ('41000000','549999PL')
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD

            UNION ALL

            -- ��ü��ȹ
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4' THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN '41000000' THEN '����'
                        WHEN '549999PL' THEN '��������' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '����'
                        ELSE '��ȹ' END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT) AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT) AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT) AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT) AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_S A
        --    WHERE  A.BASE_YYYYMM    BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
            WHERE  A.BASE_YYYYMM    = P_BASIS_YYYYMM
            AND    A.DIV_CD         = 'ALL'
            AND    A.SUBSDR_CD      = 'ALL'
            AND    A.LDGR_TYPE_CD   = '1'
            AND    A.SUBSDR_TYPE_CD = 'S'
            AND    A.KPI_TYPE_CD    = 'TB'
            AND    A.ACCT_CD        IN ('41000000','549999PL')
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD

            UNION ALL

            -- B2B����
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN 'BEP20000B2B' THEN '����'
                        WHEN 'BEP60000B2B' THEN '��������' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '����'
                        ELSE '��ȹ' 
                        END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT)*-1 AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT)*-1 AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT)*-1 AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT)*-1 AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
            WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
        --    WHERE  A.BASE_YYYYMM     = P_BASIS_YYYYMM
            AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
            AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
            AND    A.DATA_DELIMT_CD  = 'DIV'
            AND    A.PROD_CD         = 'ALL'
            AND    A.SUBSDR_CD       = 'ALL'
            AND    A.KPI_TYPE_CD     = 'B2B'
            AND    A.SUMM_FLAG       = 'Y'
            AND    A.SCENARIO_TYPE_CD IN ('AC0')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD

            UNION ALL

            -- B2B��ȹ
            SELECT CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN A.BASE_YYYYMM
                        WHEN 'PR1'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 1 MONTH, 'YYYYMM') 
                        WHEN 'PR2'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 2 MONTH, 'YYYYMM') 
                        WHEN 'PR3'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 3 MONTH, 'YYYYMM') 
                        WHEN 'PR4'  THEN TO_CHAR(TO_DATE(A.BASE_YYYYMM, 'YYYYMM') + 4 MONTH, 'YYYYMM') 
                        END AS BASE_YYYYMM
                  ,CASE A.ACCT_CD
                        WHEN 'BEP20000B2B' THEN '����'
                        WHEN 'BEP60000B2B' THEN '��������' END AS ACCT_CD
                  ,CASE A.SCENARIO_TYPE_CD
                        WHEN 'AC0' THEN '����'
                        ELSE '��ȹ' 
                        END AS SCENARIO_TYPE_CD
                  ,'B2C' AS B2B_TYPE
                  ,'ALL' AS WEEK_INPUT
                  ,SUM(A.CURRM_KRW_AMT)*-1 AS CURRM_KRW_AMT
                  ,SUM(A.CURRM_USD_AMT)*-1 AS CURRM_USD_AMT
                  ,SUM(A.ACCUM_KRW_AMT)*-1 AS ACCUM_KRW_AMT
                  ,SUM(A.ACCUM_USD_AMT)*-1 AS ACCUM_USD_AMT
            FROM   IPTDW.IPTDW_RES_KPI_DIV_B2B_S A
        --    WHERE  A.BASE_YYYYMM     BETWEEN SUBSTR(TO_CHAR(TO_DATE(P_BASIS_YYYYMM, 'YYYYMM') - 24 MONTH, 'YYYYMM'),1,4)||'01' AND P_BASIS_YYYYMM
            WHERE  A.BASE_YYYYMM     = P_BASIS_YYYYMM
            AND    A.ACCT_CD         IN ('BEP60000B2B','BEP20000B2B')
            AND    A.DIV_BIZ_TYPE_CD = 'B2B_ALL'
            AND    A.DATA_DELIMT_CD  = 'DIV'
            AND    A.PROD_CD         = 'ALL'
            AND    A.SUBSDR_CD       = 'ALL'
            AND    A.KPI_TYPE_CD     = 'B2B'
            AND    A.SUMM_FLAG       = 'Y'
            AND    A.SCENARIO_TYPE_CD IN ('PR1','PR2','PR3','PR4')
            GROUP BY A.BASE_YYYYMM
                    ,A.ACCT_CD
                    ,A.SCENARIO_TYPE_CD

           ) A    
    GROUP BY A.BASE_YYYYMM
          ,A.ACCT_CD
          ,A.SCENARIO_TYPE_CD
          ,A.B2B_TYPE
          ,A.WEEK_INPUT 
                      
    WITH UR;

    OPEN C1;

   /* LOG ���� RESET */
    SET v_load_start_timestamp       = CURRENT TIMESTAMP;
    SET v_serial_no                  = '1';
    SET v_target_insert_count        = 0;
    SET v_target_update_count        = 0;
    SET v_target_delete_count        = 0;
    SET v_source_table_name          = 'IPTDW_RES_KPI_DIV_S';
    SET v_basis_yyyymmdd             = P_BASIS_YYYYMM;
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