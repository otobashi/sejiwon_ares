
INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','BLSN','수주잔고','수주잔고','99999','Y');

INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','SALE_PRJ','매출(프로젝트)','매출(프로젝트)','99999','Y');

INSERT INTO IPTDW.IPTDW_RES_DIM_CORP_DISPLAY_MASTER
(
 CODE_TYPE
,DIVISION_CODE
,CORPORATION_CODE
,DISPLAY_NAME1
,DISPLAY_NAME2
,DISPLAY_ORDER_SEQ
,USE_FLAG
,ATTRIBUTE1
,ATTRIBUTE2
,ATTRIBUTE3
,ATTRIBUTE4
,ATTRIBUTE5
,CREATION_DATE
,CREATE_USER_ID
,LAST_UPDATE_DATE
,LAST_UPDATE_USER_ID
)
SELECT 'SMART_CORP_NATION' AS CODE_TYPE
      ,COUNTRY_CODE        AS DIVISION_CODE
      ,CORPORATION_CODE    AS CORPORATION_CODE
      ,COUNTRY_NAME        AS DISPLAY_NAME1
      ,COUNTRY_KOREAN_NAME AS DISPLAY_NAME2
      ,TO_NUMBER(SUBSTR(TO_CHAR(DISPLAY_ORDER_SEQ),1,4))   AS DISPLAY_ORDER_SEQ
      ,USE_FLAG            AS USE_FLAG
      ,REGION_CODE         AS ATTRIBUTE1
      ,REGION_NAME         AS ATTRIBUTE2
      ,ZONE_CODE           AS ATTRIBUTE3
      ,ZONE_NAME           AS ATTRIBUTE4
      ,CURRENCY_CODE       AS ATTRIBUTE5
      ,CURRENT TIMESTAMP   
      ,'ares'              
      ,CURRENT TIMESTAMP   
      ,'ares'              
FROM   IPTDW.IPTDW_DIM_CORPORATIONS
WITH UR
;  


INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','BB','BB Ratio','BB Ratio','99999','Y');

INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','BB_W5','5주 BB Ratio','5주 BB Ratio','99999','Y');

INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','BB_W13','13주 BB Ratio','13주 BB Ratio','99999','Y');

INSERT INTO IPTDW.IPTDW_RES_DIM_CODES
(CODE_TYPE, CODE_ID, CODE_NAME, DESCRIPTION, DISPLAY_ORDER_SEQ, USE_FLAG)
VALUES
('KPI_TYPE','BB_W52','52주 BB Ratio','52주 BB Ratio','99999','Y');
