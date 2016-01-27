        WITH w1 AS
        (
            SELECT SUBSTR(w1.start_yyyymmdd, 1, 6) yyyymm
                 , a.project_code
                 , a.project_name
                 , a.corporation_code
                 , decode(a.corporation_code, 'EEEB', 'EEBN', nvl(o.up_mgt_org_cd, a.corporation_code))   subsdr_cd
                 , decode(a.corporation_code, 'EEEB', 'EEBN', nvl(o.mgt_org_cd   , a.corporation_code))   mgt_org_cd
                 , a.au_code
                 , a.division_code
                 , a.product_level3_code
                 , a.product_level4_code
                 , a.model_suffix_code
                 , a.currency_code

                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4,  'YYYYMMDD') AND w1.start_yyyymmdd THEN a.new_amt      END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4,  'YYYYMMDD') AND w1.start_yyyymmdd THEN a.increase_amt END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4,  'YYYYMMDD') AND w1.start_yyyymmdd THEN a.decrease_amt END), 0) AS week5_award_amt
                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*4,  'YYYYMMDD') AND w1.start_yyyymmdd THEN a.change_amt   END), 0) AS week5_sales_amt

                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.new_amt      END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.increase_amt END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.decrease_amt END), 0) AS week13_award_amt
                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*12, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.change_amt   END), 0) AS week13_sales_amt

                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.new_amt      END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.increase_amt END), 0) +
                   NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.decrease_amt END), 0) AS week52_award_amt
                 , NVL(SUM(CASE WHEN w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd THEN a.change_amt   END), 0) AS week52_sales_amt

--                 , vd_load_date

            FROM   tb_i24_b2b_pipeline_balance a
                 , tb_cm_week_m                w1
                 , tb_cm_week_m                w2
                 , tb_cm_mgt_org_m             o
            WHERE  o.mgt_org_type_cd         (+)= 'IS'
            AND    o.curr_flag               (+)= 'Y'
            AND    o.mgt_org_eng_name        (+)= a.corporation_name
            AND    a.stage_code                 = 'A'
            AND    a.currency_code             IN ('KRW', 'USD')

--            AND    w1.base_yyyy >= '2013'
            AND    w1.base_yyyy >= '2015'
            AND    w1.start_yyyymmdd BETWEEN '20151201' AND '20151231'
            AND    to_char(last_day(to_date(w1.start_yyyymmdd, 'YYYYMMDD')), 'YYYYMMDD') BETWEEN w1.start_yyyymmdd AND w1.end_yyyymmdd /* 매월 마지막 주차만 찾아옴 */
            AND    w1.start_yyyymmdd < to_char(SYSDATE, 'YYYYMM') || '01' /* 전월까지만 */

            AND    replace(w2.base_yyyyweek, 'W', '') = a.basis_yyyyww
            AND    w2.start_yyyymmdd BETWEEN to_char(to_date(w1.start_yyyymmdd, 'YYYYMMDD') - 7*51, 'YYYYMMDD') AND w1.start_yyyymmdd /* 실제 데이터는 과거 52주 데이터를 읽어옴 */

            GROUP  BY
                   SUBSTR(w1.start_yyyymmdd, 1, 6)
                 , a.project_code
                 , a.project_name
                 , a.corporation_code
                 , decode(a.corporation_code, 'EEEB', 'EEBN', nvl(o.up_mgt_org_cd, a.corporation_code))
                 , decode(a.corporation_code, 'EEEB', 'EEBN', nvl(o.mgt_org_cd   , a.corporation_code))
                 , a.au_code
                 , a.division_code
                 , a.product_level3_code
                 , a.product_level4_code
                 , a.model_suffix_code
                 , a.currency_code
        )
        SELECT *
        FROM   w1
        WHERE  week5_award_amt  <> 0
        OR     week5_sales_amt  <> 0
        OR     week13_award_amt <> 0
        OR     week13_sales_amt <> 0
        OR     week52_award_amt <> 0
        OR     week52_sales_amt <> 0
        ;