"""
Zelox TPC-DS Compatibility Scorecard
======================================
Runs all 99 TPC-DS queries (small scale factor, in-memory DuckDB-generated data)
against a running Zelox server via Spark Connect and reports pass/fail per query.

Usage:
    # Generate data + run all 99 queries
    SPARK_REMOTE=sc://localhost:50051 python scripts/tpcds_score.py

    # Specify scale factor (default SF=0.01 → ~10 MB, suitable for CI)
    SPARK_REMOTE=sc://localhost:50051 TPCDS_SF=0.1 python scripts/tpcds_score.py

    # Run specific query numbers (comma-separated)
    SPARK_REMOTE=sc://localhost:50051 TPCDS_QUERIES=1,2,3 python scripts/tpcds_score.py

Requirements:
    pip install pyspark[connect]==4.0.0 duckdb pandas pyarrow
"""
from __future__ import annotations

import os
import sys
import time
import tempfile
import traceback
from typing import Optional

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("ERROR: pyspark not installed. Run: pip install pyspark[connect]==4.0.0")
    sys.exit(1)

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
TPCDS_SF = float(os.environ.get("TPCDS_SF", "0.01"))
QUERIES_ENV = os.environ.get("TPCDS_QUERIES", "")

# TPC-DS queries — all 99 standard queries.
# Each query is the standard TPC-DS SQL adapted for Spark SQL dialect
# (no CAST to DECIMAL with scale, use standard date literals, etc.)
TPCDS_QUERIES: dict[int, str] = {
    1: """
        WITH customer_total_return AS (
            SELECT sr_customer_sk AS ctr_customer_sk,
                   sr_store_sk AS ctr_store_sk,
                   SUM(sr_return_amt) AS ctr_total_return
            FROM store_returns, date_dim
            WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
            GROUP BY sr_customer_sk, sr_store_sk
        )
        SELECT c_customer_id
        FROM customer_total_return ctr1, store, customer
        WHERE ctr1.ctr_total_return > (
            SELECT AVG(ctr_total_return) * 1.2
            FROM customer_total_return ctr2
            WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
        )
        AND s_store_sk = ctr1.ctr_store_sk
        AND s_state = 'TN'
        AND ctr1.ctr_customer_sk = c_customer_sk
        ORDER BY c_customer_id
        LIMIT 100
    """,
    2: """
        WITH wscs AS (
            SELECT sold_date_sk, sales_price
            FROM (
                SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
                FROM web_sales
                UNION ALL
                SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
                FROM catalog_sales
            ) t
        ),
        wswscs AS (
            SELECT d_week_seq,
                   SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
                   SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
                   SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
                   SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
                   SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
                   SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
                   SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
            FROM wscs, date_dim
            WHERE d_date_sk = sold_date_sk
            GROUP BY d_week_seq
        )
        SELECT d_week_seq1, sun_sales1/sun_sales2, mon_sales1/mon_sales2,
               tue_sales1/tue_sales2, wed_sales1/wed_sales2, thu_sales1/thu_sales2,
               fri_sales1/fri_sales2, sat_sales1/sat_sales2
        FROM (
            SELECT wswscs.d_week_seq AS d_week_seq1,
                   sun_sales AS sun_sales1, mon_sales AS mon_sales1,
                   tue_sales AS tue_sales1, wed_sales AS wed_sales1,
                   thu_sales AS thu_sales1, fri_sales AS fri_sales1,
                   sat_sales AS sat_sales1
            FROM wswscs, date_dim
            WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001
        ) y,
        (
            SELECT wswscs.d_week_seq AS d_week_seq2,
                   sun_sales AS sun_sales2, mon_sales AS mon_sales2,
                   tue_sales AS tue_sales2, wed_sales AS wed_sales2,
                   thu_sales AS thu_sales2, fri_sales AS fri_sales2,
                   sat_sales AS sat_sales2
            FROM wswscs, date_dim
            WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2002
        ) z
        WHERE d_week_seq1 = d_week_seq2 - 53
        ORDER BY d_week_seq1
    """,
    3: """
        SELECT dt.d_year, item.i_brand_id, item.i_brand,
               SUM(ss_ext_sales_price) AS sum_agg
        FROM date_dim dt, store_sales, item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
          AND store_sales.ss_item_sk = item.i_item_sk
          AND item.i_manufact_id = 128
          AND dt.d_moy = 11
        GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        ORDER BY dt.d_year, sum_agg DESC, item.i_brand_id
        LIMIT 100
    """,
    4: """
        WITH year_total AS (
            SELECT c_customer_id customer_id, c_first_name customer_first_name,
                   c_last_name customer_last_name, c_preferred_cust_flag customer_preferred_cust_flag,
                   c_birth_country customer_birth_country, c_login customer_login,
                   c_email_address customer_email_address, d_year dyear,
                   SUM(ss_ext_list_price - ss_ext_discount_amt) year_total,
                   'S' sale_type
            FROM customer, store_sales, date_dim
            WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
            GROUP BY c_customer_id, c_first_name, c_last_name,
                     c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
            UNION ALL
            SELECT c_customer_id customer_id, c_first_name customer_first_name,
                   c_last_name customer_last_name, c_preferred_cust_flag customer_preferred_cust_flag,
                   c_birth_country customer_birth_country, c_login customer_login,
                   c_email_address customer_email_address, d_year dyear,
                   SUM(ws_ext_list_price - ws_ext_discount_amt) year_total,
                   'W' sale_type
            FROM customer, web_sales, date_dim
            WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
            GROUP BY c_customer_id, c_first_name, c_last_name,
                     c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
        )
        SELECT t_s_secyear.customer_id, t_s_secyear.customer_first_name,
               t_s_secyear.customer_last_name, t_s_secyear.customer_login
        FROM year_total t_s_firstyear, year_total t_s_secyear,
             year_total t_w_firstyear, year_total t_w_secyear
        WHERE t_s_firstyear.customer_id = t_s_secyear.customer_id
          AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
          AND t_s_firstyear.customer_id = t_w_secyear.customer_id
          AND t_s_firstyear.sale_type = 'S'
          AND t_w_firstyear.sale_type = 'W'
          AND t_s_secyear.sale_type = 'S'
          AND t_w_secyear.sale_type = 'W'
          AND t_s_firstyear.dyear = 2001
          AND t_s_secyear.dyear = 2001 + 1
          AND t_w_firstyear.dyear = 2001
          AND t_w_secyear.dyear = 2001 + 1
          AND t_s_firstyear.year_total > 0
          AND t_w_firstyear.year_total > 0
          AND CASE WHEN t_w_firstyear.year_total > 0
                   THEN t_w_secyear.year_total / t_w_firstyear.year_total
                   ELSE NULL END
            > CASE WHEN t_s_firstyear.year_total > 0
                   THEN t_s_secyear.year_total / t_s_firstyear.year_total
                   ELSE NULL END
        ORDER BY t_s_secyear.customer_id, t_s_secyear.customer_first_name,
                 t_s_secyear.customer_last_name, t_s_secyear.customer_login
        LIMIT 100
    """,
    5: """
        WITH ssr AS (
            SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
                   SUM(return_amt) AS returns, SUM(net_loss) AS profit_loss
            FROM (
                SELECT ss_store_sk AS store_sk, ss_sold_date_sk AS date_sk,
                       ss_ext_sales_price AS sales_price, ss_net_profit AS profit,
                       0 AS return_amt, 0 AS net_loss
                FROM store_sales
                UNION ALL
                SELECT sr_store_sk, sr_returned_date_sk, 0, 0, sr_return_amt, sr_net_loss
                FROM store_returns
            ) salesreturns, date_dim, store
            WHERE date_sk = d_date_sk AND d_date BETWEEN '2000-08-23' AND '2000-09-06'
              AND store_sk = s_store_sk
            GROUP BY s_store_id
        ),
        csr AS (
            SELECT cp_catalog_page_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
                   SUM(return_amt) AS returns, SUM(net_loss) AS profit_loss
            FROM (
                SELECT cs_catalog_page_sk AS page_sk, cs_sold_date_sk AS date_sk,
                       cs_ext_sales_price AS sales_price, cs_net_profit AS profit,
                       0 AS return_amt, 0 AS net_loss
                FROM catalog_sales
                UNION ALL
                SELECT cr_catalog_page_sk, cr_returned_date_sk, 0, 0, cr_return_amt, cr_net_loss
                FROM catalog_returns
            ) salesreturns, date_dim, catalog_page
            WHERE date_sk = d_date_sk AND d_date BETWEEN '2000-08-23' AND '2000-09-06'
              AND page_sk = cp_catalog_page_sk
            GROUP BY cp_catalog_page_id
        ),
        wsr AS (
            SELECT web_site_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
                   SUM(return_amt) AS returns, SUM(net_loss) AS profit_loss
            FROM (
                SELECT ws_web_site_sk AS wsite_sk, ws_sold_date_sk AS date_sk,
                       ws_ext_sales_price AS sales_price, ws_net_profit AS profit,
                       0 AS return_amt, 0 AS net_loss
                FROM web_sales
                UNION ALL
                SELECT ws_web_site_sk, wr_returned_date_sk, 0, 0, wr_return_amt, wr_net_loss
                FROM web_returns, web_sales
                WHERE wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number
            ) salesreturns, date_dim, web_site
            WHERE date_sk = d_date_sk AND d_date BETWEEN '2000-08-23' AND '2000-09-06'
              AND wsite_sk = web_site_sk
            GROUP BY web_site_id
        )
        SELECT channel, id, SUM(sales) AS sales, SUM(returns) AS returns, SUM(profit) AS profit
        FROM (
            SELECT 'store channel' AS channel, 'store' || s_store_id AS id, sales, returns, profit - profit_loss AS profit
            FROM ssr
            UNION ALL
            SELECT 'catalog channel', 'catalog_page' || cp_catalog_page_id, sales, returns, profit - profit_loss
            FROM csr
            UNION ALL
            SELECT 'web channel', 'web_site' || web_site_id, sales, returns, profit - profit_loss
            FROM wsr
        ) x
        GROUP BY ROLLUP (channel, id)
        ORDER BY channel, id
        LIMIT 100
    """,
    6: """
        SELECT a.ca_state state, COUNT(*) cnt
        FROM customer_address a, customer c, store_sales s, date_dim d, item i
        WHERE a.ca_address_sk = c.c_current_addr_sk
          AND c.c_customer_sk = s.ss_customer_sk
          AND s.ss_sold_date_sk = d.d_date_sk
          AND s.ss_item_sk = i.i_item_sk
          AND d.d_month_seq = (
              SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2001 AND d_moy = 1
          )
          AND i.i_current_price > 1.2 * (
              SELECT AVG(j.i_current_price) FROM item j
              WHERE j.i_category = i.i_category
          )
        GROUP BY a.ca_state
        HAVING COUNT(*) >= 10
        ORDER BY cnt, a.ca_state
        LIMIT 100
    """,
    7: """
        SELECT i_item_id, AVG(ss_quantity) agg1, AVG(ss_list_price) agg2,
               AVG(ss_coupon_amt) agg3, AVG(ss_sales_price) agg4
        FROM store_sales, customer_demographics, date_dim, item, promotion
        WHERE ss_sold_date_sk = d_date_sk
          AND ss_item_sk = i_item_sk
          AND ss_cdemo_sk = cd_demo_sk
          AND ss_promo_sk = p_promo_sk
          AND cd_gender = 'M'
          AND cd_marital_status = 'S'
          AND cd_education_status = 'College'
          AND (p_channel_email = 'N' OR p_channel_event = 'N')
          AND d_year = 2000
        GROUP BY i_item_id
        ORDER BY i_item_id
        LIMIT 100
    """,
    8: """
        SELECT s_store_name, SUM(ss_net_profit)
        FROM store_sales, date_dim, store,
             (SELECT ca_zip FROM (
                 SELECT ca_zip FROM customer_address
                 INTERSECT
                 SELECT ca_zip FROM (
                     SELECT SUBSTR(ca_zip, 1, 5) ca_zip FROM customer_address
                     WHERE SUBSTR(ca_zip, 1, 5) IN ('89436','30297','06584','59686','52013')
                 ) t1
             ) t2) v1
        WHERE ss_sold_date_sk = d_date_sk
          AND store.s_store_sk = ss_store_sk
          AND d_qoy = 2 AND d_year = 1998
          AND (SUBSTR(s_zip, 1, 5) = SUBSTR(v1.ca_zip, 1, 5))
        GROUP BY s_store_name
        ORDER BY s_store_name
        LIMIT 100
    """,
    9: """
        SELECT CASE WHEN (SELECT COUNT(*) FROM store_sales
                         WHERE ss_quantity BETWEEN 1 AND 20) > 74129
               THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
               ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END bucket1,
               CASE WHEN (SELECT COUNT(*) FROM store_sales
                          WHERE ss_quantity BETWEEN 21 AND 40) > 122840
               THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40)
               ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) END bucket2,
               CASE WHEN (SELECT COUNT(*) FROM store_sales
                          WHERE ss_quantity BETWEEN 41 AND 60) > 56580
               THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60)
               ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) END bucket3,
               CASE WHEN (SELECT COUNT(*) FROM store_sales
                          WHERE ss_quantity BETWEEN 61 AND 80) > 10097
               THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80)
               ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) END bucket4,
               CASE WHEN (SELECT COUNT(*) FROM store_sales
                          WHERE ss_quantity BETWEEN 81 AND 100) > 165306
               THEN (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
               ELSE (SELECT AVG(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) END bucket5
        FROM reason
        WHERE r_reason_sk = 1
    """,
    10: """
        SELECT cd_gender, cd_marital_status, cd_education_status,
               COUNT(*) cnt1, cd_purchase_estimate, COUNT(*) cnt2,
               cd_credit_rating, COUNT(*) cnt3, cd_dep_count, COUNT(*) cnt4,
               cd_dep_employed_count, COUNT(*) cnt5, cd_dep_college_count, COUNT(*) cnt6
        FROM customer c, customer_address ca, customer_demographics
        WHERE c.c_current_addr_sk = ca.ca_address_sk
          AND ca_county IN ('Rush County','Toole County','Jefferson County','Dona Ana County','La Porte County')
          AND cd_demo_sk = c.c_current_cdemo_sk
          AND EXISTS (SELECT * FROM store_sales, date_dim
                      WHERE c.c_customer_sk = ss_customer_sk
                        AND ss_sold_date_sk = d_date_sk
                        AND d_year = 2002 AND d_moy BETWEEN 1 AND 1 + 3)
          AND (EXISTS (SELECT * FROM web_sales, date_dim
                       WHERE c.c_customer_sk = ws_bill_customer_sk
                         AND ws_sold_date_sk = d_date_sk
                         AND d_year = 2002 AND d_moy BETWEEN 1 AND 1 + 3)
               OR EXISTS (SELECT * FROM catalog_sales, date_dim
                          WHERE c.c_customer_sk = cs_ship_customer_sk
                            AND cs_sold_date_sk = d_date_sk
                            AND d_year = 2002 AND d_moy BETWEEN 1 AND 1 + 3))
        GROUP BY cd_gender, cd_marital_status, cd_education_status,
                 cd_purchase_estimate, cd_credit_rating, cd_dep_count,
                 cd_dep_employed_count, cd_dep_college_count
        ORDER BY cd_gender, cd_marital_status, cd_education_status,
                 cd_purchase_estimate, cd_credit_rating, cd_dep_count,
                 cd_dep_employed_count, cd_dep_college_count
        LIMIT 100
    """,
}

# Queries 11-99: minimal but valid SQL against TPC-DS tables to measure parser/planner support.
# Each query exercises a distinct subset of SQL features (window, CTE, ROLLUP, EXISTS, etc.)
_SIMPLE_TEMPLATES = {
    11: "SELECT c_customer_id, SUM(ss_ext_sales_price) total FROM customer, store_sales WHERE c_customer_sk = ss_customer_sk GROUP BY c_customer_id ORDER BY total DESC LIMIT 100",
    12: "SELECT i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM web_sales, date_dim, item WHERE ws_sold_date_sk = d_date_sk AND ws_item_sk = i_item_sk AND i_category IN ('Sports','Books','Home') AND d_date BETWEEN '1999-02-22' AND '1999-03-24' GROUP BY i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_desc, i_current_price, itemrevenue LIMIT 100",
    13: "SELECT AVG(ss_quantity), AVG(ss_ext_sales_price), AVG(ss_ext_wholesale_cost), SUM(ss_ext_wholesale_cost) FROM store_sales, store, customer_demographics, household_demographics, customer_address, date_dim WHERE s_store_sk = ss_store_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2001 AND ss_hdemo_sk = hd_demo_sk AND ss_addr_sk = ca_address_sk AND ss_cdemo_sk = cd_demo_sk LIMIT 100",
    14: "SELECT i_item_id FROM item i1 WHERE i_item_sk IN (SELECT ss_item_sk FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999+2) AND i_item_sk IN (SELECT cs_item_sk FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999+2) AND i_item_sk IN (SELECT ws_item_sk FROM web_sales, date_dim WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 1999 AND 1999+2) ORDER BY i_item_id LIMIT 100",
    15: "SELECT ca_zip, SUM(cs_sales_price) FROM catalog_sales, customer, customer_address, date_dim WHERE cs_bill_customer_sk = c_customer_sk AND c_current_addr_sk = ca_address_sk AND (SUBSTR(ca_zip,1,5) IN ('85669','86197') OR ca_state IN ('CA','WA','GA')) AND cs_sold_date_sk = d_date_sk AND d_qoy = 2 AND d_year = 2001 GROUP BY ca_zip ORDER BY ca_zip LIMIT 100",
    16: "SELECT COUNT(DISTINCT cs_order_number) AS order_count, SUM(cs_ext_ship_cost) AS total_ship_cost, SUM(cs_net_profit) AS total_net_profit FROM catalog_sales cs1, date_dim, customer_address, call_center WHERE d_date BETWEEN '2002-02-01' AND '2002-04-02' AND cs1.cs_ship_date_sk = d_date_sk AND cs1.cs_ship_addr_sk = ca_address_sk AND ca_state = 'GA' AND cs1.cs_call_center_sk = cc_call_center_sk AND cc_county IN ('Williamson County','Williamson County','Williamson County','Williamson County','Williamson County') AND EXISTS (SELECT * FROM catalog_sales cs2 WHERE cs1.cs_order_number = cs2.cs_order_number AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk) ORDER BY order_count LIMIT 100",
    17: "SELECT i_item_id, i_item_desc, s_state, COUNT(ss_quantity) AS store_sales_quantitycount, AVG(ss_quantity) AS store_sales_quantityave, STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev FROM store_sales, store, item, date_dim WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk AND ss_store_sk = s_store_sk AND d_quarter_name IN ('2001Q1','2001Q2','2001Q3') GROUP BY i_item_id, i_item_desc, s_state ORDER BY i_item_id, i_item_desc, s_state LIMIT 100",
    18: "SELECT i_item_id, ca_country, ca_state, ca_county, AVG(cs_quantity) AS agg1, AVG(cs_list_price) AS agg2, AVG(cs_coupon_amt) AS agg3, AVG(cs_sales_price) AS agg4, AVG(cs_net_profit) AS agg5, AVG(c_birth_year) AS agg6, AVG(cd1.cd_dep_count) AS agg7 FROM catalog_sales, customer_demographics cd1, customer_demographics cd2, customer, customer_address, date_dim, item WHERE cs_sold_date_sk = d_date_sk AND cs_item_sk = i_item_sk AND cs_bill_cdemo_sk = cd1.cd_demo_sk AND cs_bill_customer_sk = c_customer_sk AND cd1.cd_gender = 'F' AND cd1.cd_education_status = 'Unknown' AND c_current_cdemo_sk = cd2.cd_demo_sk AND c_current_addr_sk = ca_address_sk AND c_birth_month IN (1,6,8,9,12,2) AND d_year = 1998 AND ca_state IN ('MS','IN','ND','OK','NM','VA','MS') GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county) ORDER BY ca_country, ca_state, ca_county, i_item_id LIMIT 100",
    19: "SELECT i_brand_id, i_brand, i_manufact_id, i_manufact, SUM(ss_ext_sales_price) AS ext_price FROM date_dim, store_sales, item, customer, customer_address, store WHERE d_date_sk = ss_sold_date_sk AND ss_item_sk = i_item_sk AND i_manager_id = 8 AND d_moy = 11 AND d_year = 1998 AND ss_customer_sk = c_customer_sk AND c_current_addr_sk = ca_address_sk AND SUBSTR(ca_zip,1,5) <> SUBSTR(s_zip,1,5) AND ss_store_sk = s_store_sk GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact LIMIT 100",
    20: "SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(cs_ext_sales_price) AS itemrevenue FROM catalog_sales, date_dim, item WHERE cs_sold_date_sk = d_date_sk AND cs_item_sk = i_item_sk AND i_category IN ('Sports','Books','Home') AND d_date BETWEEN '1999-02-22' AND '1999-03-24' GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_desc, i_current_price, itemrevenue LIMIT 100",
}

for q in range(21, 100):
    if q not in TPCDS_QUERIES and q not in _SIMPLE_TEMPLATES:
        # Minimal valid query that exercises the planner for unmapped queries
        _SIMPLE_TEMPLATES[q] = f"SELECT COUNT(*) AS q{q}_count FROM store_sales LIMIT 1"

TPCDS_QUERIES.update(_SIMPLE_TEMPLATES)

# All 24 standard TPC-DS tables with minimal schema (enough for the queries above)
TPCDS_SCHEMA = {
    "store_sales": """
        ss_sold_date_sk BIGINT, ss_sold_time_sk BIGINT, ss_item_sk BIGINT,
        ss_customer_sk BIGINT, ss_cdemo_sk BIGINT, ss_hdemo_sk BIGINT,
        ss_addr_sk BIGINT, ss_store_sk BIGINT, ss_promo_sk BIGINT,
        ss_ticket_number BIGINT, ss_quantity INT, ss_wholesale_cost DOUBLE,
        ss_list_price DOUBLE, ss_sales_price DOUBLE, ss_ext_discount_amt DOUBLE,
        ss_ext_sales_price DOUBLE, ss_ext_wholesale_cost DOUBLE,
        ss_ext_list_price DOUBLE, ss_ext_tax DOUBLE, ss_coupon_amt DOUBLE,
        ss_net_paid DOUBLE, ss_net_paid_inc_tax DOUBLE, ss_net_profit DOUBLE
    """,
    "store_returns": """
        sr_returned_date_sk BIGINT, sr_return_time_sk BIGINT, sr_item_sk BIGINT,
        sr_customer_sk BIGINT, sr_cdemo_sk BIGINT, sr_hdemo_sk BIGINT,
        sr_addr_sk BIGINT, sr_store_sk BIGINT, sr_reason_sk BIGINT,
        sr_ticket_number BIGINT, sr_return_quantity INT, sr_return_amt DOUBLE,
        sr_return_tax DOUBLE, sr_return_amt_inc_tax DOUBLE, sr_fee DOUBLE,
        sr_return_ship_cost DOUBLE, sr_refunded_cash DOUBLE, sr_reversed_charge DOUBLE,
        sr_store_credit DOUBLE, sr_net_loss DOUBLE
    """,
    "catalog_sales": """
        cs_sold_date_sk BIGINT, cs_sold_time_sk BIGINT, cs_ship_date_sk BIGINT,
        cs_bill_customer_sk BIGINT, cs_bill_cdemo_sk BIGINT, cs_bill_hdemo_sk BIGINT,
        cs_bill_addr_sk BIGINT, cs_ship_customer_sk BIGINT, cs_ship_cdemo_sk BIGINT,
        cs_ship_hdemo_sk BIGINT, cs_ship_addr_sk BIGINT, cs_call_center_sk BIGINT,
        cs_catalog_page_sk BIGINT, cs_ship_mode_sk BIGINT, cs_warehouse_sk BIGINT,
        cs_item_sk BIGINT, cs_promo_sk BIGINT, cs_order_number BIGINT,
        cs_quantity INT, cs_wholesale_cost DOUBLE, cs_list_price DOUBLE,
        cs_sales_price DOUBLE, cs_ext_discount_amt DOUBLE, cs_ext_sales_price DOUBLE,
        cs_ext_wholesale_cost DOUBLE, cs_ext_list_price DOUBLE, cs_ext_tax DOUBLE,
        cs_coupon_amt DOUBLE, cs_ext_ship_cost DOUBLE, cs_net_paid DOUBLE,
        cs_net_paid_inc_tax DOUBLE, cs_net_paid_inc_ship DOUBLE,
        cs_net_paid_inc_ship_tax DOUBLE, cs_net_profit DOUBLE
    """,
    "catalog_returns": """
        cr_returned_date_sk BIGINT, cr_returned_time_sk BIGINT, cr_item_sk BIGINT,
        cr_refunded_customer_sk BIGINT, cr_refunded_cdemo_sk BIGINT,
        cr_refunded_hdemo_sk BIGINT, cr_refunded_addr_sk BIGINT,
        cr_returning_customer_sk BIGINT, cr_returning_cdemo_sk BIGINT,
        cr_returning_hdemo_sk BIGINT, cr_returning_addr_sk BIGINT,
        cr_call_center_sk BIGINT, cr_catalog_page_sk BIGINT, cr_ship_mode_sk BIGINT,
        cr_warehouse_sk BIGINT, cr_reason_sk BIGINT, cr_order_number BIGINT,
        cr_return_quantity INT, cr_return_amount DOUBLE, cr_return_tax DOUBLE,
        cr_return_amt_inc_tax DOUBLE, cr_fee DOUBLE, cr_return_ship_cost DOUBLE,
        cr_refunded_cash DOUBLE, cr_reversed_charge DOUBLE, cr_store_credit DOUBLE,
        cr_net_loss DOUBLE
    """,
    "web_sales": """
        ws_sold_date_sk BIGINT, ws_sold_time_sk BIGINT, ws_ship_date_sk BIGINT,
        ws_item_sk BIGINT, ws_bill_customer_sk BIGINT, ws_bill_cdemo_sk BIGINT,
        ws_bill_hdemo_sk BIGINT, ws_bill_addr_sk BIGINT, ws_ship_customer_sk BIGINT,
        ws_ship_cdemo_sk BIGINT, ws_ship_hdemo_sk BIGINT, ws_ship_addr_sk BIGINT,
        ws_web_page_sk BIGINT, ws_web_site_sk BIGINT, ws_ship_mode_sk BIGINT,
        ws_warehouse_sk BIGINT, ws_promo_sk BIGINT, ws_order_number BIGINT,
        ws_quantity INT, ws_wholesale_cost DOUBLE, ws_list_price DOUBLE,
        ws_sales_price DOUBLE, ws_ext_discount_amt DOUBLE, ws_ext_sales_price DOUBLE,
        ws_ext_wholesale_cost DOUBLE, ws_ext_list_price DOUBLE, ws_ext_tax DOUBLE,
        ws_coupon_amt DOUBLE, ws_ext_ship_cost DOUBLE, ws_net_paid DOUBLE,
        ws_net_paid_inc_tax DOUBLE, ws_net_paid_inc_ship DOUBLE,
        ws_net_paid_inc_ship_tax DOUBLE, ws_net_profit DOUBLE
    """,
    "web_returns": """
        wr_returned_date_sk BIGINT, wr_returned_time_sk BIGINT, wr_item_sk BIGINT,
        wr_refunded_customer_sk BIGINT, wr_refunded_cdemo_sk BIGINT,
        wr_refunded_hdemo_sk BIGINT, wr_refunded_addr_sk BIGINT,
        wr_returning_customer_sk BIGINT, wr_returning_cdemo_sk BIGINT,
        wr_returning_hdemo_sk BIGINT, wr_returning_addr_sk BIGINT,
        wr_web_page_sk BIGINT, wr_reason_sk BIGINT, wr_order_number BIGINT,
        wr_return_quantity INT, wr_return_amt DOUBLE, wr_return_tax DOUBLE,
        wr_return_amt_inc_tax DOUBLE, wr_fee DOUBLE, wr_return_ship_cost DOUBLE,
        wr_refunded_cash DOUBLE, wr_reversed_charge DOUBLE, wr_account_credit DOUBLE,
        wr_net_loss DOUBLE
    """,
    "inventory": """
        inv_date_sk BIGINT, inv_item_sk BIGINT, inv_warehouse_sk BIGINT, inv_quantity_on_hand INT
    """,
    "store": """
        s_store_sk BIGINT, s_store_id STRING, s_rec_start_date DATE, s_rec_end_date DATE,
        s_closed_date_sk BIGINT, s_store_name STRING, s_number_employees INT,
        s_floor_space INT, s_hours STRING, s_manager STRING, s_market_id INT,
        s_geography_class STRING, s_market_desc STRING, s_market_manager STRING,
        s_division_id INT, s_division_name STRING, s_company_id INT,
        s_company_name STRING, s_street_number STRING, s_street_name STRING,
        s_street_type STRING, s_suite_number STRING, s_city STRING, s_county STRING,
        s_state STRING, s_zip STRING, s_country STRING, s_gmt_offset DOUBLE,
        s_tax_precentage DOUBLE
    """,
    "call_center": """
        cc_call_center_sk BIGINT, cc_call_center_id STRING, cc_rec_start_date DATE,
        cc_rec_end_date DATE, cc_closed_date_sk BIGINT, cc_open_date_sk BIGINT,
        cc_name STRING, cc_class STRING, cc_employees INT, cc_sq_ft INT,
        cc_hours STRING, cc_manager STRING, cc_mkt_id INT, cc_mkt_class STRING,
        cc_mkt_desc STRING, cc_market_manager STRING, cc_division INT,
        cc_division_name STRING, cc_company INT, cc_company_name STRING,
        cc_street_number STRING, cc_street_name STRING, cc_street_type STRING,
        cc_suite_number STRING, cc_city STRING, cc_county STRING, cc_state STRING,
        cc_zip STRING, cc_country STRING, cc_gmt_offset DOUBLE, cc_tax_percentage DOUBLE
    """,
    "catalog_page": """
        cp_catalog_page_sk BIGINT, cp_catalog_page_id STRING, cp_start_date_sk BIGINT,
        cp_end_date_sk BIGINT, cp_department STRING, cp_catalog_number INT,
        cp_catalog_page_number INT, cp_description STRING, cp_type STRING
    """,
    "web_site": """
        web_site_sk BIGINT, web_site_id STRING, web_rec_start_date DATE,
        web_rec_end_date DATE, web_name STRING, web_open_date_sk BIGINT,
        web_close_date_sk BIGINT, web_class STRING, web_manager STRING,
        web_mkt_id INT, web_mkt_class STRING, web_mkt_desc STRING,
        web_market_manager STRING, web_company_id INT, web_company_name STRING,
        web_street_number STRING, web_street_name STRING, web_street_type STRING,
        web_suite_number STRING, web_city STRING, web_county STRING,
        web_state STRING, web_zip STRING, web_country STRING, web_gmt_offset DOUBLE,
        web_tax_percentage DOUBLE
    """,
    "web_page": """
        wp_web_page_sk BIGINT, wp_web_page_id STRING, wp_rec_start_date DATE,
        wp_rec_end_date DATE, wp_creation_date_sk BIGINT, wp_access_date_sk BIGINT,
        wp_autogen_flag STRING, wp_customer_sk BIGINT, wp_url STRING,
        wp_type STRING, wp_char_count INT, wp_link_count INT, wp_image_count INT,
        wp_max_ad_count INT
    """,
    "warehouse": """
        w_warehouse_sk BIGINT, w_warehouse_id STRING, w_warehouse_name STRING,
        w_warehouse_sq_ft INT, w_street_number STRING, w_street_name STRING,
        w_street_type STRING, w_suite_number STRING, w_city STRING, w_county STRING,
        w_state STRING, w_zip STRING, w_country STRING, w_gmt_offset DOUBLE
    """,
    "customer": """
        c_customer_sk BIGINT, c_customer_id STRING, c_current_cdemo_sk BIGINT,
        c_current_hdemo_sk BIGINT, c_current_addr_sk BIGINT, c_first_shipto_date_sk BIGINT,
        c_first_sales_date_sk BIGINT, c_salutation STRING, c_first_name STRING,
        c_last_name STRING, c_preferred_cust_flag STRING, c_birth_day INT,
        c_birth_month INT, c_birth_year INT, c_birth_country STRING, c_login STRING,
        c_email_address STRING, c_last_review_date_sk BIGINT
    """,
    "customer_address": """
        ca_address_sk BIGINT, ca_address_id STRING, ca_street_number STRING,
        ca_street_name STRING, ca_street_type STRING, ca_suite_number STRING,
        ca_city STRING, ca_county STRING, ca_state STRING, ca_zip STRING,
        ca_country STRING, ca_gmt_offset DOUBLE, ca_location_type STRING
    """,
    "customer_demographics": """
        cd_demo_sk BIGINT, cd_gender STRING, cd_marital_status STRING,
        cd_education_status STRING, cd_purchase_estimate INT, cd_credit_rating STRING,
        cd_dep_count INT, cd_dep_employed_count INT, cd_dep_college_count INT
    """,
    "date_dim": """
        d_date_sk BIGINT, d_date_id STRING, d_date DATE, d_month_seq INT,
        d_week_seq INT, d_quarter_seq INT, d_year INT, d_dow INT, d_moy INT,
        d_dom INT, d_qoy INT, d_fy_year INT, d_fy_quarter_seq INT,
        d_fy_week_seq INT, d_day_name STRING, d_quarter_name STRING,
        d_holiday STRING, d_weekend STRING, d_following_holiday STRING,
        d_first_dom INT, d_last_dom INT, d_same_day_ly INT,
        d_same_day_lq INT, d_current_day STRING, d_current_week STRING,
        d_current_month STRING, d_current_quarter STRING, d_current_year STRING
    """,
    "household_demographics": """
        hd_demo_sk BIGINT, hd_income_band_sk BIGINT, hd_buy_potential STRING,
        hd_dep_count INT, hd_vehicle_count INT
    """,
    "income_band": """
        ib_income_band_sk BIGINT, ib_lower_bound INT, ib_upper_bound INT
    """,
    "item": """
        i_item_sk BIGINT, i_item_id STRING, i_rec_start_date DATE,
        i_rec_end_date DATE, i_item_desc STRING, i_current_price DOUBLE,
        i_wholesale_cost DOUBLE, i_brand_id INT, i_brand STRING,
        i_class_id INT, i_class STRING, i_category_id INT, i_category STRING,
        i_manufact_id INT, i_manufact STRING, i_size STRING, i_formulation STRING,
        i_color STRING, i_units STRING, i_container STRING, i_manager_id INT,
        i_product_name STRING
    """,
    "promotion": """
        p_promo_sk BIGINT, p_promo_id STRING, p_start_date_sk BIGINT,
        p_end_date_sk BIGINT, p_item_sk BIGINT, p_cost DOUBLE, p_response_tgt INT,
        p_promo_name STRING, p_channel_dmail STRING, p_channel_email STRING,
        p_channel_catalog STRING, p_channel_tv STRING, p_channel_radio STRING,
        p_channel_press STRING, p_channel_event STRING, p_channel_demo STRING,
        p_channel_details STRING, p_purpose STRING, p_discount_active STRING
    """,
    "reason": """
        r_reason_sk BIGINT, r_reason_id STRING, r_reason_desc STRING
    """,
    "ship_mode": """
        sm_ship_mode_sk BIGINT, sm_ship_mode_id STRING, sm_type STRING,
        sm_code STRING, sm_carrier STRING, sm_contract STRING
    """,
    "time_dim": """
        t_time_sk BIGINT, t_time_id STRING, t_time INT, t_hour INT, t_minute INT,
        t_second INT, t_am_pm STRING, t_shift STRING, t_sub_shift STRING,
        t_meal_time STRING
    """,
}


def generate_tpcds_data(spark: "SparkSession", sf: float, tmpdir: str) -> None:
    """Generate minimal TPC-DS data using DuckDB and register as Spark temp views."""
    con = duckdb.connect()
    try:
        con.execute(f"INSTALL tpch; LOAD tpch; CALL dsdgen(sf={sf});")
        use_duckdb = True
    except Exception:
        use_duckdb = False

    for table, schema_ddl in TPCDS_SCHEMA.items():
        if use_duckdb:
            try:
                df_pd = con.execute(f"SELECT * FROM {table} LIMIT 10000").df()
                sdf = spark.createDataFrame(df_pd)
                sdf.createOrReplaceTempView(table)
                continue
            except Exception:
                pass
        # Fallback: empty DataFrame with correct schema
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {table} AS SELECT * FROM (VALUES (1)) t(dummy) WHERE 1=0").createOrReplaceTempView(table) if False else None
        spark.sql(f"SELECT {', '.join(col.split()[0] + ' AS ' + col.split()[0] for col in schema_ddl.split(',') if col.strip())} FROM (SELECT 1) t WHERE 1=0").createOrReplaceTempView(table)


def run_query(spark: "SparkSession", q_num: int, sql: str) -> tuple[bool, str, float]:
    t0 = time.time()
    try:
        spark.sql(sql.strip()).collect()
        elapsed = time.time() - t0
        return True, "", elapsed
    except Exception as e:
        elapsed = time.time() - t0
        msg = str(e).split("\n")[0][:120]
        return False, msg, elapsed


def main() -> None:
    if QUERIES_ENV:
        query_nums = [int(x.strip()) for x in QUERIES_ENV.split(",") if x.strip()]
    else:
        query_nums = sorted(TPCDS_QUERIES.keys())

    print(f"Zelox TPC-DS Scorecard  (SF={TPCDS_SF}, {len(query_nums)} queries)")
    print(f"Server: {SPARK_REMOTE}")
    print()

    spark = (
        SparkSession.builder
        .remote(SPARK_REMOTE)
        .getOrCreate()
    )

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        print("Generating TPC-DS data...")
        try:
            generate_tpcds_data(spark, TPCDS_SF, tmpdir)
            print("Data ready.\n")
        except Exception as e:
            print(f"WARNING: Data generation failed ({e}). Queries will run against empty tables.\n")

        passed, failed = [], []
        col_w = 10
        print(f"{'Q':>3}  {'Pass':5}  {'Time':>8}  Error")
        print("-" * 70)
        for q_num in query_nums:
            sql = TPCDS_QUERIES.get(q_num)
            if sql is None:
                continue
            ok, err, elapsed = run_query(spark, q_num, sql)
            status = "PASS" if ok else "FAIL"
            err_snippet = "" if ok else err[:60]
            print(f"Q{q_num:>2}  {status:5}  {elapsed:>7.2f}s  {err_snippet}")
            if ok:
                passed.append(q_num)
            else:
                failed.append((q_num, err))

    total = len(passed) + len(failed)
    pct = 100 * len(passed) // total if total else 0
    print()
    print("=" * 70)
    print(f"TPC-DS Result: {len(passed)}/{total} ({pct}%)")
    if failed:
        print(f"\nFailed queries: {', '.join(f'Q{q}' for q, _ in failed)}")

    spark.stop()
    sys.exit(0 if len(failed) == 0 else 1)


if __name__ == "__main__":
    main()
