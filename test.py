import json
from ddl_lineage_engine import *

sql = """
    CREATE VIEW [GCF].[sales_summary] AS
    WITH recent_orders AS (
        SELECT order_id, customer_id, order_date
        FROM orders
        WHERE order_date >= '2024-01-01'
    ),
    customer_orders AS (
        SELECT c.customer_id, c.customer_name, ro.order_id, ro.order_date
        FROM customers c
        JOIN recent_orders ro ON c.customer_id = ro.customer_id
    )
    SELECT 
        co.customer_id AS [Customer ID],
        co.customer_name,
        co.order_id,
        co.order_date,
        SUM(oi.quantity * oi.unit_price) AS total_amount,
        DATEDIFF(mi, co.order_date, GETDATE()) AS DaysDifferenceInMins
    FROM customer_orders co
    JOIN order_items oi ON co.order_id = oi.order_id
    GROUP BY co.customer_id, co.customer_name, co.order_id, co.order_date;
    """
engine = NormalizationLineageEngine(DDLNormalizer(), LineageParser())
result = engine.process(sql)
print(json.dumps(result, indent=4))