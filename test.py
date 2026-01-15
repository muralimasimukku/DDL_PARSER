import normalize_view_ddl
import parse_view_ddl

view_sql = """CREATE VIEW sales_summary AS
    SELECT 
        co.customer_id,
        co.customer_name,
        co.order_id,
        co.order_date,
        SUM(oi.quantity * oi.unit_price) AS total_amount
    FROM (SELECT c.customer_id, c.customer_name, ro.order_id, ro.order_date
        FROM customers c
        JOIN (SELECT order_id, customer_id, order_date
        FROM orders
        WHERE order_date >= '2024-01-01') ro ON c.customer_id = ro.customer_id) co
    JOIN order_items oi ON co.order_id = oi.order_id
    GROUP BY co.customer_id, co.customer_name, co.order_id, co.order_date;
    """
parser = parse_view_ddl.DDLMetadataParser(view_sql)
print(parser.print_json())

