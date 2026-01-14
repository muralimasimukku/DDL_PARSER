import sqlglot
from sqlglot import exp
from typing import Dict, List, Set, Tuple
import json

def find_select_node(expr: exp.Expression) -> exp.Select:
    """Recursively find the first SELECT node inside the parsed SQL."""
    if isinstance(expr, exp.Select):
        return expr
    for arg in expr.args.values():
        if isinstance(arg, exp.Expression):
            found = find_select_node(arg)
            if found:
                return found
        elif isinstance(arg, list):
            for sub in arg:
                if isinstance(sub, exp.Expression):
                    found = find_select_node(sub)
                    if found:
                        return found
    return None

def build_alias_to_table_map(select_expr: exp.Select) -> Dict[str, str]:
    """Build a mapping of table aliases to real table names for a SELECT."""
    alias_map = {}
    for table_expr in select_expr.find_all(exp.Table):
        alias = table_expr.alias_or_name
        table_name = table_expr.name
        alias_map[alias] = table_name
    return alias_map

def extract_ctes(expr: exp.Expression) -> Dict[str, exp.Select]:
    """Extract CTE name → SELECT expression mapping."""
    ctes = {}
    for cte in expr.find_all(exp.CTE):
        cte_name = cte.alias_or_name
        cte_select = find_select_node(cte.this)
        if cte_select:
            ctes[cte_name] = cte_select
    return ctes

def extract_subqueries(select_expr: exp.Select) -> Dict[str, exp.Select]:
    """Extract subquery alias → SELECT expression mapping from FROM/JOIN."""
    subqueries = {}
    for subquery in select_expr.find_all(exp.Subquery):
        alias = subquery.alias_or_name
        sub_select = find_select_node(subquery)
        if alias and sub_select:
            subqueries[alias] = sub_select
    return subqueries

def guess_table_for_unqualified(col_name: str, alias_to_table: Dict[str, str]) -> str:
    """Guess table for unqualified column."""
    if len(alias_to_table) == 1:
        return list(alias_to_table.values())[0]
    return "<ambiguous>"

def resolve_column_lineage(expr: exp.Expression,
                           alias_to_table: Dict[str, str],
                           ctes: Dict[str, exp.Select],
                           subqueries: Dict[str, exp.Select]) -> Set[Tuple[str, str]]:
    """Recursively resolve a column or computed expression to base table columns."""
    lineage = set()

    for col in expr.find_all(exp.Column):
        table_alias = col.table
        col_name = col.name

        if table_alias:
            table_name = alias_to_table.get(table_alias, table_alias)
        else:
            table_name = guess_table_for_unqualified(col_name, alias_to_table)

        if table_name in ctes:
            cte_select = ctes[table_name]
            cte_alias_map = build_alias_to_table_map(cte_select)
            cte_subqueries = extract_subqueries(cte_select)

            matched_proj = None
            for proj in cte_select.expressions:
                if isinstance(proj, exp.Alias) and proj.alias == col_name:
                    matched_proj = proj.this
                    break
                elif proj.name == col_name:
                    matched_proj = proj
                    break

            if matched_proj:
                lineage |= resolve_column_lineage(matched_proj, cte_alias_map, ctes, cte_subqueries)
            else:
                lineage.add((table_name, col_name))

        elif table_name in subqueries:
            sub_select = subqueries[table_name]
            sub_alias_map = build_alias_to_table_map(sub_select)
            sub_subqueries = extract_subqueries(sub_select)

            matched_proj = None
            for proj in sub_select.expressions:
                if isinstance(proj, exp.Alias) and proj.alias == col_name:
                    matched_proj = proj.this
                    break
                elif proj.name == col_name:
                    matched_proj = proj
                    break

            if matched_proj:
                lineage |= resolve_column_lineage(matched_proj, sub_alias_map, ctes, sub_subqueries)
            else:
                lineage.add((table_name, col_name))

        else:
            lineage.add((table_name, col_name))

    return lineage

def extract_full_lineage_grouped(sql: str, dialect: str = "mysql") -> Dict[str, List[str]]:
    """Return lineage grouped by base table in JSON-friendly format."""
    parsed = sqlglot.parse_one(sql, read=dialect)
    ctes = extract_ctes(parsed)
    select_expr = find_select_node(parsed)
    if not select_expr:
        raise ValueError("No SELECT statement found in SQL.")

    alias_to_table = build_alias_to_table_map(select_expr)
    subqueries = extract_subqueries(select_expr)

    table_column_map: Dict[str, Set[str]] = {}

    for projection in select_expr.expressions:
        if isinstance(projection, exp.Alias):
            source_expr = projection.this
        else:
            source_expr = projection

        lineage = resolve_column_lineage(source_expr, alias_to_table, ctes, subqueries)
        for tbl, col in lineage:
            if tbl not in table_column_map:
                table_column_map[tbl] = set()
            table_column_map[tbl].add(col)

    # Convert sets to sorted lists for JSON output
    return {tbl: sorted(cols) for tbl, cols in table_column_map.items()}

if __name__ == "__main__":
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

    grouped_lineage = extract_full_lineage_grouped(view_sql)
    print(json.dumps(grouped_lineage, indent=4))
