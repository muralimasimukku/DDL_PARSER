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

def is_noncolumn_identifier(node: exp.Expression) -> bool:
    """
    Identifiers inside functions or special constructs should not be treated as columns.
    """
    if not isinstance(node, exp.Identifier):
        return False

    parent = node.parent

    # DATEADD(day, ...)
    if isinstance(parent, exp.Func):
        return True

    # INTERVAL day
    if isinstance(parent, exp.Interval):
        return True

    # EXTRACT(year FROM ...)
    if isinstance(parent, exp.Extract):
        return True

    return False



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
    """
    Guess the closest table for an unqualified column.
    Strategy:
      - If only one table is in scope, use it.
      - Otherwise pick the first table in alias_to_table (closest in FROM order).
    """
    if not alias_to_table:
        return "<unknown>"

    if len(alias_to_table) == 1:
        return list(alias_to_table.values())[0]

    # Pick the first table in FROM/JOIN order
    return next(iter(alias_to_table.values()))


def resolve_column_lineage(expr: exp.Expression,
                           alias_to_table: Dict[str, str],
                           ctes: Dict[str, exp.Select],
                           subqueries: Dict[str, exp.Select]) -> Set[Tuple[str, str]]:
    """Recursively resolve a column or computed expression to base table columns."""
    lineage = set()

    for col in expr.find_all(exp.Column):
        if is_noncolumn_identifier(col):
            continue
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

def extract_full_lineage_grouped_with_view(sql: str, dialect: str = "mysql") -> Dict[str, Dict[str, List[str]]]:
    """Return lineage grouped by base table, with view name as top-level key."""
    parsed = sqlglot.parse_one(sql, read=dialect)

    # Handle CREATE VIEW properly
    if isinstance(parsed, exp.Create) and parsed.args.get("kind") == "VIEW":
        view_name = parsed.this.name
        select_expr = find_select_node(parsed.expression)  # Correct: use .expression for SELECT
    else:
        view_name = "unknown_view"
        select_expr = find_select_node(parsed)

    if not select_expr:
        raise ValueError("No SELECT statement found in SQL.")

    ctes = extract_ctes(parsed)
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

    return {view_name: {tbl: sorted(cols) for tbl, cols in table_column_map.items()}}

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

    grouped_lineage = extract_full_lineage_grouped_with_view(view_sql)
    print(json.dumps(grouped_lineage, indent=4))
