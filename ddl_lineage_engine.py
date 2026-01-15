import json
import sqlglot
from sqlglot import exp


# ---------------------------------------------------------
# SIMPLE NORMALIZER (replace with your real implementation)
# ---------------------------------------------------------
class DDLNormalizer:
    def normalize(self, sql: str) -> str:
        # Use TSQL dialect if you have brackets; adjust as needed
        ast = sqlglot.parse_one(sql, read="tsql")
        return ast.sql(pretty=True, dialect="tsql")


# ---------------------------------------------------------
# LINEAGE PARSER (recursive, subquery + CTE aware)
# ---------------------------------------------------------
class LineageParser:
    # -----------------------------
    # MAIN SELECT PARSER
    # -----------------------------
    def extract_select(self, select_node: exp.Select, parent_alias_map: dict, cte_map: dict):
        alias_map = self.build_alias_map(select_node, parent_alias_map, cte_map)

        columns = []
        for proj in select_node.expressions:
            col_meta = self.extract_column_metadata(proj, alias_map, cte_map)
            columns.append(col_meta)

        tables = self.extract_tables(select_node)
        joins = self.extract_joins(select_node)
        filters = self.extract_filters(select_node)

        # Make alias_map JSON-safe
        serializable_alias_map = {}
        for alias, target in alias_map.items():
            if isinstance(target, exp.Subquery):
                serializable_alias_map[alias] = {
                    "type": "subquery",
                    "sql": target.sql()
                }
            elif isinstance(target, exp.Select):
                serializable_alias_map[alias] = {
                    "type": "cte",
                    "sql": target.sql()
                }
            else:
                serializable_alias_map[alias] = target

        return {
            "columns": columns,
            "tables": tables,
            "joins": joins,
            "filters": filters,
            "alias_map": serializable_alias_map
        }

    # -----------------------------
    # CTE MAP
    # -----------------------------
    def build_cte_map(self, ast) -> dict:
        """
        Build a map of CTE name -> SELECT expression.
        """
        cte_map = {}
        with_expr = ast.find(exp.With)
        if not with_expr:
            return cte_map

        for cte in with_expr.find_all(exp.CTE):
            name = cte.alias
            query = cte.this
            if isinstance(query, exp.Select) or isinstance(query, exp.Subquery):
                # Store the inner SELECT for lineage
                if isinstance(query, exp.Subquery):
                    query = query.this
                cte_map[name] = query
        return cte_map

    # -----------------------------
    # ALIAS MAP (TABLES, SUBQUERIES, CTEs)
    # -----------------------------
    def build_alias_map(self, select_node: exp.Select, parent_alias_map: dict, cte_map: dict):
        alias_map = dict(parent_alias_map)

        # Physical tables
        for table in select_node.find_all(exp.Table):
            alias_map[table.alias_or_name] = table.name

        # Subqueries
        for subquery in select_node.find_all(exp.Subquery):
            alias = subquery.alias
            if alias:
                alias_map[alias] = subquery

        # CTE references: when a CTE is used like a table, it appears as a Table
        # We resolve them in resolve_column using cte_map, not here.

        return alias_map

    # -----------------------------
    # COLUMN METADATA + LINEAGE
    # -----------------------------
    def extract_column_metadata(self, proj: exp.Expression, alias_map: dict, cte_map: dict):
        if isinstance(proj, exp.Alias):
            name = proj.alias
            expr = proj.this
        else:
            name = proj.sql()
            expr = proj

        lineage = self.resolve_expression(expr, alias_map, cte_map)

        return {
            "column_name": name,
            "expression": expr.sql(),
            "lineage": lineage
        }

    # -----------------------------
    # EXPRESSION RESOLUTION
    # -----------------------------
    def resolve_expression(self, expr: exp.Expression, alias_map: dict, cte_map: dict):
        if isinstance(expr, exp.Column):
            return self.resolve_column(expr, alias_map, cte_map)

        deps = []
        for child in expr.args.values():
            if isinstance(child, exp.Expression):
                deps.extend(self.resolve_expression(child, alias_map, cte_map))
            elif isinstance(child, list):
                for c in child:
                    if isinstance(c, exp.Expression):
                        deps.extend(self.resolve_expression(c, alias_map, cte_map))

        return list(dict.fromkeys(deps))

    # -----------------------------
    # COLUMN RESOLUTION (TABLES, SUBQUERIES, CTEs)
    # -----------------------------
    def resolve_column(self, col: exp.Column, alias_map: dict, cte_map: dict):
        table_alias = col.table
        col_name = col.name

        # Case 1: explicit table alias
        if table_alias:
            target = alias_map.get(table_alias)

            # Subquery → recurse
            if isinstance(target, exp.Subquery):
                sub_select = target.this
                sub_meta = self.extract_select(sub_select, {}, cte_map)
                for c in sub_meta["columns"]:
                    if c["column_name"] == col_name:
                        return c["lineage"]
                return [f"{table_alias}.{col_name}"]

            # CTE reference: alias is actually a CTE name
            if table_alias in cte_map:
                cte_select = cte_map[table_alias]
                sub_meta = self.extract_select(cte_select, {}, cte_map)
                for c in sub_meta["columns"]:
                    if c["column_name"] == col_name:
                        return c["lineage"]
                return [f"{table_alias}.{col_name}"]

            # Physical table
            if isinstance(target, str):
                return [f"{target}.{col_name}"]

        # Case 2: no table alias → search all physical tables
        results = []
        for alias, target in alias_map.items():
            if isinstance(target, str):
                results.append(f"{target}.{col_name}")

        # Also consider CTEs when no explicit table alias
        for cte_name, cte_select in cte_map.items():
            sub_meta = self.extract_select(cte_select, {}, cte_map)
            for c in sub_meta["columns"]:
                if c["column_name"] == col_name:
                    results.extend(c["lineage"])

        return list(dict.fromkeys(results)) or [col_name]

    # -----------------------------
    # TABLES
    # -----------------------------
    def extract_tables(self, select_node: exp.Select):
        tables = []
        for table in select_node.find_all(exp.Table):
            tables.append({
                "table_name": table.name,
                "alias": table.alias_or_name
            })
        return tables

    # -----------------------------
    # JOINS
    # -----------------------------
    def extract_joins(self, select_node: exp.Select):
        joins = []
        for join in select_node.find_all(exp.Join):
            joins.append({
                "type": join.args.get("kind"),
                "table": join.this.sql(),
                "condition": join.args.get("on").sql() if join.args.get("on") else None
            })
        return joins

    # -----------------------------
    # FILTERS (WHERE)
    # -----------------------------
    def extract_filters(self, select_node: exp.Select):
        where = select_node.args.get("where")
        if not where:
            return []
        return [where.this.sql()]


# ---------------------------------------------------------
# NORMALIZATION + LINEAGE ENGINE (with CTE support)
# ---------------------------------------------------------
class NormalizationLineageEngine:
    def __init__(self, normalizer: DDLNormalizer, lineage_parser: LineageParser):
        self.normalizer = normalizer
        self.lineage_parser = lineage_parser

    def extract_view_name(self, ast):
        if not isinstance(ast, exp.Create):
            return None
        this = ast.args.get("this")
        if not this:
            return None
        return this.sql(dialect="tsql")

    def process(self, sql: str) -> dict:
        normalized_sql = self.normalizer.normalize(sql)
        normalized_ast = sqlglot.parse_one(normalized_sql, read="tsql")

        view_name = self.extract_view_name(normalized_ast)

        # Build CTE map from the whole AST
        cte_map = self.lineage_parser.build_cte_map(normalized_ast)

        select_node = normalized_ast.find(exp.Select)
        if not select_node:
            return {
                "view_name": view_name,
                "lineage": None
            }

        lineage = self.lineage_parser.extract_select(select_node, parent_alias_map={}, cte_map=cte_map)

        return {
            "view_name": view_name,
            "lineage": lineage
        }


# ---------------------------------------------------------
# EXAMPLE USAGE WITH CTEs
# ---------------------------------------------------------
if __name__ == "__main__":
    sql = """
    CREATE VIEW sales_summary AS
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
        co.customer_id,
        co.customer_name,
        co.order_id,
        co.order_date,
        SUM(oi.quantity * oi.unit_price) AS total_amount
    FROM customer_orders co
    JOIN order_items oi ON co.order_id = oi.order_id
    GROUP BY co.customer_id, co.customer_name, co.order_id, co.order_date;
    """

    engine = NormalizationLineageEngine(DDLNormalizer(), LineageParser())
    result = engine.process(sql)
    print(json.dumps(result, indent=4))
