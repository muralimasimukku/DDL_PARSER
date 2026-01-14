import json
import sqlglot
from sqlglot.expressions import Select, Table, Column, Join, Subquery


class DDLMetadataParser:
    def __init__(self, ddl: str):
        self.ddl = ddl
        self.ast = sqlglot.parse_one(ddl)

    # ---------------------------------------------------
    # Build alias → table_name or alias → Subquery
    # ---------------------------------------------------
    def build_alias_map(self, select_expr):
        alias_map = {}
        physical_tables = []

        # Physical tables
        for table in select_expr.find_all(Table):
            alias = table.alias_or_name
            table_name = table.this.sql()
            alias_map[alias] = table_name
            physical_tables.append(table_name)

        # Subqueries
        for sub in select_expr.find_all(Subquery):
            alias_map[sub.alias_or_name] = sub

        # Also store physical tables list for heuristics
        alias_map["__physical_tables__"] = physical_tables
        return alias_map

    # ---------------------------------------------------
    # Heuristic: resolve unknown alias when only one table exists
    # ---------------------------------------------------
    def resolve_unknown_alias(self, table_alias, alias_map, col_name):
        physical_tables = alias_map.get("__physical_tables__", [])
        if len(physical_tables) == 1:
            # Assume this unknown alias refers to the only table in scope
            return [f"{physical_tables[0]}.{col_name}"]
        return [f"{table_alias}.{col_name}"]

    # ---------------------------------------------------
    # Resolve alias.column → base table columns (recursive)
    # Returns a list of fully qualified columns
    # ---------------------------------------------------
    def resolve_column(self, col, alias_map):
        table_alias = col.table
        name = col.name

        # Case 1: alias refers to a subquery → recurse into it
        if table_alias in alias_map and isinstance(alias_map[table_alias], Subquery):
            sub = alias_map[table_alias]
            sub_meta = self.extract_select(sub.this)

            # Find matching column inside subquery
            for c in sub_meta["columns"]:
                if c["column_name"] == name:
                    # Propagate its lineage upward
                    return c["lineage"]

            # Fallback if not found
            return [f"{table_alias}.{name}"]

        # Case 2: alias refers to a physical table
        if table_alias in alias_map and not isinstance(alias_map[table_alias], Subquery):
            table_name = alias_map[table_alias]
            return [f"{table_name}.{name}"]

        # Case 3: unknown alias but only one table in scope → heuristic
        if table_alias:
            return self.resolve_unknown_alias(table_alias, alias_map, name)

        # Case 4: no table alias at all
        return [name]

    # ---------------------------------------------------
    # Extract dependencies (tables + columns) for an expression
    # ---------------------------------------------------
    def extract_dependencies(self, expr, alias_map):
        tables = set()
        columns = set()

        for col in expr.find_all(Column):
            resolved_cols = self.resolve_column(col, alias_map)
            for rc in resolved_cols:
                columns.add(rc)
                if "." in rc:
                    tables.add(rc.split(".")[0])

        return {
            "tables": sorted(tables),
            "columns": sorted(columns)
        }

    # ---------------------------------------------------
    # Extract SELECT metadata (recursive for subqueries)
    # ---------------------------------------------------
    def extract_select(self, select_expr):
        alias_map = self.build_alias_map(select_expr)
        columns_meta = []

        for proj in select_expr.expressions:
            alias = proj.alias
            expr = proj.this

            if isinstance(expr, Column):
                column_name = alias or expr.name
                formula = expr.sql()
                deps = self.extract_dependencies(expr, alias_map)
                lineage = deps["columns"]
                base_table = lineage[0].split(".")[0] if lineage else None
            else:
                column_name = alias or proj.sql()
                formula = expr.sql() if expr else proj.sql()
                deps = self.extract_dependencies(expr, alias_map)
                lineage = deps["columns"]
                base_table = None

            columns_meta.append({
                "column_name": column_name,
                "base_table": base_table,
                "formula": formula,
                "lineage": lineage,
                "dependencies": deps
            })

        return {"columns": columns_meta}

    # ---------------------------------------------------
    # Main extract()
    # ---------------------------------------------------
    def extract(self):
        metadata = {
            "view_name": str(self.ast.args.get("this")) if self.ast.args.get("this") else None,
            "columns": [],
            "tables": [],
            "joins": [],
            "filters": []
        }

        select_expr = self.ast.args.get("expression")
        if not isinstance(select_expr, Select):
            return metadata

        # Columns with full lineage
        select_meta = self.extract_select(select_expr)
        metadata["columns"] = select_meta["columns"]

        # Tables
        for table in select_expr.find_all(Table):
            metadata["tables"].append({
                "table_name": table.this.sql(),
                "alias": table.alias_or_name
            })

        # Joins
        for join in select_expr.find_all(Join):
            metadata["joins"].append({
                "type": join.args.get("kind"),
                "table": join.this.sql(),
                "on": join.args.get("on").sql() if join.args.get("on") else None
            })

        # Filters
        where_expr = select_expr.args.get("where")
        if where_expr:
            metadata["filters"].append(where_expr.sql())

        return metadata

    def print_json(self):
        print(json.dumps(self.extract(), indent=4))


if __name__ == "__main__":
    ddl = """
    CREATE VIEW sales_summary AS
    SELECT 
        co.customer_id,
        co.customer_name,
        co.order_id,
        co.order_date,
        SUM(oi.quantity * oi.unit_price) AS total_amount
    FROM (
        SELECT 
            c.customer_id,
            c.customer_name,
            ro.order_id,
            ro.order_date
        FROM customers AS c
        JOIN (
            SELECT co.order_id, co.customer_id, co.order_date
            FROM orders
            WHERE co.order_date >= '2024-01-01'
        ) AS ro ON c.customer_id = ro.customer_id
    ) AS co
    JOIN order_items AS oi ON co.order_id = oi.order_id
    GROUP BY co.customer_id, co.customer_name, co.order_id, co.order_date
    """

    parser = DDLMetadataParser(ddl)
    parser.print_json()
