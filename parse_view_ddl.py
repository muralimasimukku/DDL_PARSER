import json
import sqlglot
from sqlglot.expressions import (
    Select, Table, Column, Join, Subquery, CTE, With
)


class DDLMetadataParser:
    def __init__(self, ddl: str):
        self.ddl = ddl
        self.ast = sqlglot.parse_one(ddl)
        self.cte_map = self.build_cte_map()

    # ---------------------------------------------------
    # Build CTE map from WITH clause
    # ---------------------------------------------------
    def build_cte_map(self):
        cte_map = {}
        with_expr = self.ast.args.get("with")

        if isinstance(with_expr, With):
            for cte in with_expr.expressions:
                if isinstance(cte, CTE):
                    name = cte.alias_or_name
                    cte_map[name] = cte.this  # Select inside CTE

        return cte_map

    # ---------------------------------------------------
    # Build alias → table_name or alias → Subquery or alias → CTE Select
    # ---------------------------------------------------
    def build_alias_map(self, select_expr, base_alias_map=None):
        alias_map = dict(base_alias_map or {})
        physical_tables = alias_map.get("__physical_tables__", [])

        # Physical tables
        for table in select_expr.find_all(Table):
            alias = table.alias_or_name
            table_name = table.this.sql()
            alias_map[alias] = table_name
            physical_tables.append(table_name)

        # Subqueries
        for sub in select_expr.find_all(Subquery):
            alias_map[sub.alias_or_name] = sub

        alias_map["__physical_tables__"] = physical_tables
        return alias_map

    # ---------------------------------------------------
    # Heuristic for unknown alias
    # ---------------------------------------------------
    def resolve_unknown_alias(self, table_alias, alias_map, col_name):
        physical_tables = alias_map.get("__physical_tables__", [])
        if len(physical_tables) == 1:
            return [f"{physical_tables[0]}.{col_name}"]
        return [f"{table_alias}.{col_name}"]

    # ---------------------------------------------------
    # Resolve alias.column → base table columns (recursive)
    # ---------------------------------------------------
    def resolve_column(self, col, alias_map):
        table_alias = col.table
        name = col.name

        # Case 0: alias refers to a CTE
        if table_alias in alias_map and alias_map[table_alias] in self.cte_map:
            sub_select = self.cte_map[alias_map[table_alias]]
            sub_meta = self.extract_select(sub_select)
            for c in sub_meta["columns"]:
                if c["column_name"] == name:
                    return c["lineage"]
            return [f"{table_alias}.{name}"]

        # Case 1: alias refers to a subquery
        if table_alias in alias_map and isinstance(alias_map[table_alias], Subquery):
            sub = alias_map[table_alias]
            sub_meta = self.extract_select(sub.this)
            for c in sub_meta["columns"]:
                if c["column_name"] == name:
                    return c["lineage"]
            return [f"{table_alias}.{name}"]

        # Case 2: alias refers to a physical table
        if table_alias in alias_map and not isinstance(alias_map[table_alias], Subquery):
            table_name = alias_map[table_alias]
            return [f"{table_name}.{name}"]

        # Case 3: unknown alias
        if table_alias:
            return self.resolve_unknown_alias(table_alias, alias_map, name)

        # Case 4: no table alias
        return [name]

    # ---------------------------------------------------
    # Extract dependencies (tables + columns)
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
    # Extract SELECT metadata (recursive)
    # ---------------------------------------------------
    def extract_select(self, select_expr):
        alias_map = self.build_alias_map(select_expr, base_alias_map=self.cte_map)
        columns_meta = []

        for proj in select_expr.expressions:
            alias = proj.alias
            expr = proj.this

            deps = self.extract_dependencies(expr, alias_map)
            lineage = deps["columns"]

            # base table always comes from lineage
            base_table = lineage[0].split(".")[0] if lineage and "." in lineage[0] else None

            if isinstance(expr, Column):
                column_name = alias or expr.name
                formula = expr.sql()
            else:
                column_name = alias or proj.sql()
                formula = expr.sql() if expr else proj.sql()

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
        return json.dumps(self.extract(), indent=4)

