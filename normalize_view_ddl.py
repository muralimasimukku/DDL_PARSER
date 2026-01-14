import re
import sqlglot
from sqlglot import exp


class DDLNormalizer:
    def __init__(self, schema=None):
        self.schema = schema

        # Default table + alias (from top-level FROM)
        self.default_table = None
        self.default_alias = None

        # Global aliasing
        self.alias_counter = 1
        self.table_alias_map = {}   # id(table_node) → alias

        # Context flag: are we normalizing a CREATE statement?
        self.in_create = False

    # ---------------------------------------------------------
    # PUBLIC ENTRY POINT
    # ---------------------------------------------------------
    def normalize(self, ddl: str) -> str:
        parsed = sqlglot.parse_one(ddl)

        # Detect CREATE context
        self.in_create = isinstance(parsed, exp.Create)

        # Detect default table BEFORE normalization
        self._detect_default_table(parsed)

        # Normalize AST
        normalized = self._normalize_expression(parsed)

        # Generate SQL
        sql_text = normalized.sql(pretty=False)

        # Minify
        return self._minify_sql(sql_text)

    # ---------------------------------------------------------
    # DETECT DEFAULT TABLE + ALIAS
    # ---------------------------------------------------------
    def _detect_default_table(self, expression):
        from_clause = expression.find(exp.From)
        if not from_clause:
            return

        table_expr = from_clause.this

        # Case 1: FROM users u
        if isinstance(table_expr, exp.Alias):
            self.default_table = table_expr.this.name
            self.default_alias = self._alias_name(table_expr.alias)

        # Case 2: FROM users
        elif isinstance(table_expr, exp.Table):
            self.default_table = table_expr.name
            self.default_alias = (
                self._alias_name(table_expr.alias) or table_expr.name
            )

        # Case 3: FROM (SELECT ...) u
        elif isinstance(table_expr, exp.Subquery):
            alias = table_expr.args.get("alias")
            if alias:
                self.default_alias = self._alias_name(alias)
                self.default_table = self.default_alias

    # ---------------------------------------------------------
    # SAFE ALIAS EXTRACTION
    # ---------------------------------------------------------
    def _alias_name(self, alias):
        if alias is None:
            return None

        if isinstance(alias, str):
            return alias

        if isinstance(alias, exp.Identifier):
            return alias.this

        if isinstance(alias, exp.TableAlias):
            return self._alias_name(alias.this)

        if isinstance(alias, exp.Alias):
            return self._alias_name(alias.alias)

        return str(alias)

    # ---------------------------------------------------------
    # UNIQUE ALIAS GENERATOR
    # ---------------------------------------------------------
    def _next_alias(self):
        alias = f"t{self.alias_counter}"
        self.alias_counter += 1
        return alias

    # ---------------------------------------------------------
    # NORMALIZE AST
    # ---------------------------------------------------------
    def _normalize_expression(self, expression):
        for node in expression.walk():

            if isinstance(node, exp.Table):
                self._normalize_table(node)

            if isinstance(node, exp.Subquery):
                self._normalize_subquery(node)

            if isinstance(node, exp.Join):
                self._normalize_join(node)

            if isinstance(node, exp.Column):
                self._qualify_column(node)

        return expression

    # ---------------------------------------------------------
    # NORMALIZE TABLE NODES
    # ---------------------------------------------------------
    def _normalize_table(self, table_node):
        # Add schema if missing
        if self.schema and not table_node.args.get("db"):
            table_node.set("db", exp.to_identifier(self.schema))

        # Skip aliasing ONLY for the CREATE target table
        if self.in_create and isinstance(table_node.parent, exp.Create):
            return

        # If alias exists → register and return
        if table_node.args.get("alias"):
            alias_name = self._alias_name(table_node.args["alias"])
            self.table_alias_map[id(table_node)] = alias_name
            return

        # Assign new alias
        alias_name = self._next_alias()
        table_node.set("alias", exp.TableAlias(this=exp.to_identifier(alias_name)))
        self.table_alias_map[id(table_node)] = alias_name

    # ---------------------------------------------------------
    # NORMALIZE SUBQUERIES
    # ---------------------------------------------------------
    def _normalize_subquery(self, subq):
        # Skip aliasing only for CREATE target
        if self.in_create and isinstance(subq.parent, exp.Create):
            return

        if subq.args.get("alias"):
            return

        alias_name = self._next_alias()
        subq.set("alias", exp.TableAlias(this=exp.to_identifier(alias_name)))

    # ---------------------------------------------------------
    # NORMALIZE JOIN TARGETS
    # ---------------------------------------------------------
    def _normalize_join(self, join_node):
        target = join_node.this

        # JOIN table
        if isinstance(target, exp.Table):
            self._normalize_table(target)

        # JOIN (SELECT ...)
        elif isinstance(target, exp.Subquery):
            self._normalize_subquery(target)

    # ---------------------------------------------------------
    # QUALIFY COLUMNS
    # ---------------------------------------------------------
    def _qualify_column(self, col_node):
        # Already qualified → do not overwrite
        if col_node.table:
            return

        # Use default alias if available
        if self.default_alias:
            col_node.set("table", exp.to_identifier(self.default_alias))
            return

        # If only one table exists, use its alias
        if len(self.table_alias_map) == 1:
            alias = list(self.table_alias_map.values())[0]
            col_node.set("table", exp.to_identifier(alias))
            return

        # Otherwise leave unqualified (ambiguous)

    # ---------------------------------------------------------
    # MINIFIER
    # ---------------------------------------------------------
    def _minify_sql(self, sql: str) -> str:
        sql = sql.replace("\n", " ").replace("\t", " ")
        sql = re.sub(r"\s+", " ", sql)
        sql = re.sub(r"\s+,", ",", sql)
        sql = re.sub(r",\s+", ",", sql)
        sql = re.sub(r"\(\s+", "(", sql)
        sql = re.sub(r"\s+\)", ")", sql)
        return sql.strip()
