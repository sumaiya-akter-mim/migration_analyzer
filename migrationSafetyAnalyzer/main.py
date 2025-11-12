import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

# ------------- Read sql files -------------
SQL_FOLDER = "../migrations/"   # <-- CHANGE THIS TO YOUR FOLDER

def read_sql_files(folder: str):
    """Return a list of (filename, contents) for all .sql files in a folder."""
    files = []
    for name in sorted(os.listdir(folder)):
        if name.lower().endswith(".sql"):
            full = os.path.join(folder, name)
            with open(full, "r", encoding="utf-8") as f:
                files.append((name, f.read()))
    return files

# ------------- AST definitions -------------

class DDLNode:
    raw_sql: str

@dataclass
class ColumnDef:
    name: str
    col_type: str
    nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False

@dataclass
class CreateTable(DDLNode):
    table_name: str
    columns: List[ColumnDef]
    table_primary_key: List[str]
    raw_sql: str = ""

@dataclass
class AlterAction:
    pass

@dataclass
class DropColumn(AlterAction):
    column_name: str

@dataclass
class AddColumn(AlterAction):
    column: ColumnDef

@dataclass
class SetNotNull(AlterAction):
    column_name: str

@dataclass
class AlterTable(DDLNode):
    table_name: str
    actions: List[AlterAction]
    raw_sql: str = ""

@dataclass
class DropTable(DDLNode):
    table_name: str
    raw_sql: str = ""


# ------------- Analyzer using AST -------------

class DDLAnalyzer:
    def __init__(self):
        self.tables: Dict[str, Any] = {}
        self.issues: List[str] = []
        self.ast: List[DDLNode] = []

    # ---------- Public API ----------

    def parse_ddl(self, ddl_script: str) -> Dict[str, Any]:
        print("\n[DEBUG] Starting DDL analysis...\n")
        statements = self._split_statements(ddl_script)

        for statement in statements:
            print(f"[DEBUG] Parsing statement:\n  {statement}\n")
            node = self._build_ast(statement)
            print(f"[DEBUG] → Built AST node: {type(node).__name__ if node else 'None'}\n")

            if not node:
                continue

            self.ast.append(node)
            self._analyze_node(node)

        return {
            "tables": self.tables,
            "issues": self.issues,
            "summary": self._generate_summary(),
            "ast": self.ast,
        }

    # ---------- Splitting & routing ----------

    def _split_statements(self, ddl: str) -> List[str]:
        ddl_clean = re.sub(r"--.*$", "", ddl, flags=re.MULTILINE)
        ddl_clean = re.sub(r"/\*.*?\*/", "", ddl_clean, flags=re.DOTALL)
        statements = [stmt.strip() for stmt in ddl_clean.split(";") if stmt.strip()]
        print(f"[DEBUG] Split into {len(statements)} statements\n")
        return statements

    def _build_ast(self, statement: str) -> Optional[DDLNode]:
        upper = statement.upper()

        if upper.startswith("CREATE TABLE"):
            return self._build_create_table_ast(statement)
        elif upper.startswith("ALTER TABLE"):
            return self._build_alter_table_ast(statement)
        elif upper.startswith("DROP TABLE"):
            return self._build_drop_table_ast(statement)
        else:
            print("[DEBUG] Unsupported statement type\n")
            return None

    # ---------- AST builders ----------

    def _build_create_table_ast(self, statement: str) -> Optional[CreateTable]:
        table_match = re.search(r"CREATE\s+TABLE\s+(\w+)", statement, re.IGNORECASE)
        if not table_match:
            return None

        table_name = table_match.group(1)
        print(f"[DEBUG]   CREATE TABLE detected for '{table_name}'")

        m_body = re.search(r"\((.*)\)", statement, re.DOTALL)
        parts = [p.strip() for p in m_body.group(1).split(",")] if m_body else []

        columns = []
        table_pk = []

        for part in parts:
            upper = part.upper()

            if upper.startswith("PRIMARY KEY"):
                pk_cols = re.findall(r"\(([^)]+)\)", part)
                if pk_cols:
                    table_pk.extend([c.strip() for c in pk_cols[0].split(",")])
                continue

            tokens = part.split()
            if len(tokens) < 2:
                continue

            col_name = tokens[0]
            col_type = tokens[1]

            nullable = "NOT NULL" not in upper
            default = None
            is_pk = "PRIMARY KEY" in upper

            columns.append(
                ColumnDef(
                    name=col_name,
                    col_type=col_type,
                    nullable=nullable,
                    default=default,
                    is_primary_key=is_pk,
                )
            )

        return CreateTable(
            table_name=table_name, columns=columns, table_primary_key=table_pk, raw_sql=statement
        )

    def _build_alter_table_ast(self, statement: str) -> Optional[AlterTable]:
        m_table = re.search(r"ALTER\s+TABLE\s+(\w+)", statement, re.IGNORECASE)
        if not m_table:
            return None

        table_name = m_table.group(1)
        print(f"[DEBUG]   ALTER TABLE detected for '{table_name}'")

        rest = statement[m_table.end():].strip()
        action_parts = [a.strip() for a in rest.split(",")]

        actions = []

        for act in action_parts:
            upper = act.upper()

            if "DROP COLUMN" in upper:
                col = re.search(r"DROP\s+COLUMN\s+(\w+)", act, re.IGNORECASE).group(1)
                actions.append(DropColumn(column_name=col))

            elif upper.startswith("ADD COLUMN"):
                col_def = act.split(None, 2)[2]
                tokens = col_def.split()
                col_name = tokens[0]
                col_type = tokens[1]
                nullable = "NOT NULL" not in upper
                actions.append(AddColumn(ColumnDef(col_name, col_type, nullable)))

            elif "SET NOT NULL" in upper:
                col = re.search(r"ALTER\s+COLUMN\s+(\w+)", act, re.IGNORECASE).group(1)
                actions.append(SetNotNull(column_name=col))

        return AlterTable(table_name=table_name, actions=actions, raw_sql=statement)

    def _build_drop_table_ast(self, statement: str) -> Optional[DropTable]:
        table_name = re.search(r"DROP\s+TABLE\s+(\w+)", statement, re.IGNORECASE).group(1)
        print(f"[DEBUG]   DROP TABLE detected for '{table_name}'")
        return DropTable(table_name=table_name, raw_sql=statement)

    # ---------- AST-based analysis ----------

    def _analyze_node(self, node: DDLNode):
        print(f"[DEBUG] Analyzing AST node: {type(node).__name__}")

        if isinstance(node, CreateTable):
            self._analyze_create(node)
        elif isinstance(node, AlterTable):
            self._analyze_alter(node)
        elif isinstance(node, DropTable):
            self._analyze_drop(node)

        print("[DEBUG] Current schema state:", self.tables, "\n")

    def _analyze_create(self, node: CreateTable):
        table = node.table_name
        print(f"[DEBUG]   Handling CREATE TABLE for '{table}'")

        self.tables[table] = {"columns": {}, "primary_key": []}

        for col in node.columns:
            self.tables[table]["columns"][col.name] = {
                "type": col.col_type,
                "nullable": col.nullable,
            }

        # Primary key rule
        pk = node.table_primary_key + [c.name for c in node.columns if c.is_primary_key]
        self.tables[table]["primary_key"] = pk

        if not pk:
            issue = f"Table '{table}' missing PRIMARY KEY"
            print("[DEBUG]   ISSUE:", issue)
            self.issues.append(issue)

    def _analyze_alter(self, node: AlterTable):
        table = node.table_name
        print(f"[DEBUG]   Handling ALTER TABLE for '{table}'")

        for act in node.actions:
            if isinstance(act, DropColumn):
                issue = f"Potential data loss: DROP COLUMN '{act.column_name}' on '{table}'"
                print("[DEBUG]   ISSUE:", issue)
                self.issues.append(issue)

                self.tables[table]["columns"].pop(act.column_name, None)

            elif isinstance(act, AddColumn):
                col = act.column
                self.tables[table]["columns"][col.name] = {
                    "type": col.col_type,
                    "nullable": col.nullable,
                }

                if not col.nullable:
                    issue = f"Adding NOT NULL column '{col.name}' without default on '{table}'"
                    print("[DEBUG]   ISSUE:", issue)
                    self.issues.append(issue)

            elif isinstance(act, SetNotNull):
                issue = f"Risky: SET NOT NULL on '{table}.{act.column_name}'"
                print("[DEBUG]   ISSUE:", issue)
                self.issues.append(issue)

    def _analyze_drop(self, node: DropTable):
        table = node.table_name
        issue = f"CRITICAL: DROP TABLE '{table}'"
        print("[DEBUG]   ISSUE:", issue)
        self.issues.append(issue)
        self.tables.pop(table, None)

    # ---------- Summary ----------

    def _generate_summary(self) -> Dict[str, Any]:
        return {
            "total_tables": len(self.tables),
            "total_issues": len(self.issues),
            "critical_issues": sum(1 for i in self.issues if "CRITICAL" in i),
        }


# ------------- Example usage -------------

def main():
    analyzer = DDLAnalyzer()   # reuse one analyzer for whole schema

    print("\n=== Scanning SQL folder:", SQL_FOLDER, "===\n")

    sql_files = read_sql_files(SQL_FOLDER)

    if not sql_files:
        print("No .sql files found in folder:", SQL_FOLDER)
        return

    for filename, content in sql_files:
        print("\n" + "=" * 80)
        print(f"Analyzing file: {filename}")
        print("=" * 80)

        result = analyzer.parse_ddl(content)

        print("\n[SUMMARY AFTER THIS FILE]")
        print("  Tables:", result["summary"]["total_tables"])
        print("  Issues:", result["summary"]["total_issues"])
        print("  Critical:", result["summary"]["critical_issues"])
        print()

    print("\n===== FINAL OVERALL ISSUES =====")
    for issue in analyzer.issues:
        print("  -", issue)

if __name__ == "__main__":
    main()
