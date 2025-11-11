import sqlparse
import re
from typing import Dict, List, Any

class DDLAnalyzer:
    def __init__(self):
        self.tables = {}
        self.issues = []

    def parse_ddl(self, ddl_script: str) -> Dict[str, Any]:  # Fixed: parse_ddl NOT parse_dd1
        """Parse DDL script and return schema information"""
        statements = self._split_statements(ddl_script)

        for statement in statements:
            self._analyze_statement(statement)

        return {
            'tables': self.tables,
            'issues': self.issues,
            'summary': self._generate_summary()
        }

    def _split_statements(self, ddl: str) -> List[str]:
        """Split DDL into individual statements"""
        ddl_clean = re.sub(r'--.*$', '', ddl, flags=re.MULTILINE)
        ddl_clean = re.sub(r'/\*.*?\*/', '', ddl_clean, flags=re.DOTALL)
        return [stmt.strip() for stmt in ddl_clean.split(';') if stmt.strip()]

    def _analyze_statement(self, statement: str):
        """Analyze individual DDL statement"""
        statement_upper = statement.upper()

        if 'CREATE TABLE' in statement_upper:
            self._analyze_create_table(statement)
        elif 'ALTER TABLE' in statement_upper:
            self._analyze_alter_table(statement)
        elif 'DROP TABLE' in statement_upper:
            self._analyze_drop_table(statement)

    def _analyze_create_table(self, statement: str):
        """Analyze CREATE TABLE for potential issues"""
        # Extract table name
        table_match = re.search(r'CREATE TABLE\s+(\w+)', statement, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1)
            self.tables[table_name] = {'columns': [], 'issues': []}

            # Check for missing primary key
            if 'PRIMARY KEY' not in statement.upper():
                self.issues.append(f"Table '{table_name}' missing PRIMARY KEY")

    def _analyze_alter_table(self, statement: str):
        """Analyze ALTER TABLE statements"""
        statement_upper = statement.upper()  # Added missing variable
        if 'DROP COLUMN' in statement_upper:
            self.issues.append("Potential data loss: DROP COLUMN operation detected")

    def _analyze_drop_table(self, statement: str):
        """Analyze DROP TABLE statements"""
        self.issues.append("CRITICAL: DROP TABLE operation detected")

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate analysis summary"""
        return {
            'total_tables': len(self.tables),
            'total_issues': len(self.issues),
            'critical_issues': len([i for i in self.issues if 'CRITICAL' in i]),
            'warning_issues': len([i for i in self.issues if 'CRITICAL' not in i])
        }

def main():
    """Main function for migration safety analyzer"""
    print("Migration Safety Analyzer")
    print("=" * 30)

    # Example DDL for testing - Fixed: sample_ddl NOT sample_dd1
    sample_ddl = """
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(255)
    );
    
    CREATE TABLE orders (
        order_id INT,
        user_id INT,
        amount DECIMAL(10,2)
    );
    
    ALTER TABLE users DROP COLUMN email;
    DROP TABLE temporary_data;
    """

    analyzer = DDLAnalyzer()
    results = analyzer.parse_ddl(sample_ddl)  # Fixed: parse_ddl NOT parse_dd1

    # Print results
    print(f"Tables analyzed: {results['summary']['total_tables']}")
    print(f"Issues found: {results['summary']['total_issues']}")
    print(f"Critical issues: {results['summary']['critical_issues']}")

    print("\nDetailed Issues:")
    for issue in results['issues']:
        print(f"  - {issue}")

if __name__ == "__main__":
    main()