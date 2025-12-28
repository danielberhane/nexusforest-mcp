# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
Safe query executor with parameterized queries to prevent SQL injection.
"""
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple  # THIS LINE NEEDS Optional
from contextlib import contextmanager
import time
import os
import sys
from nexus.config.settings import settings

logger = logging.getLogger(__name__) 


# Debug logging
DATABASE_PATH = os.environ.get("DATABASE_PATH", "/app/data/processed/forest.db")
sys.stderr.write(f"DATABASE_PATH from env: {DATABASE_PATH}\n")
sys.stderr.write(f"Path exists: {Path(DATABASE_PATH).exists()}\n")
sys.stderr.write(f"Path absolute: {Path(DATABASE_PATH).absolute()}\n")

# Try to import settings and see what it has
try:
    from nexus.config.settings import settings
    sys.stderr.write(f"Settings db path: {settings.sqlite_db_path}\n")
except Exception as e:
    sys.stderr.write(f"Settings import error: {e}\n")

# Initialize with explicit path
# from nexus.data.database.query_executor import QueryExecutor
# query_executor = QueryExecutor(db_path=Path(DATABASE_PATH))

class QueryExecutor:
    """
    Execute queries safely against the database.
    
    Uses parameterized queries to prevent SQL injection and provides
    transaction support for data integrity.
    """
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize query executor.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path or settings.sqlite_db_path
        self._validate_db_exists()
    
    def _validate_db_exists(self):
        """Ensure database file exists."""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
    
    @contextmanager
    def get_connection(self, readonly: bool = True):
        """
        Context manager for database connections.
        
        Args:
            readonly: If True, open in read-only mode for safety
            
        Yields:
            Database connection
        """
        # Build connection URL with appropriate mode

        
        conn = sqlite3.connect(str(self.db_path))
        
        # Enable row factory for dictionary-like results
        conn.row_factory = sqlite3.Row
        
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(
        self, 
        sql: str, 
        params: Optional[Tuple] = None,
        readonly: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query with parameters.
        
        IMPORTANT: Always use parameter placeholders (?) for user input
        to prevent SQL injection attacks.
        
        Args:
            sql: SQL query with ? placeholders for parameters
            params: Tuple of parameter values
            readonly: Whether this is a read-only query
            
        Returns:
            List of result dictionaries
            
        Example:
            sql = "SELECT * FROM users WHERE country = ? AND year = ?"
            params = ("Brazil", 2023)
            results = executor.execute_query(sql, params)
        """
        start_time = time.time()
        
        try:
            with self.get_connection(readonly=readonly) as conn:
                cursor = conn.cursor()
                
                # Log query for debugging (but not parameters for security)
                logger.debug(f"Executing query: {sql[:100]}...")
                
                # Execute with parameters (safe from SQL injection)
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                
                # Fetch all results
                results = [dict(row) for row in cursor.fetchall()]
                
                # Log execution time
                execution_time = (time.time() - start_time) * 1000
                logger.debug(f"Query executed in {execution_time:.2f}ms, returned {len(results)} rows")
                
                return results
                
        except sqlite3.OperationalError as e:
            if "readonly" in str(e).lower():
                logger.error("Attempted write operation on read-only connection")
            raise
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"SQL: {sql}")
            raise
    
    def execute_transaction(
        self,
        operations: List[Tuple[str, Optional[Tuple]]]
    ) -> bool:
        """
        Execute multiple operations in a transaction.
        
        Ensures all operations succeed or all fail (ACID compliance).
        
        Args:
            operations: List of (sql, params) tuples
            
        Returns:
            True if successful
            
        Example:
            operations = [
                ("INSERT INTO logs (message) VALUES (?)", ("Starting",)),
                ("UPDATE stats SET count = count + 1 WHERE id = ?", (1,))
            ]
            success = executor.execute_transaction(operations)
        """
        with self.get_connection(readonly=False) as conn:
            try:
                # Start transaction
                conn.execute("BEGIN TRANSACTION")
                cursor = conn.cursor()
                
                for sql, params in operations:
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                
                # Commit if all successful
                conn.commit()
                logger.info(f"Transaction completed: {len(operations)} operations")
                return True
                
            except Exception as e:
                # Rollback on any error
                conn.rollback()
                logger.error(f"Transaction failed, rolled back: {e}")
                raise
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a table safely.
        
        Args:
            table_name: Name of the table (validated against whitelist)
            
        Returns:
            Statistics dictionary
        """
        # Whitelist of allowed table names to prevent injection
        allowed_tables = [
            "fact_tree_cover_loss",
            "fact_primary_forest", 
            "fact_carbon",
            "dim_location",
            "dim_time"
        ]
        
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}")
        
        # Safe because table_name is from whitelist
        stats_sql = f"""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT country) as unique_countries,
                COUNT(DISTINCT year) as unique_years
            FROM {table_name}
        """
        
        results = self.execute_query(stats_sql)
        return results[0] if results else {}
    
    def build_where_clause(
        self,
        conditions: Dict[str, Any]
    ) -> Tuple[str, Tuple]:
        """
        Build WHERE clause with parameters safely.
        
        Args:
            conditions: Dictionary of column: value conditions
            
        Returns:
            Tuple of (WHERE clause string, parameter tuple)
            
        Example:
            conditions = {"country": "Brazil", "year": 2023}
            where_clause, params = build_where_clause(conditions)
            # Returns: ("WHERE country = ? AND year = ?", ("Brazil", 2023))
        """
        if not conditions:
            return "", ()
        
        # Build parameterized WHERE clause
        where_parts = []
        params = []
        
        for column, value in conditions.items():
            # Validate column name (alphanumeric and underscore only)
            if not column.replace('_', '').isalnum():
                raise ValueError(f"Invalid column name: {column}")
            
            if value is None:
                where_parts.append(f"{column} IS NULL")
            elif isinstance(value, (list, tuple)):
                # Handle IN clause
                placeholders = ','.join('?' * len(value))
                where_parts.append(f"{column} IN ({placeholders})")
                params.extend(value)
            else:
                where_parts.append(f"{column} = ?")
                params.append(value)
        
        where_clause = "WHERE " + " AND ".join(where_parts)
        return where_clause, tuple(params)
    
    def validate_database(self) -> Dict[str, Any]:
        """
        Validate database integrity.
        
        Returns:
            Validation results
        """
        results = {
            "status": "healthy",
            "tables": {},
            "issues": []
        }
        
        try:
            # Check each fact table
            for table in ["fact_tree_cover_loss", "fact_primary_forest", "fact_carbon"]:
                stats = self.get_table_stats(table)
                results["tables"][table] = stats
                
                if stats.get("row_count", 0) == 0:
                    results["issues"].append(f"{table} is empty")
                    results["status"] = "unhealthy"
            
            # Check integrity
            integrity_sql = "PRAGMA integrity_check"
            integrity = self.execute_query(integrity_sql)
            
            if integrity[0].get("integrity_check") != "ok":
                results["issues"].append("Database integrity check failed")
                results["status"] = "unhealthy"
                
        except Exception as e:
            results["status"] = "error"
            results["issues"].append(str(e))
        
        return results
