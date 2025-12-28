# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
SQLite database schema definitions with FIXED index creation.
"""
import logging
from typing import List, Dict, Any
from dataclasses import dataclass
import sqlite3
from pathlib import Path

from nexus.config.settings import settings

CARBON_THRESHOLDS = [30, 50, 75]  # Only these have carbon data

logger = logging.getLogger(__name__)


@dataclass
class TableSchema:
    """Represents a database table schema."""
    name: str
    columns: List[tuple]  # (column_name, data_type, constraints)
    primary_key: List[str]
    indexes: List[Dict[str, Any]]
    

FACT_TREE_COVER_SCHEMA = TableSchema(
    name="fact_tree_cover_loss",
    columns=[
        ("country", "TEXT", "NOT NULL"),
        ("year", "INTEGER", "NOT NULL"),
        ("threshold", "INTEGER", "NOT NULL"),
        ("tree_cover_loss_ha", "REAL", ""),
        ("extent_2000_ha", "REAL", ""),
        ("extent_2010_ha", "REAL", ""),
        ("gain_2000-2012_ha", "REAL", ""),
        ("area_ha", "REAL", ""),
        ("loss_rate_pct", "REAL", ""),
        ("data_quality_flag", "TEXT", ""),
    ],
    primary_key=["country", "year", "threshold"],
    indexes=[
        {"name": "idx_tcl_country_year", "columns": ["country", "year"]},
        {"name": "idx_tcl_threshold", "columns": ["threshold"]},
        {"name": "idx_tcl_year", "columns": ["year"]},
    ]
)

FACT_PRIMARY_FOREST_SCHEMA = TableSchema(
    name="fact_primary_forest",
    columns=[
        ("country", "TEXT", "NOT NULL"),
        ("year", "INTEGER", "NOT NULL"),
        ("threshold", "INTEGER", "DEFAULT 30"),
        ("primary_forest_loss_ha", "REAL", ""),
        ("is_tropical", "BOOLEAN", ""),
        ("loss_status", "TEXT", ""),
    ],
    primary_key=["country", "year"],
    indexes=[
        {"name": "idx_pf_country_year", "columns": ["country", "year"]},
        {"name": "idx_pf_tropical", "columns": ["is_tropical"]},
    ]
)

FACT_CARBON_SCHEMA = TableSchema(
    name="fact_carbon",
    columns=[
        ("country", "TEXT", "NOT NULL"),
        ("year", "INTEGER", "NOT NULL"),
        ("threshold", "INTEGER", f"NOT NULL CHECK (threshold IN ({','.join(map(str, CARBON_THRESHOLDS))}))"),
        ("carbon_emissions_mg_co2e", "REAL", ""),
        ("carbon_emissions_annual_avg", "REAL", ""),
        ("carbon_removals_annual_avg", "REAL", ""),
        ("carbon_net_flux_annual_avg", "REAL", ""),
        ("carbon_density_mg_c_ha", "REAL", ""),
        ("carbon_flux_status", "TEXT", ""),
    ],
    primary_key=["country", "year", "threshold"],
    indexes=[
        {"name": "idx_carbon_country_year", "columns": ["country", "year", "threshold"]},
        {"name": "idx_carbon_threshold", "columns": ["threshold"]},
        {"name": "idx_carbon_status", "columns": ["carbon_flux_status"]},
    ]
)

DIM_LOCATION_SCHEMA = TableSchema(
    name="dim_location",
    columns=[
        ("country", "TEXT", ""),
        ("region", "TEXT", ""),
        ("subregion", "TEXT", ""),
        ("is_tropical", "BOOLEAN", ""),
        ("iso_code", "TEXT", ""),
    ],
    primary_key=["country"],
    indexes=[]
)

DIM_TIME_SCHEMA = TableSchema(
    name="dim_time",
    columns=[
        ("year", "INTEGER", ""),
        ("decade", "TEXT", ""),
        ("period", "TEXT", ""),
    ],
    primary_key=["year"],
    indexes=[]
)

ALL_SCHEMAS = [
    FACT_TREE_COVER_SCHEMA,
    FACT_PRIMARY_FOREST_SCHEMA,
    FACT_CARBON_SCHEMA,
    DIM_LOCATION_SCHEMA,
    DIM_TIME_SCHEMA,
]

FACT_TABLE_SCHEMAS = [
    FACT_TREE_COVER_SCHEMA,
    FACT_PRIMARY_FOREST_SCHEMA,
    FACT_CARBON_SCHEMA,
]


class SchemaManager:
    """Manages database schema creation and updates."""
    
    def __init__(self, db_path: Path = None):
        """Initialize schema manager."""
        self.db_path = db_path or settings.sqlite_db_path
        
    def create_schema(self, schema: TableSchema, connection: sqlite3.Connection):
        """Create a table from schema definition with proper index creation."""
        cursor = connection.cursor()
        
        # Build CREATE TABLE statement
        columns_sql = []
        for col_name, col_type, constraints in schema.columns:
            # Quote column names to handle special characters
            columns_sql.append(f'"{col_name}" {col_type} {constraints}'.strip())
            
        # Add primary key
        if schema.primary_key:
            quoted_pk = [f'"{col}"' for col in schema.primary_key]
            pk_sql = f"PRIMARY KEY ({', '.join(quoted_pk)})"
            columns_sql.append(pk_sql)
            
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema.name} (
            {', '.join(columns_sql)}
        )
        """
        
        logger.debug(f"Creating table {schema.name}")
        cursor.execute(create_sql)
        
        # Create indexes IMMEDIATELY after table creation
        index_count = 0
        for index in schema.indexes:
            quoted_cols = [f'"{col}"' for col in index['columns']]
            index_sql = f"""
            CREATE INDEX IF NOT EXISTS {index['name']}
            ON {schema.name} ({', '.join(quoted_cols)})
            """
            cursor.execute(index_sql)
            index_count += 1
            logger.debug(f"Created index {index['name']} on {schema.name}")
        
        # DON'T commit here - let the caller manage transactions
        logger.info(f"Created table {schema.name} with {index_count} indexes")
        
    def create_all_tables(self, connection: sqlite3.Connection = None):
        """Create all tables in the database with proper transaction management."""
        if connection is None:
            connection = sqlite3.connect(self.db_path)
            close_connection = True
        else:
            close_connection = False
            
        try:
            # Start explicit transaction
            connection.execute("BEGIN IMMEDIATE")
            
            for schema in ALL_SCHEMAS:
                self.create_schema(schema, connection)
            
            # Create views
            self._create_views(connection)
            
            # CRITICAL: Verify indexes were created
            cursor = connection.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type = 'index' AND sql IS NOT NULL
                ORDER BY name
            """)
            indexes = cursor.fetchall()
            logger.info(f"Created {len(indexes)} indexes total: {[idx[0] for idx in indexes]}")
            
            # Commit the entire transaction
            connection.commit()
            logger.info("Database schema and indexes committed successfully")
            
            # Optimize after commit
            self._optimize_database(connection)
            
        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to create schema: {e}")
            raise
        finally:
            if close_connection:
                connection.close()
                
    def _create_views(self, connection: sqlite3.Connection):
        """Create useful database views for common queries."""
        cursor = connection.cursor()
        
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_primary_forest_percentage AS
        SELECT 
            t.country,
            t.year,
            t.tree_cover_loss_ha,
            p.primary_forest_loss_ha,
            CASE 
                WHEN t.tree_cover_loss_ha > 0 
                THEN ROUND((p.primary_forest_loss_ha / t.tree_cover_loss_ha) * 100, 2)
                ELSE NULL 
            END as primary_percentage
        FROM fact_tree_cover_loss t
        LEFT JOIN fact_primary_forest p
            ON t.country = p.country 
            AND t.year = p.year
        WHERE t.threshold = 30
        """)
        
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_carbon_intensity AS
        SELECT 
            t.country,
            t.year,
            t.threshold,
            t.tree_cover_loss_ha,
            c.carbon_emissions_mg_co2e,
            CASE 
                WHEN t.tree_cover_loss_ha > 0 
                THEN ROUND(c.carbon_emissions_mg_co2e / t.tree_cover_loss_ha, 2)
                ELSE NULL 
            END as carbon_per_hectare
        FROM fact_tree_cover_loss t
        INNER JOIN fact_carbon c
            ON t.country = c.country
            AND t.year = c.year
            AND t.threshold = c.threshold
        """)
        
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_annual_summary AS
        SELECT 
            year,
            COUNT(DISTINCT country) as countries_reporting,
            SUM(tree_cover_loss_ha) as total_loss_ha,
            AVG(tree_cover_loss_ha) as avg_loss_ha,
            MAX(tree_cover_loss_ha) as max_loss_ha
        FROM fact_tree_cover_loss
        WHERE threshold = 30
        GROUP BY year
        """)
        
        logger.info("Created database views")
        
    def _optimize_database(self, connection: sqlite3.Connection):
        """Optimize database for performance."""
        cursor = connection.cursor()
        
        # Analyze tables for query planner
        cursor.execute("ANALYZE")
        
        # Set pragmas for performance
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA cache_size = 10000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        
        connection.commit()
        logger.info("Database optimized")
        
    def verify_indexes(self, connection: sqlite3.Connection = None) -> Dict[str, List[str]]:
        """Verify that indexes exist in the database."""
        if connection is None:
            connection = sqlite3.connect(self.db_path)
            close_connection = True
        else:
            close_connection = False
            
        try:
            cursor = connection.cursor()
            
            # Get all indexes grouped by table
            cursor.execute("""
                SELECT tbl_name, name 
                FROM sqlite_master 
                WHERE type = 'index' AND sql IS NOT NULL
                ORDER BY tbl_name, name
            """)
            
            indexes_by_table = {}
            for table, index in cursor.fetchall():
                if table not in indexes_by_table:
                    indexes_by_table[table] = []
                indexes_by_table[table].append(index)
            
            # Log results
            for table, indexes in indexes_by_table.items():
                logger.info(f"Table {table} has indexes: {indexes}")
            
            return indexes_by_table
            
        finally:
            if close_connection:
                connection.close()
                
    def drop_all_tables(self, connection: sqlite3.Connection = None):
        """Drop all tables (use with caution)."""
        if connection is None:
            connection = sqlite3.connect(self.db_path)
            close_connection = True
        else:
            close_connection = False
            
        try:
            cursor = connection.cursor()
            
            # Drop views first
            cursor.execute("DROP VIEW IF EXISTS v_primary_forest_percentage")
            cursor.execute("DROP VIEW IF EXISTS v_carbon_intensity")
            cursor.execute("DROP VIEW IF EXISTS v_annual_summary")
            
            # Drop tables
            for schema in ALL_SCHEMAS:
                cursor.execute(f"DROP TABLE IF EXISTS {schema.name}")
                
            connection.commit()
            logger.info("All tables dropped")
            
        finally:
            if close_connection:
                connection.close()