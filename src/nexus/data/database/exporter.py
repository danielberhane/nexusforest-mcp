# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
Database exporter for saving transformed data to SQLite.
"""
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional, List
import polars as pl

from nexus.config.settings import settings
from nexus.data.database.schema import SchemaManager

logger = logging.getLogger(__name__)


class DatabaseExporter:
    """Export transformed data to SQLite database."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize database exporter.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path or settings.sqlite_db_path
        self.schema_manager = SchemaManager(self.db_path)
        
    def initialize_database(self, drop_existing: bool = False):
        """
        Initialize database with schema.
        
        Args:
            drop_existing: Whether to drop existing tables
        """
        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        connection = sqlite3.connect(self.db_path)
        try:
            if drop_existing:
                logger.warning("Dropping existing tables")
                self.schema_manager.drop_all_tables(connection)
                
            logger.info("Creating database schema")
            self.schema_manager.create_all_tables(connection)
            
            # ✅ ADD THIS: Final commit to ensure everything is persisted
            connection.commit()
            logger.info("Database schema created and committed successfully")
            
        finally:
            connection.close()
    def export_dataframe(
    self, 
    df: pl.DataFrame, 
    table_name: str,
    if_exists: str = "replace"
) -> int:
        """Export a Polars DataFrame to SQLite table."""
        logger.info(f"Exporting {len(df)} rows to {table_name}")
        
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        
        try:
            # ✅ FIXED: Delete data instead of dropping table
            if if_exists == "replace":
                cursor.execute(f"DELETE FROM {table_name}")  # ← Keep table structure!
            
            # ✅ REMOVED: Don't recreate table!
            # The table (with indexes) already exists from initialize_database()
            
            # Get columns and quote them for SQL
            columns = df.columns
            quoted_columns = [f'"{col}"' for col in columns]
            
            # Insert data
            placeholders = ','.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {table_name} ({', '.join(quoted_columns)}) VALUES ({placeholders})"
            
            # Convert to list of tuples for SQLite
            data_tuples = list(df.iter_rows())
            
            # Insert in batches
            batch_size = 10000
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
            
            connection.commit()
            
            # Verify
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            logger.info(f"Successfully exported {row_count} rows to {table_name}")
            return row_count
            
        except Exception as e:
            logger.error(f"Failed to export to {table_name}: {e}")
            logger.error(f"DataFrame columns: {df.columns}")
            connection.rollback()
            raise
            
        finally:
            connection.close()
            
    def export_all_tables(
    self,
    tree_cover_df: pl.DataFrame,
    primary_forest_df: pl.DataFrame,
    carbon_df: pl.DataFrame,
    dimension_dfs: Optional[Dict[str, pl.DataFrame]] = None
) -> Dict[str, int]:
        """
        Export all fact and dimension tables.
        
        Args:
            tree_cover_df: Tree cover loss data
            primary_forest_df: Primary forest loss data
            carbon_df: Carbon emissions data
            dimension_dfs: Optional dimension tables
            
        Returns:
            Dictionary with row counts for each table
        """
        results = {}
        
        # Export fact tables
        results["fact_tree_cover_loss"] = self.export_dataframe(
            tree_cover_df, "fact_tree_cover_loss"
        )
        
        results["fact_primary_forest"] = self.export_dataframe(
            primary_forest_df, "fact_primary_forest"
        )
        
        results["fact_carbon"] = self.export_dataframe(
            carbon_df, "fact_carbon"
        )
        
        # Export dimension tables if provided
        if dimension_dfs:
            for table_name, df in dimension_dfs.items():
                results[table_name] = self.export_dataframe(df, table_name)
        
        # ============= ADD THIS SECTION HERE =============
        # Verify indexes were created and persist
        logger.info("Verifying database indexes...")
        connection = sqlite3.connect(self.db_path)
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type = 'index' AND sql IS NOT NULL
                ORDER BY name
            """)
            indexes = cursor.fetchall()
            
            if not indexes:
                logger.error("NO INDEXES FOUND - Critical failure!")
                logger.error("Attempting to recreate indexes...")
                # Try to recreate indexes
                self.schema_manager.create_all_tables(connection)
            else:
                logger.info(f"Verified {len(indexes)} indexes exist: {[idx[0] for idx in indexes]}")
        finally:
            connection.close()
        # ============= END OF ADDED SECTION =============
                
        # Create indexes and optimize
        self._post_export_optimization()
        
        return results
        
    def _post_export_optimization(self):
        """Perform post-export optimizations."""
        connection = sqlite3.connect(self.db_path)
        try:
            cursor = connection.cursor()
            
            # Update statistics
            cursor.execute("ANALYZE")
            
            # Vacuum to reclaim space
            cursor.execute("VACUUM")
            
            connection.commit()
            logger.info("Database optimized after export")
            
        finally:
            connection.close()
            
    def validate_export(self) -> Dict[str, Dict]:
        """
        Validate exported data.
        
        Returns:
            Validation results for each table
        """
        results = {}
        connection = sqlite3.connect(self.db_path)
        
        try:
            cursor = connection.cursor()
            
            # Check fact_tree_cover_loss
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT country) as countries,
                    COUNT(DISTINCT year) as years,
                    COUNT(DISTINCT threshold) as thresholds,
                    SUM(CASE WHEN tree_cover_loss_ha IS NULL THEN 1 ELSE 0 END) as nulls
                FROM fact_tree_cover_loss
            """)
            results["fact_tree_cover_loss"] = dict(zip(
                ["total_rows", "countries", "years", "thresholds", "nulls"],
                cursor.fetchone()
            ))
            
            # Check fact_primary_forest
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT country) as countries,
                    COUNT(DISTINCT year) as years,
                    SUM(CASE WHEN is_tropical = 1 THEN 1 ELSE 0 END) as tropical_rows,
                    SUM(CASE WHEN primary_forest_loss_ha IS NULL THEN 1 ELSE 0 END) as nulls
                FROM fact_primary_forest
            """)
            results["fact_primary_forest"] = dict(zip(
                ["total_rows", "countries", "years", "tropical_rows", "nulls"],
                cursor.fetchone()
            ))
            
            # Check fact_carbon
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT country) as countries,
                    COUNT(DISTINCT year) as years,
                    COUNT(DISTINCT threshold) as thresholds,
                    SUM(CASE WHEN carbon_emissions_mg_co2e IS NULL THEN 1 ELSE 0 END) as nulls
                FROM fact_carbon
            """)
            results["fact_carbon"] = dict(zip(
                ["total_rows", "countries", "years", "thresholds", "nulls"],
                cursor.fetchone()
            ))
            
            # Add validation status
            for table, stats in results.items():
                if table == "fact_tree_cover_loss":
                    expected_rows = 165 * 24 * 8  # Approximate
                    results[table]["validation"] = "PASS" if stats["total_rows"] > 30000 else "FAIL"
                elif table == "fact_primary_forest":
                    expected_rows = 75 * 23  # Approximate
                    results[table]["validation"] = "PASS" if stats["total_rows"] > 1500 else "FAIL"
                elif table == "fact_carbon":
                    expected_rows = 165 * 24 * 3  # Approximate
                    results[table]["validation"] = "PASS" if stats["total_rows"] > 10000 else "FAIL"
                    
        finally:
            connection.close()
            
        return results
        
    def create_dimension_tables(self) -> Dict[str, pl.DataFrame]:
        """
        Create dimension tables from fact tables.
        
        Returns:
            Dictionary of dimension DataFrames
        """
        connection = sqlite3.connect(self.db_path)
        dimensions = {}
        
        try:
            # Create location dimension
            query = """
                SELECT DISTINCT 
                    country,
                    MAX(CASE WHEN is_tropical = 1 THEN 1 ELSE 0 END) as is_tropical
                FROM (
                    SELECT country, 0 as is_tropical FROM fact_tree_cover_loss
                    UNION ALL
                    SELECT country, is_tropical FROM fact_primary_forest
                )
                GROUP BY country
                ORDER BY country
            """
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert to Polars DataFrame
            if rows:
                countries = [row[0] for row in rows]
                is_tropical = [row[1] for row in rows]
                dimensions["dim_location"] = pl.DataFrame({
                    "country": countries,
                    "is_tropical": is_tropical
                })
            
            # Create time dimension
            query = """
                SELECT DISTINCT 
                    year,
                    CASE 
                        WHEN year BETWEEN 2000 AND 2009 THEN '2000s'
                        WHEN year BETWEEN 2010 AND 2019 THEN '2010s'
                        WHEN year BETWEEN 2020 AND 2029 THEN '2020s'
                    END as decade,
                    CASE 
                        WHEN year <= 2005 THEN 'Early 2000s'
                        WHEN year <= 2010 THEN 'Late 2000s'
                        WHEN year <= 2015 THEN 'Early 2010s'
                        WHEN year <= 2020 THEN 'Late 2010s'
                        ELSE 'Early 2020s'
                    END as period
                FROM (
                    SELECT DISTINCT year FROM fact_tree_cover_loss
                    UNION
                    SELECT DISTINCT year FROM fact_primary_forest
                    UNION
                    SELECT DISTINCT year FROM fact_carbon
                )
                ORDER BY year
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if rows:
                years = [row[0] for row in rows]
                decades = [row[1] for row in rows]
                periods = [row[2] for row in rows]
                dimensions["dim_time"] = pl.DataFrame({
                    "year": years,
                    "decade": decades,
                    "period": periods
                })
            
            # Export dimension tables
            for table_name, df in dimensions.items():
                self.export_dataframe(df, table_name, if_exists="replace")
                
        finally:
            connection.close()
            
        return dimensions


