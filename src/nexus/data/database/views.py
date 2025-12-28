"""
Optimized database views for common query patterns.
"""
import logging
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict

from nexus.config.settings import settings

logger = logging.getLogger(__name__)


class ViewManager:
    """Manages creation and maintenance of database views."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize view manager.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path or settings.sqlite_db_path
        
    def create_all_views(self, connection: Optional[sqlite3.Connection] = None):
        """
        Create all optimized views in the database.
        
        Args:
            connection: Optional database connection to use
        """
        if connection is None:
            connection = sqlite3.connect(self.db_path)
            close_connection = True
        else:
            close_connection = False
            
        try:
            cursor = connection.cursor()
            
            # Create each view
            self._create_primary_forest_percentage_view(cursor)
            self._create_carbon_intensity_view(cursor)
            self._create_annual_summary_view(cursor)
            self._create_country_summary_view(cursor)
            self._create_trend_analysis_view(cursor)
            self._create_top_emitters_view(cursor)
            
            connection.commit()
            logger.info("All database views created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create views: {e}")
            raise
            
        finally:
            if close_connection:
                connection.close()
                
    def _create_primary_forest_percentage_view(self, cursor: sqlite3.Cursor):
        """Create view for primary forest percentage calculations."""
        cursor.execute("DROP VIEW IF EXISTS v_primary_forest_percentage")
        
        sql = """
        CREATE VIEW v_primary_forest_percentage AS
        SELECT 
            t.country,
            t.year,
            t.tree_cover_loss_ha,
            p.primary_forest_loss_ha,
            CASE 
                WHEN t.tree_cover_loss_ha > 0 
                THEN ROUND((p.primary_forest_loss_ha / t.tree_cover_loss_ha) * 100, 2)
                ELSE NULL 
            END as primary_percentage,
            CASE
                WHEN t.tree_cover_loss_ha > 0 AND p.primary_forest_loss_ha > 0
                THEN 'Both'
                WHEN t.tree_cover_loss_ha > 0 
                THEN 'Total Only'
                WHEN p.primary_forest_loss_ha > 0
                THEN 'Primary Only'
                ELSE 'No Loss'
            END as loss_type
        FROM fact_tree_cover_loss t
        LEFT JOIN fact_primary_forest p
            ON t.country = p.country 
            AND t.year = p.year
        WHERE t.threshold = 30
        """
        
        cursor.execute(sql)
        logger.debug("Created v_primary_forest_percentage view")
        
    def _create_carbon_intensity_view(self, cursor: sqlite3.Cursor):
        """Create view for carbon intensity calculations."""
        cursor.execute("DROP VIEW IF EXISTS v_carbon_intensity")
        
        sql = """
        CREATE VIEW v_carbon_intensity AS
        SELECT 
            t.country,
            t.year,
            t.threshold,
            t.tree_cover_loss_ha,
            c.carbon_emissions_mg_co2e,
            c.carbon_net_flux_annual_avg,
            c.carbon_flux_status,
            CASE 
                WHEN t.tree_cover_loss_ha > 0 
                THEN ROUND(c.carbon_emissions_mg_co2e / t.tree_cover_loss_ha, 2)
                ELSE NULL 
            END as carbon_per_hectare,
            CASE
                WHEN c.carbon_net_flux_annual_avg < 0 THEN 'Carbon Sink'
                WHEN c.carbon_net_flux_annual_avg > 0 THEN 'Carbon Source'
                ELSE 'Carbon Neutral'
            END as carbon_role
        FROM fact_tree_cover_loss t
        INNER JOIN fact_carbon c
            ON t.country = c.country
            AND t.year = c.year
            AND t.threshold = c.threshold
        """
        
        cursor.execute(sql)
        logger.debug("Created v_carbon_intensity view")
        
    def _create_annual_summary_view(self, cursor: sqlite3.Cursor):
        """Create view for annual summaries."""
        cursor.execute("DROP VIEW IF EXISTS v_annual_summary")
        
        sql = """
        CREATE VIEW v_annual_summary AS
        SELECT 
            year,
            COUNT(DISTINCT country) as countries_reporting,
            SUM(tree_cover_loss_ha) as total_loss_ha,
            AVG(tree_cover_loss_ha) as avg_loss_ha,
            MAX(tree_cover_loss_ha) as max_loss_ha,
            MIN(CASE WHEN tree_cover_loss_ha > 0 THEN tree_cover_loss_ha ELSE NULL END) as min_loss_ha,
            ROUND(SUM(tree_cover_loss_ha) / 1000000, 2) as total_loss_million_ha
        FROM fact_tree_cover_loss
        WHERE threshold = 30
        GROUP BY year
        """
        
        cursor.execute(sql)
        logger.debug("Created v_annual_summary view")
        
    def _create_country_summary_view(self, cursor: sqlite3.Cursor):
        """Create view for country-level summaries."""
        cursor.execute("DROP VIEW IF EXISTS v_country_summary")
        
        sql = """
        CREATE VIEW v_country_summary AS
        WITH country_stats AS (
            SELECT 
                country,
                SUM(tree_cover_loss_ha) as total_loss,
                AVG(tree_cover_loss_ha) as avg_annual_loss,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT year) as years_of_data
            FROM fact_tree_cover_loss
            WHERE threshold = 30
            GROUP BY country
        ),
        primary_stats AS (
            SELECT 
                country,
                SUM(primary_forest_loss_ha) as total_primary_loss,
                COUNT(DISTINCT year) as primary_years_of_data
            FROM fact_primary_forest
            GROUP BY country
        ),
        carbon_stats AS (
            SELECT 
                country,
                AVG(carbon_net_flux_annual_avg) as avg_carbon_flux,
                SUM(carbon_emissions_mg_co2e) as total_emissions
            FROM fact_carbon
            WHERE threshold = 30
            GROUP BY country
        )
        SELECT 
            cs.*,
            ps.total_primary_loss,
            ps.primary_years_of_data,
            ROUND((ps.total_primary_loss / cs.total_loss) * 100, 2) as primary_loss_percentage,
            ca.avg_carbon_flux,
            ca.total_emissions,
            CASE 
                WHEN ca.avg_carbon_flux < 0 THEN 'Net Sink'
                WHEN ca.avg_carbon_flux > 0 THEN 'Net Source'
                ELSE 'Neutral'
            END as carbon_status
        FROM country_stats cs
        LEFT JOIN primary_stats ps ON cs.country = ps.country
        LEFT JOIN carbon_stats ca ON cs.country = ca.country
        """
        
        cursor.execute(sql)
        logger.debug("Created v_country_summary view")
        
    def _create_trend_analysis_view(self, cursor: sqlite3.Cursor):
        """Create view for trend analysis."""
        cursor.execute("DROP VIEW IF EXISTS v_trend_analysis")
        
        sql = """
        CREATE VIEW v_trend_analysis AS
        WITH yearly_data AS (
            SELECT 
                country,
                year,
                SUM(tree_cover_loss_ha) as annual_loss
            FROM fact_tree_cover_loss
            WHERE threshold = 30
            GROUP BY country, year
        ),
        lagged_data AS (
            SELECT 
                country,
                year,
                annual_loss,
                LAG(annual_loss, 1) OVER (PARTITION BY country ORDER BY year) as prev_year_loss,
                LAG(annual_loss, 5) OVER (PARTITION BY country ORDER BY year) as five_year_ago_loss
            FROM yearly_data
        )
        SELECT 
            country,
            year,
            annual_loss,
            prev_year_loss,
            CASE 
                WHEN prev_year_loss > 0 
                THEN ROUND(((annual_loss - prev_year_loss) / prev_year_loss) * 100, 2)
                ELSE NULL
            END as yoy_change_percent,
            five_year_ago_loss,
            CASE 
                WHEN five_year_ago_loss > 0 
                THEN ROUND(((annual_loss - five_year_ago_loss) / five_year_ago_loss) * 100, 2)
                ELSE NULL
            END as five_year_change_percent,
            CASE
                WHEN prev_year_loss IS NOT NULL AND annual_loss > prev_year_loss * 1.1 THEN 'Increasing'
                WHEN prev_year_loss IS NOT NULL AND annual_loss < prev_year_loss * 0.9 THEN 'Decreasing'
                ELSE 'Stable'
            END as trend_direction
        FROM lagged_data
        """
        
        cursor.execute(sql)
        logger.debug("Created v_trend_analysis view")
        
    def _create_top_emitters_view(self, cursor: sqlite3.Cursor):
        """Create view for top carbon emitters."""
        cursor.execute("DROP VIEW IF EXISTS v_top_emitters")
        
        sql = """
        CREATE VIEW v_top_emitters AS
        SELECT 
            c.country,
            c.year,
            SUM(c.carbon_emissions_mg_co2e) as total_emissions,
            SUM(t.tree_cover_loss_ha) as total_forest_loss,
            ROUND(SUM(c.carbon_emissions_mg_co2e) / NULLIF(SUM(t.tree_cover_loss_ha), 0), 2) as avg_carbon_intensity,
            RANK() OVER (PARTITION BY c.year ORDER BY SUM(c.carbon_emissions_mg_co2e) DESC) as emission_rank
        FROM fact_carbon c
        INNER JOIN fact_tree_cover_loss t
            ON c.country = t.country 
            AND c.year = t.year 
            AND c.threshold = t.threshold
        WHERE c.threshold = 30
        GROUP BY c.country, c.year
        """
        
        cursor.execute(sql)
        logger.debug("Created v_top_emitters view")
        
    def drop_all_views(self, connection: Optional[sqlite3.Connection] = None):
        """
        Drop all views from the database.
        
        Args:
            connection: Optional database connection to use
        """
        if connection is None:
            connection = sqlite3.connect(self.db_path)
            close_connection = True
        else:
            close_connection = False
            
        try:
            cursor = connection.cursor()
            
            views = [
                "v_primary_forest_percentage",
                "v_carbon_intensity",
                "v_annual_summary",
                "v_country_summary",
                "v_trend_analysis",
                "v_top_emitters"
            ]
            
            for view in views:
                cursor.execute(f"DROP VIEW IF EXISTS {view}")
                
            connection.commit()
            logger.info("All views dropped successfully")
            
        finally:
            if close_connection:
                connection.close()
                
    def list_views(self) -> List[str]:
        """
        List all views in the database.
        
        Returns:
            List of view names
        """
        connection = sqlite3.connect(self.db_path)
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='view' 
                ORDER BY name
            """)
            return [row[0] for row in cursor.fetchall()]
        finally:
            connection.close()
            
    def get_view_definition(self, view_name: str) -> str:
        """
        Get the SQL definition of a view.
        
        Args:
            view_name: Name of the view
            
        Returns:
            SQL definition of the view
        """
        connection = sqlite3.connect(self.db_path)
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT sql FROM sqlite_master 
                WHERE type='view' AND name=?
            """, (view_name,))
            result = cursor.fetchone()
            return result[0] if result else ""
        finally:
            connection.close()
            
    def query_view(self, view_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Query a view and return results.
        
        Args:
            view_name: Name of the view to query
            limit: Optional limit on number of rows
            
        Returns:
            List of dictionaries with query results
        """
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        
        try:
            cursor = connection.cursor()
            
            sql = f"SELECT * FROM {view_name}"
            if limit:
                sql += f" LIMIT {limit}"
                
            cursor.execute(sql)
            
            # Convert to list of dicts
            results = [dict(row) for row in cursor.fetchall()]
            return results
            
        finally:
            connection.close()