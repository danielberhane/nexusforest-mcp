# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
Data transformers for converting Excel data to three-table architecture.
CRITICAL: Maintains separate fact tables to avoid sparse matrix problem.
"""
import logging
import re
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
import polars as pl

from nexus.data.metadata.metadata_manager import metadata_manager

# Get constants from metadata
TROPICAL_COUNTRIES = metadata_manager.semantic.tropical_countries
ALL_THRESHOLDS = metadata_manager.semantic.thresholds
CARBON_THRESHOLDS = [30, 50, 75]  # Only these have carbon data
PRIMARY_THRESHOLD = 30  # Primary forest is always at 30%
FAO_STANDARD_THRESHOLD = 30
TREE_COVER_YEARS = range(metadata_manager.semantic.year_ranges["tree_cover"][0], metadata_manager.semantic.year_ranges["tree_cover"][1] + 1)
PRIMARY_FOREST_YEARS = range(metadata_manager.semantic.year_ranges["primary_forest"][0], metadata_manager.semantic.year_ranges["primary_forest"][1] + 1)
CARBON_YEARS = range(metadata_manager.semantic.year_ranges["carbon"][0], metadata_manager.semantic.year_ranges["carbon"][1] + 1)

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self):
        self.transformation_stats = {}
        
    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform the input DataFrame."""
        pass
        
    def _melt_year_columns(self, df: pl.DataFrame, year_pattern: str, value_name: str, id_vars: List[str]) -> pl.DataFrame:
        # Find year columns matching pattern
        year_cols = [col for col in df.columns if re.match(year_pattern, col)]
        
        if not year_cols:
            raise ValueError(f"No year columns found matching pattern: {year_pattern}")
        
        # Keep only existing id_vars
        id_vars = [col for col in id_vars if col in df.columns]
        
        # Use unpivot instead of melt (new Polars API)
        df_long = df.unpivot(
            index=id_vars,  # Changed from id_vars
            on=year_cols,    # Changed from value_vars
            variable_name="year_column",
            value_name=value_name
        )
        
        # Extract year from column name
        df_long = df_long.with_columns(
            pl.col("year_column")
            .str.extract(r'(\d{4})', 1)
            .cast(pl.Int32)
            .alias("year")
        ).drop("year_column")
        
        return df_long
        
    def _add_data_quality_flag(self, df: pl.DataFrame, value_column: str) -> pl.DataFrame:
        """Add data quality flag based on value column."""
        return df.with_columns(
            pl.when(pl.col(value_column).is_null()).then(pl.lit("NULL"))
            .when(pl.col(value_column) == 0).then(pl.lit("ZERO"))
            .when(pl.col(value_column) < 0).then(pl.lit("INVALID"))
            .otherwise(pl.lit("VALID"))
            .alias("data_quality_flag")
        )


class TreeCoverTransformer(BaseTransformer):
    """
    Transform tree cover loss data to fact_tree_cover_loss table.
    Expected output: ~31,680 rows (165 countries × 24 years × 8 thresholds)
    """
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform tree cover loss data."""
        logger.info("Starting tree cover loss transformation")
        
        # Normalize column names
        if "umd_tree_cover_density_2000__threshold" in df.columns:
            df = df.rename({"umd_tree_cover_density_2000__threshold": "threshold"})
            
        # Define static columns (non-year columns)
        static_cols = ["country", "threshold", "area_ha", 
                      "extent_2000_ha", "extent_2010_ha", "gain_2000-2012_ha"]
        
        # Keep only columns that exist
        static_cols = [col for col in static_cols if col in df.columns]
        
        # Melt year columns (tc_loss_ha_YYYY pattern)
        df_long = self._melt_year_columns(
            df=df,
            year_pattern=r'tc_loss_ha_\d{4}$',
            value_name="tree_cover_loss_ha",
            id_vars=static_cols
        )
        
        # Add computed columns
        df_long = df_long.with_columns([
            # Calculate loss rate as percentage of 2000 extent
            pl.when(pl.col("extent_2000_ha") > 0)
            .then((pl.col("tree_cover_loss_ha") / pl.col("extent_2000_ha")) * 100)
            .otherwise(None)
            .alias("loss_rate_pct"),
        ])
        
        # Add data quality flag
        df_long = self._add_data_quality_flag(df_long, "tree_cover_loss_ha")
        
        # Filter to valid year range
        df_long = df_long.filter(
            pl.col("year").is_between(min(TREE_COVER_YEARS), max(TREE_COVER_YEARS))
        )
        
        # Sort for consistent output
        df_long = df_long.sort(["country", "year", "threshold"])
        
        # Log statistics
        self.transformation_stats = {
            "input_rows": len(df),
            "output_rows": len(df_long),
            "unique_countries": df_long["country"].n_unique(),
            "unique_years": df_long["year"].n_unique(),
            "unique_thresholds": df_long["threshold"].n_unique(),
            "null_values": df_long["tree_cover_loss_ha"].null_count(),
        }
        
        logger.info(f"Tree cover transformation complete: {self.transformation_stats}")
        
        return df_long


class PrimaryForestTransformer(BaseTransformer):
    """
    Transform primary forest data to fact_primary_forest table.
    Expected output: ~1,725 rows (75 tropical countries × 23 years)
    CRITICAL: Only tropical countries, fixed at threshold=30
    """
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform primary forest loss data."""
        logger.info("Starting primary forest transformation")
        
        # Primary forest columns start with tc_loss_ha_
        df_long = self._melt_year_columns(
            df=df,
            year_pattern=r'tc_loss_ha_\d{4}$',
            value_name="primary_forest_loss_ha",
            id_vars=["country"]
        )
        
        # Add fixed threshold (always 30 for primary forest)
        df_long = df_long.with_columns([
            pl.lit(PRIMARY_THRESHOLD).cast(pl.Int32).alias("threshold"),
        ])
        
        # Add tropical country flag
        df_long = df_long.with_columns([
            pl.col("country").is_in(list(TROPICAL_COUNTRIES)).alias("is_tropical")
        ])
        
        # Add loss status categorization
        df_long = df_long.with_columns([
            pl.when(pl.col("primary_forest_loss_ha").is_null()).then(pl.lit("NO_DATA"))
            .when(pl.col("primary_forest_loss_ha") == 0).then(pl.lit("NO_LOSS"))
            .otherwise(pl.lit("LOSS_RECORDED"))
            .alias("loss_status")
        ])
        
        # Filter to valid year range (2002-2023 for primary forest)
        df_long = df_long.filter(
            pl.col("year").is_between(min(PRIMARY_FOREST_YEARS), max(PRIMARY_FOREST_YEARS))
        )
        
        # Verify all countries are tropical
        non_tropical = df_long.filter(~pl.col("is_tropical"))
        if len(non_tropical) > 0:
            countries = non_tropical["country"].unique().to_list()
            logger.warning(f"Found non-tropical countries in primary forest data: {countries}")
            
        # Sort for consistent output
        df_long = df_long.sort(["country", "year"])
        
        # Log statistics
        self.transformation_stats = {
            "input_rows": len(df),
            "output_rows": len(df_long),
            "unique_countries": df_long["country"].n_unique(),
            "tropical_countries": df_long.filter(pl.col("is_tropical"))["country"].n_unique(),
            "unique_years": df_long["year"].n_unique(),
            "null_values": df_long["primary_forest_loss_ha"].null_count(),
        }
        
        logger.info(f"Primary forest transformation complete: {self.transformation_stats}")
        
        return df_long


class CarbonTransformer(BaseTransformer):
    """
    Transform carbon data to fact_carbon table.
    Expected output: ~11,880 rows (165 countries × 24 years × 3 thresholds)
    CRITICAL: Only thresholds 30, 50, 75 have carbon data
    """
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform carbon emissions data."""
        logger.info("Starting carbon data transformation")
        
        # Fix column names
        if "umd_tree_cover_density_2000__threshold" in df.columns:
            df = df.rename({"umd_tree_cover_density_2000__threshold": "threshold"})
            
        # CRITICAL: Filter to valid carbon thresholds (30, 50, 75 only)
        df = df.filter(pl.col("threshold").is_in(CARBON_THRESHOLDS))
        
        if len(df) == 0:
            raise ValueError(f"No data found for carbon thresholds {CARBON_THRESHOLDS}")
            
        # Rename average columns for clarity
        rename_map = {
            "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1": "carbon_emissions_annual_avg",
            "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1": "carbon_removals_annual_avg", 
            "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1": "carbon_net_flux_annual_avg",
            "avg_gfw_aboveground_carbon_stocks_2000__Mg_C_ha-1": "carbon_density_mg_c_ha",
        }
        
        # Only rename columns that exist
        rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(rename_map)
        
        # Define static columns
        static_cols = [
            "country", "threshold",
            "carbon_emissions_annual_avg",
            "carbon_removals_annual_avg",
            "carbon_net_flux_annual_avg",
            "carbon_density_mg_c_ha",
        ]
        
        # Keep only existing columns
        static_cols = [col for col in static_cols if col in df.columns]
        
        # Melt emission year columns
        df_long = self._melt_year_columns(
            df=df,
            year_pattern=r'gfw_forest_carbon_gross_emissions_\d{4}__Mg_CO2e$',
            value_name="carbon_emissions_mg_co2e",
            id_vars=static_cols
        )
        
        # Add carbon flux status (sink vs source)
        df_long = df_long.with_columns([
            pl.when(pl.col("carbon_net_flux_annual_avg") < 0).then(pl.lit("SINK"))
            .when(pl.col("carbon_net_flux_annual_avg") > 0).then(pl.lit("SOURCE"))
            .otherwise(pl.lit("NEUTRAL"))
            .alias("carbon_flux_status")
        ])
        
        # Filter to valid year range
        df_long = df_long.filter(
            pl.col("year").is_between(min(CARBON_YEARS), max(CARBON_YEARS))
        )
        
        # Sort for consistent output
        df_long = df_long.sort(["country", "year", "threshold"])
        
        # Log statistics
        self.transformation_stats = {
            "input_rows": len(df),
            "output_rows": len(df_long),
            "unique_countries": df_long["country"].n_unique(),
            "unique_years": df_long["year"].n_unique(),
            "unique_thresholds": df_long["threshold"].n_unique(),
            "thresholds": sorted(df_long["threshold"].unique().to_list()),
            "null_values": df_long["carbon_emissions_mg_co2e"].null_count(),
            "sinks": len(df_long.filter(pl.col("carbon_flux_status") == "SINK")),
            "sources": len(df_long.filter(pl.col("carbon_flux_status") == "SOURCE")),
        }
        
        logger.info(f"Carbon transformation complete: {self.transformation_stats}")
        
        return df_long