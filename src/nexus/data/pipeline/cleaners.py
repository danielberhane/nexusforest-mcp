"""
Data cleaning operations for Global Forest Watch data.
Simplified version - no artificial filling of NULLs.
"""
import logging
from typing import List, Optional, Dict, Any
import polars as pl

logger = logging.getLogger(__name__)


class DataCleaner:
    """Clean and fix data quality issues in forest data."""
    
    def __init__(self):
        """Initialize data cleaner."""
        self.cleaning_stats = {
            "countries_standardized": 0,
            "negative_values_fixed": 0,
            "impossible_values_capped": 0,
            "duplicates_removed": 0
        }
        
    def clean_country_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize country names across datasets.
        
        Args:
            df: DataFrame with country column
            
        Returns:
            DataFrame with standardized country names
        """
        if "country" not in df.columns:
            logger.warning("No 'country' column found to clean")
            return df
            
        # Country name mappings for standardization
        country_mappings = {
            # Common variations
            "USA": "United States",
            "US": "United States",
            "United States of America": "United States",
            "UK": "United Kingdom",
            "Britain": "United Kingdom",
            "DRC": "Democratic Republic of the Congo",
            "Congo, Dem. Rep.": "Democratic Republic of the Congo",
            "CAR": "Central African Republic",
            
            # Regional variations
            "Burma": "Myanmar",
            "Holland": "Netherlands",
            
            # Spelling variations
            "Cote d'Ivoire": "Côte d'Ivoire",
            "Ivory Coast": "Côte d'Ivoire",
            
            # Territory corrections
            "French Guyana": "French Guiana",
            "Virgin Islands": "U.S. Virgin Islands",
        }
        
        # Apply mappings
        original_countries = df["country"].n_unique()
        
        for old_name, new_name in country_mappings.items():
            df = df.with_columns(
                pl.when(pl.col("country") == old_name)
                .then(pl.lit(new_name))
                .otherwise(pl.col("country"))
                .alias("country")
            )
    
        # Trim whitespace only
        df = df.with_columns(
            pl.col("country").str.strip_chars().alias("country")
        )
        
        new_countries = df["country"].n_unique()
        self.cleaning_stats["countries_standardized"] = original_countries - new_countries
        
        if self.cleaning_stats["countries_standardized"] > 0:
            logger.info(f"Standardized {self.cleaning_stats['countries_standardized']} country name variations")
            
        return df
        
    def fix_negative_values(self, df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """
        Fix negative values in columns where they shouldn't exist.
        
        IMPORTANT: Carbon net flux CAN be negative (indicates carbon sink).
        
        Args:
            df: DataFrame to clean
            columns: Columns to check for negative values
            
        Returns:
            Cleaned DataFrame with invalid negatives converted to NULL
        """
        negative_count = 0
        
        for col in columns:
            if col not in df.columns:
                continue
                
            # CRITICAL: Skip columns that can legitimately be negative
            if "net_flux" in col or "removals" in col:
                logger.debug(f"Skipping {col} - negative values are valid (carbon sinks)")
                continue
                
            # Count and fix invalid negatives
            neg_mask = (pl.col(col) < 0) & pl.col(col).is_not_null()
            count = df.filter(neg_mask).height
            
            if count > 0:
                logger.warning(f"Found {count} negative values in {col}, converting to NULL")
                negative_count += count
                
                # Convert negative to null (not 0!)
                df = df.with_columns(
                    pl.when(pl.col(col) < 0)
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                
        self.cleaning_stats["negative_values_fixed"] = negative_count
        return df
        
    def cap_impossible_values(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Cap values that exceed logical limits.
        
        Example: Forest loss cannot exceed forest extent.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame with capped values
        """
        capped_count = 0
        
        # Cap tree cover loss at extent
        if "tree_cover_loss_ha" in df.columns and "extent_2000_ha" in df.columns:
            # Find cases where loss > extent (physically impossible)
            mask = (
                (pl.col("tree_cover_loss_ha") > pl.col("extent_2000_ha")) &
                (pl.col("extent_2000_ha") > 0) &
                pl.col("tree_cover_loss_ha").is_not_null()
            )
            
            count = df.filter(mask).height
            if count > 0:
                logger.warning(f"Capping {count} cases where loss exceeds extent")
                capped_count += count
                
                df = df.with_columns(
                    pl.when(mask)
                    .then(pl.col("extent_2000_ha"))
                    .otherwise(pl.col("tree_cover_loss_ha"))
                    .alias("tree_cover_loss_ha")
                )
                
        # Cap loss rate at 100%
        if "loss_rate_pct" in df.columns:
            mask = pl.col("loss_rate_pct") > 100
            count = df.filter(mask).height
            
            if count > 0:
                logger.warning(f"Capping {count} loss rates above 100%")
                capped_count += count
                
                df = df.with_columns(
                    pl.when(mask)
                    .then(100.0)
                    .otherwise(pl.col("loss_rate_pct"))
                    .alias("loss_rate_pct")
                )
                
        self.cleaning_stats["impossible_values_capped"] = capped_count
        return df
    
    def remove_duplicates(self, df: pl.DataFrame, subset: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Remove duplicate rows based on key columns.
        
        Args:
            df: DataFrame to clean
            subset: Columns to consider for duplicates (e.g., ['country', 'year', 'threshold'])
            
        Returns:
            DataFrame without duplicates
        """
        original_rows = len(df)
        
        if subset:
            df = df.unique(subset=subset, maintain_order=True)
        else:
            df = df.unique(maintain_order=True)
            
        duplicates_removed = original_rows - len(df)
        self.cleaning_stats["duplicates_removed"] = duplicates_removed
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate rows")
            
        return df
        
    def validate_thresholds(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Validate and fix threshold values.
        
        Args:
            df: DataFrame with threshold column
            
        Returns:
            Cleaned DataFrame with valid thresholds
        """
        if "threshold" not in df.columns:
            return df
            
        valid_thresholds = [0, 10, 15, 20, 25, 30, 50, 75]
        
        # Check for invalid thresholds
        invalid_mask = ~pl.col("threshold").is_in(valid_thresholds)
        invalid_count = df.filter(invalid_mask).height
        
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} rows with invalid thresholds")
            
            # Round to nearest valid threshold
            df = df.with_columns(
                pl.when(invalid_mask)
                .then(
                    pl.col("threshold").map_elements(
                        lambda x: min(valid_thresholds, key=lambda v: abs(v - x) if x is not None else float('inf')),
                        return_dtype=pl.Int32
                    )
                )
                .otherwise(pl.col("threshold"))
                .alias("threshold")
            )
            
        return df
        
    def validate_years(self, df: pl.DataFrame, min_year: int = 2001, max_year: int = 2024) -> pl.DataFrame:
        """
        Remove rows with invalid years.
        
        Args:
            df: DataFrame with year column
            min_year: Minimum valid year
            max_year: Maximum valid year
            
        Returns:
            Cleaned DataFrame with valid years only
        """
        if "year" not in df.columns:
            return df
            
        # Filter to valid year range
        original_rows = len(df)
        df = df.filter(
            (pl.col("year") >= min_year) & 
            (pl.col("year") <= max_year)
        )
        
        removed = original_rows - len(df)
        if removed > 0:
            logger.warning(f"Removed {removed} rows with years outside range {min_year}-{max_year}")
            
        return df
        
    def get_cleaning_summary(self) -> Dict[str, Any]:
        """
        Get summary of cleaning operations performed.
        
        Returns:
            Dictionary with cleaning statistics
        """
        return self.cleaning_stats.copy()

    # Simple method for when you need complete cases
    def get_complete_cases(self, df: pl.DataFrame, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Return only rows with no NULLs in specified columns.
        
        Args:
            df: DataFrame to filter
            columns: Specific columns to check for NULLs (None = check all)
            
        Returns:
            DataFrame with only complete cases
        """
        if columns:
            # Check specific columns
            mask = pl.all_horizontal([pl.col(c).is_not_null() for c in columns])
            complete_df = df.filter(mask)
        else:
            # Check all columns
            complete_df = df.drop_nulls()
            
        removed = len(df) - len(complete_df)
        logger.info(f"Filtered to complete cases: {len(complete_df)} rows ({removed} removed)")
        
        return complete_df