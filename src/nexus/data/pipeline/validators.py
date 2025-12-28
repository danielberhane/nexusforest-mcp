"""
Data validation operations for quality assurance.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import polars as pl

from nexus.config.settings import settings
from nexus.data.metadata.metadata_manager import metadata_manager

# Get constants from metadata
TROPICAL_COUNTRIES = metadata_manager.semantic.tropical_countries
CARBON_THRESHOLDS = [30, 50, 75]  # Only these have carbon data
PRIMARY_THRESHOLD = 30  # Primary forest is always at 30%
ALL_THRESHOLDS = metadata_manager.semantic.thresholds
TREE_COVER_YEARS = range(metadata_manager.semantic.year_ranges["tree_cover"][0], metadata_manager.semantic.year_ranges["tree_cover"][1] + 1)
PRIMARY_FOREST_YEARS = range(metadata_manager.semantic.year_ranges["primary_forest"][0], metadata_manager.semantic.year_ranges["primary_forest"][1] + 1)
CARBON_YEARS = range(metadata_manager.semantic.year_ranges["carbon"][0], metadata_manager.semantic.year_ranges["carbon"][1] + 1)
EXPECTED_ROWS = metadata_manager.runtime.row_counts if metadata_manager.runtime else {"fact_tree_cover_loss": 31680, "fact_primary_forest": 1650, "fact_carbon": 11880}

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check."""
    passed: bool
    message: str
    severity: str  # "error", "warning", "info"
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self):
        return f"[{self.severity.upper()}] {self.message}"


class DataValidator:
    """Comprehensive data validation for forest data."""
    
    def __init__(self):
        """Initialize validator."""
        self.validation_results = []
        self.stats = {}
        
    def validate_all(
        self,
        tree_cover_df: Optional[pl.DataFrame] = None,
        primary_forest_df: Optional[pl.DataFrame] = None,
        carbon_df: Optional[pl.DataFrame] = None
    ) -> Tuple[bool, List[ValidationResult]]:
        """
        Run all validation checks on provided dataframes.
        
        Args:
            tree_cover_df: Tree cover loss data
            primary_forest_df: Primary forest loss data
            carbon_df: Carbon emissions data
            
        Returns:
            Tuple of (overall_success, list_of_results)
        """
        self.validation_results = []
        
        # Validate each dataset if provided
        if tree_cover_df is not None:
            self.validate_tree_cover(tree_cover_df)
            
        if primary_forest_df is not None:
            self.validate_primary_forest(primary_forest_df)
            
        if carbon_df is not None:
            self.validate_carbon(carbon_df)
            
        # Overall success if no errors
        has_errors = any(r.severity == "error" for r in self.validation_results)
        
        return not has_errors, self.validation_results
        
    def validate_tree_cover(self, df: pl.DataFrame) -> List[ValidationResult]:
        """
        Validate tree cover loss data.
        
        Args:
            df: Tree cover loss dataframe
            
        Returns:
            List of validation results
        """
        results = []
        
        # Check required columns
        required_cols = ["country", "year", "threshold", "tree_cover_loss_ha"]
        result = self.validate_columns(df, required_cols)
        results.append(result)
        
        # Check row count
        expected = EXPECTED_ROWS["fact_tree_cover_loss"]
        actual = len(df)
        tolerance = 0.1  # 10% tolerance
        
        if abs(actual - expected) / expected > tolerance:
            results.append(ValidationResult(
                passed=False,
                message=f"Row count {actual:,} differs significantly from expected {expected:,}",
                severity="warning",
                details={"expected": expected, "actual": actual}
            ))
        else:
            results.append(ValidationResult(
                passed=True,
                message=f"Row count {actual:,} is within tolerance of expected {expected:,}",
                severity="info"
            ))
            
        # Check thresholds
        unique_thresholds = df["threshold"].unique().sort()
        if not set(unique_thresholds.to_list()).issubset(set(ALL_THRESHOLDS)):
            invalid = set(unique_thresholds.to_list()) - set(ALL_THRESHOLDS)
            results.append(ValidationResult(
                passed=False,
                message=f"Invalid thresholds found: {invalid}",
                severity="error",
                details={"invalid_thresholds": list(invalid)}
            ))
            
        # Check year range
        years = df["year"].unique().sort()
        min_year, max_year = years.min(), years.max()
        
        if min_year != min(TREE_COVER_YEARS) or max_year != max(TREE_COVER_YEARS):
            results.append(ValidationResult(
                passed=False,
                message=f"Year range {min_year}-{max_year} doesn't match expected {min(TREE_COVER_YEARS)}-{max(TREE_COVER_YEARS)}",
                severity="warning",
                details={"min_year": min_year, "max_year": max_year}
            ))
            
        # Check data completeness
        completeness = self.check_data_completeness(df)
        if completeness < settings.min_completeness_score:
            results.append(ValidationResult(
                passed=False,
                message=f"Data completeness {completeness:.1%} below threshold {settings.min_completeness_score:.1%}",
                severity="warning",
                details={"completeness": completeness}
            ))
            
        # Check for negative values (except where allowed)
        neg_values = self.check_negative_values(df, ["tree_cover_loss_ha", "extent_2000_ha"])
        if neg_values:
            results.append(ValidationResult(
                passed=False,
                message=f"Negative values found in columns: {neg_values}",
                severity="error",
                details={"columns_with_negatives": neg_values}
            ))
            
        self.validation_results.extend(results)
        return results
        
    def validate_primary_forest(self, df: pl.DataFrame) -> List[ValidationResult]:
        """
        Validate primary forest loss data.
        
        Args:
            df: Primary forest dataframe
            
        Returns:
            List of validation results
        """
        results = []
        
        # Check required columns
        required_cols = ["country", "year", "primary_forest_loss_ha"]
        result = self.validate_columns(df, required_cols)
        results.append(result)
        
        # Check all countries are tropical
        if "country" in df.columns:
            countries = df["country"].unique().to_list()
            non_tropical = set(countries) - TROPICAL_COUNTRIES
            
            if non_tropical:
                results.append(ValidationResult(
                    passed=False,
                    message=f"Non-tropical countries found in primary forest data: {non_tropical}",
                    severity="error",
                    details={"non_tropical_countries": list(non_tropical)}
                ))
            else:
                results.append(ValidationResult(
                    passed=True,
                    message=f"All {len(countries)} countries are tropical",
                    severity="info"
                ))
                
        # Check threshold is always 30
        if "threshold" in df.columns:
            unique_thresholds = df["threshold"].unique().to_list()
            if unique_thresholds != [PRIMARY_THRESHOLD]:
                results.append(ValidationResult(
                    passed=False,
                    message=f"Primary forest threshold should be {PRIMARY_THRESHOLD}, found: {unique_thresholds}",
                    severity="error",
                    details={"found_thresholds": unique_thresholds}
                ))
                
        # Check year range (2002-2023)
        if "year" in df.columns:
            years = df["year"].unique().sort()
            min_year, max_year = years.min(), years.max()
            
            if min_year != min(PRIMARY_FOREST_YEARS) or max_year != max(PRIMARY_FOREST_YEARS):
                results.append(ValidationResult(
                    passed=False,
                    message=f"Primary forest year range {min_year}-{max_year} doesn't match expected {min(PRIMARY_FOREST_YEARS)}-{max(PRIMARY_FOREST_YEARS)}",
                    severity="warning",
                    details={"min_year": min_year, "max_year": max_year}
                ))
                
        self.validation_results.extend(results)
        return results
        
    def validate_carbon(self, df: pl.DataFrame) -> List[ValidationResult]:
        """
        Validate carbon emissions data.
        
        Args:
            df: Carbon dataframe
            
        Returns:
            List of validation results
        """
        results = []
        
        # Check required columns
        required_cols = ["country", "year", "threshold", "carbon_emissions_mg_co2e"]
        result = self.validate_columns(df, required_cols)
        results.append(result)
        
        # Check thresholds are only 30, 50, 75
        if "threshold" in df.columns:
            unique_thresholds = df["threshold"].unique().sort().to_list()
            if set(unique_thresholds) != set(CARBON_THRESHOLDS):
                results.append(ValidationResult(
                    passed=False,
                    message=f"Carbon thresholds should be {CARBON_THRESHOLDS}, found: {unique_thresholds}",
                    severity="error",
                    details={"found_thresholds": unique_thresholds}
                ))
            else:
                results.append(ValidationResult(
                    passed=True,
                    message=f"Carbon thresholds correct: {unique_thresholds}",
                    severity="info"
                ))
                
        # Check that net flux can be negative (carbon sinks)
        if "carbon_net_flux_annual_avg" in df.columns:
            negative_flux = df.filter(pl.col("carbon_net_flux_annual_avg") < 0).height
            if negative_flux > 0:
                results.append(ValidationResult(
                    passed=True,
                    message=f"Found {negative_flux} carbon sinks (negative net flux) - this is expected",
                    severity="info",
                    details={"carbon_sinks": negative_flux}
                ))
                
        self.validation_results.extend(results)
        return results
        
    def validate_columns(self, df: pl.DataFrame, required_columns: List[str]) -> ValidationResult:
        """
        Validate that DataFrame has required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            ValidationResult
        """
        missing = set(required_columns) - set(df.columns)
        
        if missing:
            return ValidationResult(
                passed=False,
                message=f"Missing required columns: {missing}",
                severity="error",
                details={"missing_columns": list(missing)}
            )
            
        return ValidationResult(
            passed=True,
            message="All required columns present",
            severity="info"
        )
        
    def validate_year_columns(
        self,
        df: pl.DataFrame,
        start_year: int,
        end_year: int
    ) -> bool:
        """
        Validate that DataFrame has expected year columns.
        
        Args:
            df: DataFrame to validate
            start_year: Expected start year
            end_year: Expected end year
            
        Returns:
            True if year columns are complete
        """
        expected_years = set(range(start_year, end_year + 1))
        
        # Extract years from column names
        year_columns = []
        for col in df.columns:
            import re
            year_match = re.search(r'(\d{4})', col)
            if year_match:
                year = int(year_match.group())
                if 2000 <= year <= 2030:  # Reasonable year range
                    year_columns.append(year)
                    
        found_years = set(year_columns)
        missing_years = expected_years - found_years
        
        if missing_years:
            logger.warning(f"Missing year columns for years: {sorted(missing_years)}")
            return False
            
        return True
        
    def check_data_completeness(
        self,
        df: pl.DataFrame,
        threshold: Optional[float] = None
    ) -> float:
        """
        Check data completeness (percentage of non-null values).
        
        Args:
            df: DataFrame to check
            threshold: Optional minimum acceptable completeness ratio
            
        Returns:
            Completeness score (0-1)
        """
        threshold = threshold or settings.min_completeness_score
        
        total_cells = len(df) * len(df.columns)
        if total_cells == 0:
            return 0.0
            
        # Count nulls in numeric columns only
        numeric_cols = [
            col for col in df.columns
            if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]
        ]
        
        null_cells = sum(df[col].null_count() for col in numeric_cols)
        total_numeric_cells = len(df) * len(numeric_cols)
        
        if total_numeric_cells == 0:
            return 1.0
            
        completeness = 1 - (null_cells / total_numeric_cells)
        
        if completeness < threshold:
            logger.warning(
                f"Data completeness {completeness:.1%} is below threshold {threshold:.1%}"
            )
            
        return completeness
        
    def check_negative_values(
        self,
        df: pl.DataFrame,
        columns: List[str]
    ) -> List[str]:
        """
        Check for negative values in specified columns.
        
        Args:
            df: DataFrame to check
            columns: Columns to check for negatives
            
        Returns:
            List of columns containing negative values
        """
        columns_with_negatives = []
        
        for col in columns:
            if col not in df.columns:
                continue
                
            # Skip columns that can be negative
            if "net_flux" in col or "removals" in col:
                continue
                
            # Check for negatives
            neg_count = df.filter((pl.col(col) < 0) & pl.col(col).is_not_null()).height
            
            if neg_count > 0:
                columns_with_negatives.append(col)
                logger.warning(f"Found {neg_count} negative values in {col}")
                
        return columns_with_negatives
        
    def validate_relationships(
        self,
        tree_cover_df: pl.DataFrame,
        primary_forest_df: pl.DataFrame
    ) -> ValidationResult:
        """
        Validate relationships between tables.
        
        Example: Primary forest loss should not exceed total tree cover loss.
        
        Args:
            tree_cover_df: Tree cover loss data
            primary_forest_df: Primary forest loss data
            
        Returns:
            ValidationResult
        """
        # Join on country and year
        joined = tree_cover_df.filter(pl.col("threshold") == 30).join(
            primary_forest_df,
            on=["country", "year"],
            how="inner"
        )
        
        # Check if primary > total
        violations = joined.filter(
            (pl.col("primary_forest_loss_ha") > pl.col("tree_cover_loss_ha")) &
            pl.col("primary_forest_loss_ha").is_not_null() &
            pl.col("tree_cover_loss_ha").is_not_null()
        )
        
        if len(violations) > 0:
            return ValidationResult(
                passed=False,
                message=f"Found {len(violations)} cases where primary forest loss exceeds total forest loss",
                severity="error",
                details={"violation_count": len(violations)}
            )
            
        return ValidationResult(
            passed=True,
            message="Primary forest loss correctly bounded by total forest loss",
            severity="info"
        )