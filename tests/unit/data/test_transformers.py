"""
Unit tests for data transformation pipeline.
Tests each transformer with sample data to verify correct output structure.
"""
import pytest
import polars as pl
from datetime import datetime

from nexus.data.pipeline.transformers import (
    TreeCoverTransformer,
    PrimaryForestTransformer,
    CarbonTransformer
)
from nexus.data.pipeline.cleaners import DataCleaner
from nexus.data.pipeline.validators import DataValidator
from nexus.data.metadata.metadata_manager import metadata_manager

TROPICAL_COUNTRIES = metadata_manager.semantic.tropical_countries
CARBON_THRESHOLDS = [30, 50, 75]
PRIMARY_THRESHOLD = 30


class TestTreeCoverTransformer:
    """Test tree cover loss transformation."""
    
    @pytest.fixture
    def sample_tree_cover_data(self):
        """Create sample tree cover data in wide format."""
        return pl.DataFrame({
            "country": ["Brazil", "Indonesia", "Peru"],
            "threshold": [30, 30, 30],
            "area_ha": [1000000, 500000, 300000],
            "extent_2000_ha": [900000, 450000, 280000],
            "extent_2010_ha": [850000, 420000, 270000],
            "tc_loss_ha_2001": [1000, 800, 600],
            "tc_loss_ha_2002": [1100, 850, 650],
            "tc_loss_ha_2003": [1200, 900, 700],
        })
    
    def test_transform_basic(self, sample_tree_cover_data):
        """Test basic transformation from wide to long format."""
        transformer = TreeCoverTransformer()
        result = transformer.transform(sample_tree_cover_data)
        
        # Check output structure
        assert "country" in result.columns
        assert "year" in result.columns
        assert "threshold" in result.columns
        assert "tree_cover_loss_ha" in result.columns
        assert "loss_rate_pct" in result.columns
        assert "data_quality_flag" in result.columns
        
        # Check row count (3 countries × 3 years = 9 rows)
        assert len(result) == 9
        
        # Check year column was created correctly
        years = result["year"].unique().sort()
        assert years.to_list() == [2001, 2002, 2003]
        
        # Check countries are preserved
        countries = result["country"].unique().sort()
        assert countries.to_list() == ["Brazil", "Indonesia", "Peru"]
    
    def test_loss_rate_calculation(self, sample_tree_cover_data):
        """Test that loss rate is calculated correctly."""
        transformer = TreeCoverTransformer()
        result = transformer.transform(sample_tree_cover_data)
        
        # Get Brazil 2001 data
        brazil_2001 = result.filter(
            (pl.col("country") == "Brazil") & (pl.col("year") == 2001)
        ).to_dicts()[0]
        
        # Check loss rate calculation
        expected_rate = (1000 / 900000) * 100
        assert abs(brazil_2001["loss_rate_pct"] - expected_rate) < 0.01
    
    def test_data_quality_flags(self, sample_tree_cover_data):
        """Test data quality flag assignment."""
        # Add some nulls and zeros
        data = sample_tree_cover_data.with_columns([
            pl.when(pl.col("country") == "Peru")
            .then(None)
            .otherwise(pl.col("tc_loss_ha_2001"))
            .alias("tc_loss_ha_2001"),
            
            pl.when(pl.col("country") == "Indonesia")
            .then(0)
            .otherwise(pl.col("tc_loss_ha_2002"))
            .alias("tc_loss_ha_2002")
        ])
        
        transformer = TreeCoverTransformer()
        result = transformer.transform(data)
        
        # Check flags
        peru_2001 = result.filter(
            (pl.col("country") == "Peru") & (pl.col("year") == 2001)
        ).to_dicts()[0]
        assert peru_2001["data_quality_flag"] == "NULL"
        
        indonesia_2002 = result.filter(
            (pl.col("country") == "Indonesia") & (pl.col("year") == 2002)
        ).to_dicts()[0]
        assert indonesia_2002["data_quality_flag"] == "ZERO"


class TestPrimaryForestTransformer:
    """Test primary forest transformation."""
    
    @pytest.fixture
    def sample_primary_data(self):
        """Create sample primary forest data."""
        return pl.DataFrame({
            "country": ["Brazil", "Indonesia", "Peru"],
            "tc_loss_ha_2002": [500, 400, 300],
            "tc_loss_ha_2003": [550, 420, 310],
            "tc_loss_ha_2004": [600, 450, 320],
        })
    
    def test_transform_basic(self, sample_primary_data):
        """Test basic primary forest transformation."""
        transformer = PrimaryForestTransformer()
        result = transformer.transform(sample_primary_data)
        
        # Check output structure
        assert "country" in result.columns
        assert "year" in result.columns
        assert "threshold" in result.columns
        assert "primary_forest_loss_ha" in result.columns
        assert "is_tropical" in result.columns
        assert "loss_status" in result.columns
        
        # Check row count (3 countries × 3 years = 9 rows)
        assert len(result) == 9
        
        # Check threshold is always 30
        thresholds = result["threshold"].unique()
        assert len(thresholds) == 1
        assert thresholds[0] == PRIMARY_THRESHOLD
    
    def test_tropical_flag(self, sample_primary_data):
        """Test tropical country flag."""
        # Add a non-tropical country for testing
        data = pl.concat([
            sample_primary_data,
            pl.DataFrame({
                "country": ["Canada"],
                "tc_loss_ha_2002": [100],
                "tc_loss_ha_2003": [110],
                "tc_loss_ha_2004": [120],
            })
        ])
        
        transformer = PrimaryForestTransformer()
        result = transformer.transform(data)
        
        # Check tropical flags
        brazil = result.filter(pl.col("country") == "Brazil").to_dicts()[0]
        assert brazil["is_tropical"] == True
        
        canada = result.filter(pl.col("country") == "Canada").to_dicts()[0]
        assert canada["is_tropical"] == False
    
    def test_loss_status(self, sample_primary_data):
        """Test loss status categorization."""
        # Add nulls and zeros
        data = sample_primary_data.with_columns([
            pl.when(pl.col("country") == "Peru")
            .then(None)
            .otherwise(pl.col("tc_loss_ha_2002"))
            .alias("tc_loss_ha_2002"),
            
            pl.when(pl.col("country") == "Indonesia")
            .then(0)
            .otherwise(pl.col("tc_loss_ha_2003"))
            .alias("tc_loss_ha_2003")
        ])
        
        transformer = PrimaryForestTransformer()
        result = transformer.transform(data)
        
        peru_2002 = result.filter(
            (pl.col("country") == "Peru") & (pl.col("year") == 2002)
        ).to_dicts()[0]
        assert peru_2002["loss_status"] == "NO_DATA"
        
        indonesia_2003 = result.filter(
            (pl.col("country") == "Indonesia") & (pl.col("year") == 2003)
        ).to_dicts()[0]
        assert indonesia_2003["loss_status"] == "NO_LOSS"
        
        brazil_2004 = result.filter(
            (pl.col("country") == "Brazil") & (pl.col("year") == 2004)
        ).to_dicts()[0]
        assert brazil_2004["loss_status"] == "LOSS_RECORDED"


class TestCarbonTransformer:
    """Test carbon emissions transformation."""
    
    @pytest.fixture
    def sample_carbon_data(self):
        """Create sample carbon data."""
        return pl.DataFrame({
            "country": ["Brazil", "Brazil", "Brazil"],
            "umd_tree_cover_density_2000__threshold": [30, 50, 75],
            "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1": [1000, 900, 800],
            "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1": [-500, -450, -400],
            "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1": [500, 450, 400],
            "gfw_forest_carbon_gross_emissions_2001__Mg_CO2e": [100, 90, 80],
            "gfw_forest_carbon_gross_emissions_2002__Mg_CO2e": [110, 95, 85],
            "gfw_forest_carbon_gross_emissions_2003__Mg_CO2e": [120, 100, 90],
        })
    
    def test_transform_basic(self, sample_carbon_data):
        """Test basic carbon transformation."""
        transformer = CarbonTransformer()
        result = transformer.transform(sample_carbon_data)
        
        # Check output structure
        assert "country" in result.columns
        assert "year" in result.columns
        assert "threshold" in result.columns
        assert "carbon_emissions_mg_co2e" in result.columns
        assert "carbon_emissions_annual_avg" in result.columns
        assert "carbon_flux_status" in result.columns
        
        # Check row count (1 country × 3 thresholds × 3 years = 9 rows)
        assert len(result) == 9
        
        # Check thresholds are only 30, 50, 75
        thresholds = result["threshold"].unique().sort()
        assert thresholds.to_list() == CARBON_THRESHOLDS
    
    def test_threshold_filtering(self, sample_carbon_data):
        """Test that invalid thresholds are filtered out."""
        # Add invalid threshold
        data = pl.concat([
            sample_carbon_data,
            pl.DataFrame({
                "country": ["Brazil"],
                "umd_tree_cover_density_2000__threshold": [25],  # Invalid!
                "gfw_forest_carbon_gross_emissions__Mg_CO2e_yr-1": [700],
                "gfw_forest_carbon_gross_removals__Mg_CO2_yr-1": [-350],
                "gfw_forest_carbon_net_flux__Mg_CO2e_yr-1": [350],
                "gfw_forest_carbon_gross_emissions_2001__Mg_CO2e": [70],
                "gfw_forest_carbon_gross_emissions_2002__Mg_CO2e": [75],
                "gfw_forest_carbon_gross_emissions_2003__Mg_CO2e": [80],
            })
        ])
        
        transformer = CarbonTransformer()
        result = transformer.transform(data)
        
        # Check that threshold 25 was filtered out
        thresholds = result["threshold"].unique()
        assert 25 not in thresholds.to_list()
        assert len(result) == 9  # Still only 9 rows
    
    def test_carbon_flux_status(self, sample_carbon_data):
        """Test carbon flux status categorization."""
        # Modify to have negative (sink) and positive (source) values
        data = sample_carbon_data.with_columns([
            pl.when(pl.col("umd_tree_cover_density_2000__threshold") == 30)
            .then(-100)  # Negative = sink
            .when(pl.col("umd_tree_cover_density_2000__threshold") == 50)
            .then(100)   # Positive = source
            .otherwise(0)  # Zero = neutral
            .alias("gfw_forest_carbon_net_flux__Mg_CO2e_yr-1")
        ])
        
        transformer = CarbonTransformer()
        result = transformer.transform(data)
        
        # Check status assignments
        sink = result.filter(pl.col("threshold") == 30).to_dicts()[0]
        assert sink["carbon_flux_status"] == "SINK"
        
        source = result.filter(pl.col("threshold") == 50).to_dicts()[0]
        assert source["carbon_flux_status"] == "SOURCE"
        
        neutral = result.filter(pl.col("threshold") == 75).to_dicts()[0]
        assert neutral["carbon_flux_status"] == "NEUTRAL"


class TestDataCleaner:
    """Test data cleaning operations."""
    
    def test_clean_country_names(self):
        """Test country name standardization."""
        df = pl.DataFrame({
            "country": ["USA", "UK", "DRC", "Brazil", "  Canada  "]
        })
        
        cleaner = DataCleaner()
        result = cleaner.clean_country_names(df)
        
        countries = result["country"].to_list()
        assert "United States" in countries
        assert "United Kingdom" in countries
        assert "Democratic Republic of the Congo" in countries  
        assert "Canada" in countries  # Trimmed
    
    def test_fix_negative_values(self):
        """Test fixing negative values."""
        df = pl.DataFrame({
            "tree_cover_loss_ha": [100, -50, 200],
            "carbon_net_flux_annual_avg": [100, -50, 200],  # Can be negative
        })
        
        cleaner = DataCleaner()
        result = cleaner.fix_negative_values(df, ["tree_cover_loss_ha", "carbon_net_flux_annual_avg"])
        
        # tree_cover_loss should have negative converted to null
        assert result["tree_cover_loss_ha"].to_list()[1] is None
        
        # net_flux should keep negative (it's allowed)
        assert result["carbon_net_flux_annual_avg"].to_list()[1] == -50


class TestDataValidator:
    """Test data validation."""
    
    def test_check_completeness(self):
        """Test completeness calculation."""
        df = pl.DataFrame({
            "col1": [1, 2, None, 4],
            "col2": [5, None, None, 8],
            "col3": [9, 10, 11, 12],
        })
        
        validator = DataValidator()
        completeness = validator.check_data_completeness(df)
        
        # 3 nulls out of 12 cells = 75% complete
        assert abs(completeness - 0.75) < 0.01
    
    def test_check_negative_values(self):
        """Test negative value detection."""
        df = pl.DataFrame({
            "loss_ha": [100, -50, 200],
            "net_flux": [100, -50, 200],
            "positive_only": [100, 50, 200],
        })
        
        validator = DataValidator()
        negatives = validator.check_negative_values(df, ["loss_ha", "positive_only"])
        
        assert "loss_ha" in negatives
        assert "positive_only" not in negatives


def test_pipeline_integration():
    """Simple integration test of the full pipeline."""
    # Create minimal sample data
    tree_cover_df = pl.DataFrame({
        "country": ["Brazil"],
        "threshold": [30],
        "extent_2000_ha": [100000],
        "tc_loss_ha_2020": [1000],
        "tc_loss_ha_2021": [1100],
    })
    
    # Clean
    cleaner = DataCleaner()
    cleaned = cleaner.clean_country_names(tree_cover_df)
    
    # Transform
    transformer = TreeCoverTransformer()
    transformed = transformer.transform(cleaned)
    
    # Validate
    validator = DataValidator()
    completeness = validator.check_data_completeness(transformed)
    
    # Basic assertions
    assert len(transformed) == 2  # 1 country × 2 years
    assert "Brazil" in transformed["country"].to_list()
    assert completeness > 0.9