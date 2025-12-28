"""
Unit tests for data validators.
"""
import pytest
import polars as pl

from nexus.data.pipeline.validators import DataValidator, ValidationResult
from nexus.data.metadata.metadata_manager import metadata_manager

TROPICAL_COUNTRIES = metadata_manager.semantic.tropical_countries
CARBON_THRESHOLDS = [30, 50, 75]
PRIMARY_THRESHOLD = 30
ALL_THRESHOLDS = metadata_manager.semantic.thresholds


class TestValidationResult:
    """Test ValidationResult dataclass."""
    
    def test_validation_result_creation(self):
        """Test creating validation results."""
        result = ValidationResult(
            passed=True,
            message="Test passed",
            severity="info",
            details={"test": "data"}
        )
        
        assert result.passed is True
        assert result.severity == "info"
        assert result.details["test"] == "data"
        assert str(result) == "[INFO] Test passed"


class TestDataValidatorTreeCover:
    """Test tree cover validation."""
    
    @pytest.fixture
    def sample_tree_cover(self):
        """Create sample tree cover data."""
        return pl.DataFrame({
            'country': ['Brazil'] * 8,
            'year': [2021] * 8,
            'threshold': [0, 10, 15, 20, 25, 30, 50, 75],
            'tree_cover_loss_ha': [100, 110, 120, 130, 140, 150, 160, 170],
            'extent_2000_ha': [1000] * 8
        })
    
    def test_validate_tree_cover_valid(self, sample_tree_cover):
        """Test validation of valid tree cover data."""
        validator = DataValidator()
        results = validator.validate_tree_cover(sample_tree_cover)
        
        # Check that we get results
        assert len(results) > 0
        
        # Check for required columns validation
        column_check = [r for r in results if "required columns" in r.message]
        assert len(column_check) > 0
        assert column_check[0].passed
    
    def test_validate_tree_cover_invalid_threshold(self):
        """Test detection of invalid thresholds."""
        df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2021],
            'threshold': [40],  # Invalid threshold!
            'tree_cover_loss_ha': [100],
            'extent_2000_ha': [1000]
        })
        
        validator = DataValidator()
        results = validator.validate_tree_cover(df)
        
        # Should have an error about invalid threshold
        threshold_errors = [r for r in results if "Invalid thresholds" in r.message]
        assert len(threshold_errors) > 0
        assert threshold_errors[0].severity == "error"
    
    def test_validate_tree_cover_negative_values(self):
        """Test detection of negative values."""
        df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2021],
            'threshold': [30],
            'tree_cover_loss_ha': [-100],  # Negative!
            'extent_2000_ha': [1000]
        })
        
        validator = DataValidator()
        results = validator.validate_tree_cover(df)
        
        # Should detect negative values
        neg_errors = [r for r in results if "Negative values" in r.message]
        assert len(neg_errors) > 0
        assert neg_errors[0].severity == "error"


class TestDataValidatorPrimaryForest:
    """Test primary forest validation."""
    
    @pytest.fixture
    def sample_primary(self):
        """Create sample primary forest data."""
        return pl.DataFrame({
            'country': ['Brazil', 'Indonesia'],
            'year': [2022, 2022],
            'threshold': [30, 30],
            'primary_forest_loss_ha': [500, 400],
            'is_tropical': [True, True]
        })
    
    def test_validate_primary_forest_valid(self, sample_primary):
        """Test validation of valid primary forest data."""
        validator = DataValidator()
        results = validator.validate_primary_forest(sample_primary)
        
        # Should have info about all countries being tropical
        tropical_checks = [r for r in results if "tropical" in r.message.lower()]
        assert len(tropical_checks) > 0
    
    def test_validate_primary_non_tropical(self):
        """Test detection of non-tropical countries."""
        df = pl.DataFrame({
            'country': ['Canada', 'Brazil'],  # Canada is not tropical!
            'year': [2022, 2022],
            'primary_forest_loss_ha': [100, 500]
        })
        
        validator = DataValidator()
        results = validator.validate_primary_forest(df)
        
        # Should detect Canada as non-tropical
        non_tropical = [r for r in results if "Non-tropical" in r.message]
        assert len(non_tropical) > 0
        assert non_tropical[0].severity == "error"
        assert "Canada" in str(non_tropical[0].details)
    
    def test_validate_primary_wrong_threshold(self):
        """Test detection of wrong threshold."""
        df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2022],
            'threshold': [50],  # Should be 30!
            'primary_forest_loss_ha': [500]
        })
        
        validator = DataValidator()
        results = validator.validate_primary_forest(df)
        
        # Should detect wrong threshold
        threshold_errors = [r for r in results if "threshold should be 30" in r.message]
        assert len(threshold_errors) > 0
        assert threshold_errors[0].severity == "error"


class TestDataValidatorCarbon:
    """Test carbon data validation."""
    
    def test_validate_carbon_valid_thresholds(self):
        """Test validation of carbon thresholds."""
        df = pl.DataFrame({
            'country': ['Brazil'] * 3,
            'year': [2021] * 3,
            'threshold': [30, 50, 75],  # Valid carbon thresholds
            'carbon_emissions_mg_co2e': [100, 90, 80],
            'carbon_net_flux_annual_avg': [50, 40, -10]  # Last one is a sink
        })
        
        validator = DataValidator()
        results = validator.validate_carbon(df)
        
        # Should pass threshold check
        threshold_checks = [r for r in results if "thresholds correct" in r.message]
        assert len(threshold_checks) > 0
        assert threshold_checks[0].passed
        
        # Should detect carbon sink
        sink_info = [r for r in results if "carbon sinks" in r.message]
        assert len(sink_info) > 0
    
    def test_validate_carbon_invalid_thresholds(self):
        """Test detection of invalid carbon thresholds."""
        df = pl.DataFrame({
            'country': ['Brazil'] * 4,
            'year': [2021] * 4,
            'threshold': [25, 30, 50, 75],  # 25 is invalid for carbon!
            'carbon_emissions_mg_co2e': [100, 90, 80, 70]
        })
        
        validator = DataValidator()
        results = validator.validate_carbon(df)
        
        # Should detect invalid threshold
        threshold_errors = [r for r in results if "Carbon thresholds should be" in r.message]
        assert len(threshold_errors) > 0
        assert threshold_errors[0].severity == "error"


class TestDataValidatorRelationships:
    """Test cross-table relationship validation."""
    
    def test_validate_relationships_valid(self):
        """Test valid relationships between tables."""
        tree_cover_df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2022],
            'threshold': [30],
            'tree_cover_loss_ha': [1000]
        })
        
        primary_forest_df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2022],
            'primary_forest_loss_ha': [500]  # Less than total - valid
        })
        
        validator = DataValidator()
        result = validator.validate_relationships(tree_cover_df, primary_forest_df)
        
        assert result.passed
        assert "correctly bounded" in result.message
    
    def test_validate_relationships_invalid(self):
        """Test invalid relationships (primary > total)."""
        tree_cover_df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2022],
            'threshold': [30],
            'tree_cover_loss_ha': [100]
        })
        
        primary_forest_df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2022],
            'primary_forest_loss_ha': [200]  # More than total - invalid!
        })
        
        validator = DataValidator()
        result = validator.validate_relationships(tree_cover_df, primary_forest_df)
        
        assert not result.passed
        assert result.severity == "error"
        assert "exceeds total forest loss" in result.message


def test_validate_all():
    """Test the validate_all method."""
    validator = DataValidator()
    
    # Create minimal valid data
    tree_cover = pl.DataFrame({
        'country': ['Brazil'],
        'year': [2021],
        'threshold': [30],
        'tree_cover_loss_ha': [100],
        'extent_2000_ha': [1000]
    })
    
    primary = pl.DataFrame({
        'country': ['Brazil'],
        'year': [2022],
        'threshold': [30],
        'primary_forest_loss_ha': [50]
    })
    
    carbon = pl.DataFrame({
        'country': ['Brazil'],
        'year': [2021],
        'threshold': [30],
        'carbon_emissions_mg_co2e': [100]
    })
    
    success, results = validator.validate_all(tree_cover, primary, carbon)
    
    # Should have multiple validation results
    assert len(results) > 0
    
    # Check if any errors (determines success)
    has_errors = any(r.severity == "error" for r in results)
    assert success == (not has_errors)