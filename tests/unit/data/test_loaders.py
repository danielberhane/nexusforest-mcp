"""
Unit tests for Excel data loader - simplified version.
"""
import pytest
import polars as pl
from pathlib import Path

from nexus.data.pipeline.loaders import DataValidator


class TestDataValidator:
    """Test data validation functionality only."""
    
    def test_validate_columns(self):
        """Test column validation."""
        df = pl.DataFrame({
            'country': ['Brazil'],
            'year': [2023],
            'value': [100]
        })
        
        validator = DataValidator()
        
        # Should pass with all columns present
        assert validator.validate_columns(df, ['country', 'year'])
        
        # Should fail with missing columns
        assert not validator.validate_columns(df, ['country', 'missing_col'])
    
    def test_check_data_completeness(self):
        """Test completeness checking."""
        df = pl.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': [5, None, 7, 8],
            'col3': [9, 10, 11, 12]
        })
        
        validator = DataValidator()
        completeness = validator.check_data_completeness(df)
        
        # 2 nulls out of 12 cells = 83.3% complete
        assert 0.83 < completeness < 0.84