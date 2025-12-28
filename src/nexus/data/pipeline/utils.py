"""
Utility functions for data processing pipeline.
Streamlined for actual usage with 7MB dataset.
"""
import logging
import time
from functools import wraps
from typing import Dict, Any, List, Optional
import polars as pl
from pathlib import Path

logger = logging.getLogger(__name__)


def timer(func):
    """
    Decorator to time function execution.
    
    Usage:
        @timer
        def my_function():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"{func.__name__} took {elapsed:.2f} seconds")
        return result
    return wrapper


def log_dataframe_info(df: pl.DataFrame, name: str = "DataFrame"):
    """
    Log comprehensive information about a DataFrame.
    
    Args:
        df: DataFrame to log
        name: Name for logging
    """
    logger.info(f"\n{name} Info:")
    logger.info(f"  Shape: {len(df):,} rows × {len(df.columns)} columns")
    logger.info(f"  Memory: ~{df.estimated_size('mb'):.1f} MB")
    
    # Column types summary
    type_counts = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        type_counts[dtype] = type_counts.get(dtype, 0) + 1
    logger.info(f"  Column types: {type_counts}")
    
    # Null counts (only show columns with nulls)
    null_cols = [col for col in df.columns if df[col].null_count() > 0]
    if null_cols:
        logger.info(f"  Columns with nulls: {len(null_cols)}/{len(df.columns)}")
        for col in null_cols[:5]:  # Show first 5 columns with nulls
            logger.info(f"    {col}: {df[col].null_count()} nulls")


def create_summary_statistics(df: pl.DataFrame) -> Dict[str, Any]:
    """
    Create summary statistics for a DataFrame.
    
    Args:
        df: DataFrame to summarize
        
    Returns:
        Dictionary of statistics
    """
    stats = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": df.columns,
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "null_counts": {},
        "numeric_summary": {}
    }
    
    # Null counts (only for columns with nulls)
    for col in df.columns:
        null_count = df[col].null_count()
        if null_count > 0:
            stats["null_counts"][col] = null_count
    
    # Basic numeric summaries (only for numeric columns)
    numeric_cols = [
        col for col in df.columns
        if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]
    ]
    
    for col in numeric_cols[:10]:  # Limit to first 10 to avoid bloat
        try:
            col_stats = df[col].drop_nulls()
            if len(col_stats) > 0:
                stats["numeric_summary"][col] = {
                    "min": float(col_stats.min()),
                    "max": float(col_stats.max()),
                    "mean": float(col_stats.mean()),
                    "nulls": df[col].null_count(),
                }
        except Exception:
            # Skip columns that can't be summarized
            pass
    
    return stats


def validate_dataframe(df: pl.DataFrame, expected_cols: List[str], 
                       name: str = "DataFrame") -> bool:
    """
    Basic DataFrame validation.
    
    Args:
        df: DataFrame to validate
        expected_cols: Expected column names
        name: Name for logging
        
    Returns:
        True if valid, False otherwise
    """
    if df.is_empty():
        logger.error(f"{name} is empty")
        return False
    
    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        logger.error(f"{name} missing columns: {missing_cols}")
        return False
    
    logger.info(f"{name} validated: {len(df):,} rows")
    return True


def save_results(df: pl.DataFrame, output_path: Path, name: str = "results"):
    """
    Save DataFrame to parquet with logging.
    
    Args:
        df: DataFrame to save
        output_path: Path to save file
        name: Name for logging
    """
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        file_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Saved {name}: {output_path} ({file_size_mb:.1f} MB)")
    except Exception as e:
        logger.error(f"Failed to save {name}: {e}")
        raise