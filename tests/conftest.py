"""
Global pytest configuration and fixtures.
"""
import os
import sys
import pytest
import sqlite3
import tempfile
from pathlib import Path
from typing import Generator

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Configure pytest
pytest_plugins = []


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def test_data_dir(project_root) -> Path:
    """Return the test data directory."""
    return project_root / "tests" / "data"


@pytest.fixture(scope="session")
def database_path() -> Path:
    """Get the database path from environment or default location."""
    # Try environment variable first
    db_path_env = os.environ.get("DATABASE_PATH")
    if db_path_env:
        return Path(db_path_env)

    # Try default locations
    possible_paths = [
        Path("/app/data/processed/forest.db"),  # Docker path
        Path("data/processed/forest.db"),  # Local path
        Path("../data/processed/forest.db"),  # Alternative local path
    ]

    for path in possible_paths:
        if path.exists():
            return path

    # If no database found, skip tests that require it
    pytest.skip("Database not found in any expected location")


@pytest.fixture(scope="function")
def db_connection(database_path) -> Generator[sqlite3.Connection, None, None]:
    """
    Provide a database connection for integration tests.

    This fixture checks if the database exists and provides a connection.
    The connection is automatically closed after the test.
    """
    if not database_path.exists():
        pytest.skip(f"Database not found at {database_path}")

    # Create connection without special modes to avoid issues
    try:
        conn = sqlite3.connect(str(database_path))
        conn.row_factory = sqlite3.Row

        # Test the connection with a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()

        yield conn
    except sqlite3.OperationalError as e:
        pytest.skip(f"Cannot connect to database: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


@pytest.fixture(scope="function")
def in_memory_db() -> Generator[sqlite3.Connection, None, None]:
    """
    Create an in-memory SQLite database for unit tests.

    This is faster than file-based databases and doesn't require
    the actual database to exist.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create minimal schema for testing
    cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE fact_tree_cover_loss (
            country TEXT NOT NULL,
            year INTEGER NOT NULL,
            threshold INTEGER NOT NULL,
            tree_cover_loss_ha REAL,
            extent_2000_ha REAL,
            extent_2010_ha REAL,
            extent_2020_ha REAL,
            loss_rate_pct REAL,
            data_quality TEXT,
            updated_at TEXT,
            PRIMARY KEY (country, year, threshold)
        );

        CREATE TABLE fact_primary_forest (
            country TEXT NOT NULL,
            year INTEGER NOT NULL,
            threshold INTEGER DEFAULT 30,
            primary_forest_loss_ha REAL,
            is_tropical_country INTEGER,
            loss_status TEXT,
            PRIMARY KEY (country, year)
        );

        CREATE TABLE fact_carbon (
            country TEXT NOT NULL,
            year INTEGER NOT NULL,
            threshold INTEGER NOT NULL,
            gross_emissions_co2e_mt REAL,
            gross_removals_co2e_mt REAL,
            net_flux_co2e_mt REAL,
            carbon_flux_status TEXT,
            data_quality TEXT,
            updated_at TEXT,
            PRIMARY KEY (country, year, threshold)
        );

        -- Insert minimal test data
        INSERT INTO fact_tree_cover_loss VALUES
            ('Brazil', 2023, 30, 1000.0, 50000.0, 49000.0, 48000.0, 2.0, 'high', '2024-01-01'),
            ('Indonesia', 2023, 30, 500.0, 30000.0, 29500.0, 29000.0, 1.67, 'high', '2024-01-01');

        INSERT INTO fact_primary_forest VALUES
            ('Brazil', 2023, 30, 800.0, 1, 'loss'),
            ('Indonesia', 2023, 30, 400.0, 1, 'loss');

        INSERT INTO fact_carbon VALUES
            ('Brazil', 2023, 30, 100.0, -50.0, 50.0, 'source', 'high', '2024-01-01'),
            ('Indonesia', 2023, 30, 50.0, -30.0, 20.0, 'source', 'high', '2024-01-01');
    """)

    yield conn
    conn.close()


@pytest.fixture
def temp_db_path(tmp_path) -> Path:
    """Create a temporary database file path for testing."""
    return tmp_path / "test.db"


@pytest.fixture
def sample_tree_cover_data():
    """Sample tree cover data for testing."""
    return {
        "country": ["Brazil", "Indonesia", "Congo"],
        "year": [2023, 2023, 2023],
        "threshold": [30, 30, 30],
        "tree_cover_loss_ha": [1000.0, 500.0, 300.0],
        "extent_2000_ha": [50000.0, 30000.0, 20000.0],
    }


@pytest.fixture
def sample_primary_forest_data():
    """Sample primary forest data for testing."""
    return {
        "country": ["Brazil", "Indonesia", "Congo"],
        "year": [2023, 2023, 2023],
        "primary_forest_loss_ha": [800.0, 400.0, 250.0],
        "is_tropical": [True, True, True],
    }


@pytest.fixture
def sample_carbon_data():
    """Sample carbon data for testing."""
    return {
        "country": ["Brazil", "Indonesia", "Congo"],
        "year": [2023, 2023, 2023],
        "threshold": [30, 30, 30],
        "gross_emissions_co2e_mt": [100.0, 50.0, 30.0],
        "gross_removals_co2e_mt": [-50.0, -30.0, -20.0],
        "net_flux_co2e_mt": [50.0, 20.0, 10.0],
    }