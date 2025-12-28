# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
Centralized metadata management system.
Handles both semantic (static) and runtime (dynamic) metadata.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, asdict
import threading

from nexus.config.settings import settings
# Constants now loaded from semantic.json

logger = logging.getLogger(__name__)


@dataclass
class SemanticMetadata:
    """Static metadata about the data model and query patterns."""
    
    # Data model metadata
    thresholds: List[int]
    tropical_countries: set
    year_ranges: Dict[str, tuple]
    
    # Query patterns and mappings
    sql_templates: Dict[str, str]
    nl_column_mappings: Dict[str, str]
    query_patterns: Dict[str, List[str]]
    
    # Table relationships
    table_schemas: Dict[str, Dict[str, Any]]
    join_conditions: Dict[str, List[str]]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['tropical_countries'] = list(self.tropical_countries)
        return data


@dataclass
class RuntimeMetadata:
    """Dynamic metadata about system state and performance."""
    
    # Database statistics
    row_counts: Dict[str, int]
    last_update: datetime
    data_quality: Dict[str, float]
    
    # Performance metrics
    avg_query_time: float
    cache_hit_rate: float
    error_rate: float
    
    # System state
    pipeline_version: str
    last_pipeline_run: Optional[datetime]
    validation_status: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        data['last_update'] = self.last_update.isoformat()
        if self.last_pipeline_run:
            data['last_pipeline_run'] = self.last_pipeline_run.isoformat()
        return data


class MetadataManager:
    """
    Centralized metadata management.
    
    This manager handles all metadata operations, providing a single source of truth
    for both static configuration and runtime statistics.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single metadata source."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize metadata manager."""
        if not hasattr(self, 'initialized'):
            self.semantic: Optional[SemanticMetadata] = None
            self.runtime: Optional[RuntimeMetadata] = None
            self.metadata_dir = Path(settings.CONFIG_DIR) / "metadata"
            self.metadata_dir.mkdir(parents=True, exist_ok=True)
            self.initialized = True
            self._load_metadata()
    
    def _load_metadata(self):
        """Load both semantic and runtime metadata."""
        self._load_semantic_metadata()
        self._load_runtime_metadata()
    
    def _load_semantic_metadata(self):
        """Load or create semantic metadata."""
        semantic_file = self.metadata_dir / "semantic.json"
        
        if semantic_file.exists():
            try:
                with open(semantic_file, 'r') as f:
                    data = json.load(f)
                    self.semantic = SemanticMetadata(
                        thresholds=data['thresholds'],
                        tropical_countries=set(data['tropical_countries']),
                        year_ranges=data['year_ranges'],
                        sql_templates=data['sql_templates'],
                        nl_column_mappings=data['nl_column_mappings'],
                        query_patterns=data['query_patterns'],
                        table_schemas=data['table_schemas'],
                        join_conditions=data['join_conditions']
                    )
                    logger.info("Loaded semantic metadata from file")
            except Exception as e:
                logger.error(f"Failed to load semantic metadata: {e}")
                self._create_default_semantic_metadata()
        else:
            self._create_default_semantic_metadata()
    
    def _create_default_semantic_metadata(self):
        """Create default semantic metadata."""
        self.semantic = SemanticMetadata(
            thresholds=[0, 10, 15, 20, 25, 30, 50, 75],
            tropical_countries=set(),  # Will be loaded from semantic.json
            year_ranges={
                "tree_cover": (2001, 2024),
                "primary_forest": (2002, 2024),
                "carbon": (2001, 2024)
            },
            sql_templates={},  # Will be loaded from semantic.json
            nl_column_mappings={},  # Will be loaded from semantic.json
            query_patterns={},  # Will be loaded from semantic.json
            table_schemas={
                "fact_tree_cover_loss": {
                    "columns": ["country", "year", "threshold", "tree_cover_loss_ha"],
                    "primary_key": ["country", "year", "threshold"],
                    "value_column": "tree_cover_loss_ha"
                },
                "fact_primary_forest": {
                    "columns": ["country", "year", "primary_forest_loss_ha"],
                    "primary_key": ["country", "year"],
                    "value_column": "primary_forest_loss_ha"
                },
                "fact_carbon": {
                    "columns": ["country", "year", "threshold", "carbon_emissions_mg_co2e"],
                    "primary_key": ["country", "year", "threshold"],
                    "value_column": "carbon_emissions_mg_co2e"
                }
            },
            join_conditions={
                "tree_cover_to_primary": ["t.country = p.country", "t.year = p.year"],
                "tree_cover_to_carbon": ["t.country = c.country", "t.year = c.year", "t.threshold = c.threshold"]
            }
        )
        self.save_semantic_metadata()
    
    def _load_runtime_metadata(self):
        """Load or create runtime metadata."""
        runtime_file = self.metadata_dir / "runtime.json"
        
        if runtime_file.exists():
            try:
                with open(runtime_file, 'r') as f:
                    data = json.load(f)
                    self.runtime = RuntimeMetadata(
                        row_counts=data['row_counts'],
                        last_update=datetime.fromisoformat(data['last_update']),
                        data_quality=data['data_quality'],
                        avg_query_time=data['avg_query_time'],
                        cache_hit_rate=data['cache_hit_rate'],
                        error_rate=data['error_rate'],
                        pipeline_version=data['pipeline_version'],
                        last_pipeline_run=datetime.fromisoformat(data['last_pipeline_run']) 
                            if data.get('last_pipeline_run') else None,
                        validation_status=data['validation_status']
                    )
                    logger.info("Loaded runtime metadata from file")
            except Exception as e:
                logger.error(f"Failed to load runtime metadata: {e}")
                self._create_default_runtime_metadata()
        else:
            self._create_default_runtime_metadata()
    
    def _create_default_runtime_metadata(self):
        """Create default runtime metadata."""
        self.runtime = RuntimeMetadata(
            row_counts={
                "fact_tree_cover_loss": 0,
                "fact_primary_forest": 0,
                "fact_carbon": 0
            },
            last_update=datetime.now(),
            data_quality={
                "completeness": 0.0,
                "accuracy": 0.0,
                "consistency": 0.0
            },
            avg_query_time=0.0,
            cache_hit_rate=0.0,
            error_rate=0.0,
            pipeline_version="1.0.0",
            last_pipeline_run=None,
            validation_status={
                "fact_tree_cover_loss": "UNKNOWN",
                "fact_primary_forest": "UNKNOWN", 
                "fact_carbon": "UNKNOWN"
            }
        )
        self.save_runtime_metadata()
    
    def save_semantic_metadata(self):
        """Persist semantic metadata to disk."""
        semantic_file = self.metadata_dir / "semantic.json"
        with open(semantic_file, 'w') as f:
            json.dump(self.semantic.to_dict(), f, indent=2)
        logger.debug("Saved semantic metadata")
    
    def save_runtime_metadata(self):
        """Persist runtime metadata to disk."""
        runtime_file = self.metadata_dir / "runtime.json"
        with open(runtime_file, 'w') as f:
            json.dump(self.runtime.to_dict(), f, indent=2)
        logger.debug("Saved runtime metadata")
    
    def update_runtime_stats(self, stats: Dict[str, Any]):
        """
        Update runtime statistics.
        
        Args:
            stats: Dictionary of statistics to update
        """
        if 'row_counts' in stats:
            self.runtime.row_counts.update(stats['row_counts'])
        
        if 'validation_status' in stats:
            self.runtime.validation_status.update(stats['validation_status'])
        
        if 'data_quality' in stats:
            self.runtime.data_quality.update(stats['data_quality'])
        
        if 'pipeline_run' in stats:
            self.runtime.last_pipeline_run = datetime.now()
            self.runtime.pipeline_version = stats.get('version', '1.0.0')
        
        self.runtime.last_update = datetime.now()
        self.save_runtime_metadata()
        
    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Table metadata including schema and statistics
        """
        return {
            "schema": self.semantic.table_schemas.get(table_name, {}),
            "row_count": self.runtime.row_counts.get(table_name, 0),
            "validation": self.runtime.validation_status.get(table_name, "UNKNOWN")
        }
    
    def get_query_patterns(self, intent: str) -> List[str]:
        """
        Get query patterns for a specific intent.
        
        Args:
            intent: Query intent type
            
        Returns:
            List of matching patterns
        """
        return self.semantic.query_patterns.get(intent, [])
    
    def get_sql_template(self, template_name: str) -> Optional[str]:
        """
        Get SQL template by name.
        
        Args:
            template_name: Name of the template
            
        Returns:
            SQL template string or None
        """
        return self.semantic.sql_templates.get(template_name)
    
    def is_tropical_country(self, country: str) -> bool:
        """
        Check if a country is tropical.
        
        Args:
            country: Country name
            
        Returns:
            True if tropical
        """
        return country in self.semantic.tropical_countries
    
    def get_valid_thresholds(self, table_type: str = "tree_cover") -> List[int]:
        """
        Get valid thresholds for a table type.
        
        Args:
            table_type: Type of table
            
        Returns:
            List of valid thresholds
        """
        if table_type == "carbon":
            return [30, 50, 75]
        elif table_type == "primary_forest":
            return [30]
        else:
            return self.semantic.thresholds
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """Get current performance metrics."""
        return {
            "avg_query_time_ms": self.runtime.avg_query_time,
            "cache_hit_rate": self.runtime.cache_hit_rate,
            "error_rate": self.runtime.error_rate
        }


# Global singleton instance
metadata_manager = MetadataManager()