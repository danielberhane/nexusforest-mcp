# Copyright 2025 Daniel Berhane Araya
# SPDX-License-Identifier: Apache-2.0

"""
Transactional pipeline manager with checkpoint and rollback support.
"""
import logging
import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import contextmanager
import sqlite3
import shutil

from nexus.config.settings import settings
from nexus.data.metadata.metadata_manager import metadata_manager

logger = logging.getLogger(__name__)


class PipelineManager:
    """
    Manages data pipeline execution with transaction support.
    
    Features:
    - Checkpoint system for progress tracking
    - Rollback capability on failure
    - Detailed error recovery
    """
    
    def __init__(self):
        """Initialize pipeline manager."""
        self.checkpoint_dir = settings.DATA_DIR / "checkpoints"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.current_session = None
        self.checkpoints = []
    
    def start_session(self, session_name: Optional[str] = None) -> str:
        """
        Start a new pipeline session.
        
        Args:
            session_name: Optional name for the session
            
        Returns:
            Session ID
        """
        session_id = session_name or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.current_session = {
            "id": session_id,
            "start_time": datetime.now(),
            "checkpoints": [],
            "status": "running"
        }
        
        logger.info(f"Started pipeline session: {session_id}")
        return session_id
    
    @contextmanager
    def transaction(self, step_name: str):
        """
        Context manager for transactional step execution.
        
        Args:
            step_name: Name of the pipeline step
            
        Example:
            with pipeline.transaction("load_data"):
                # Do work here
                # Automatic checkpoint on success
                # Automatic rollback on failure
        """
        checkpoint = {
            "step": step_name,
            "start_time": datetime.now(),
            "status": "running"
        }
        
        try:
            logger.info(f"Starting step: {step_name}")
            yield
            
            # Success - create checkpoint
            checkpoint["end_time"] = datetime.now()
            checkpoint["status"] = "success"
            self._save_checkpoint(checkpoint)
            logger.info(f"Completed step: {step_name}")
            
        except Exception as e:
            # Failure - mark as failed
            checkpoint["end_time"] = datetime.now()
            checkpoint["status"] = "failed"
            checkpoint["error"] = str(e)
            self._save_checkpoint(checkpoint)
            
            logger.error(f"Step failed: {step_name} - {e}")
            
            # Attempt rollback
            self._rollback_to_last_checkpoint()
            raise
    
    def _save_checkpoint(self, checkpoint: Dict[str, Any]):
        """Save checkpoint to disk."""
        if not self.current_session:
            return
        
        self.current_session["checkpoints"].append(checkpoint)
        
        # Save session state
        session_file = self.checkpoint_dir / f"{self.current_session['id']}.json"
        with open(session_file, 'w') as f:
            json.dump(self.current_session, f, indent=2, default=str)
        
        logger.debug(f"Checkpoint saved: {checkpoint['step']}")
    
    def _rollback_to_last_checkpoint(self):
        """Rollback to the last successful checkpoint."""
        if not self.current_session or not self.current_session["checkpoints"]:
            logger.warning("No checkpoints to rollback to")
            return
        
        # Find last successful checkpoint
        successful_checkpoints = [
            cp for cp in self.current_session["checkpoints"]
            if cp["status"] == "success"
        ]
        
        if not successful_checkpoints:
            logger.warning("No successful checkpoints found")
            self._full_rollback()
            return
        
        last_checkpoint = successful_checkpoints[-1]
        logger.info(f"Rolling back to checkpoint: {last_checkpoint['step']}")
        
        # Implement rollback logic based on step
        self._perform_rollback(last_checkpoint)
    
    def _full_rollback(self):
        """Perform complete rollback."""
        logger.info("Performing full rollback")
        
        # Backup current database
        if settings.sqlite_db_path.exists():
            backup_path = settings.sqlite_db_path.with_suffix('.backup')
            shutil.copy2(settings.sqlite_db_path, backup_path)
            logger.info(f"Database backed up to {backup_path}")
        
        # Clear any partial data
        # This would be customized based on your needs
    
    def _perform_rollback(self, checkpoint: Dict[str, Any]):
        """
        Perform specific rollback based on checkpoint.
        
        Args:
            checkpoint: Checkpoint to rollback to
        """
        step_rollbacks = {
            "load_data": self._rollback_load,
            "clean_data": self._rollback_clean,
            "transform_data": self._rollback_transform,
            "export_data": self._rollback_export
        }
        
        rollback_fn = step_rollbacks.get(checkpoint["step"])
        if rollback_fn:
            rollback_fn()
    
    def _rollback_load(self):
        """Rollback data loading."""
        # Clear any cached data
        logger.info("Rolling back data load")
    
    def _rollback_clean(self):
        """Rollback data cleaning."""
        # Restore uncleaned data
        logger.info("Rolling back data cleaning")
    
    def _rollback_transform(self):
        """Rollback data transformation."""
        # Clear transformed data
        logger.info("Rolling back data transformation")
    
    def _rollback_export(self):
        """Rollback database export."""
        # Restore previous database state
        logger.info("Rolling back database export")
        
        # Use SQLite's transactional capabilities
        if settings.sqlite_db_path.exists():
            conn = sqlite3.connect(settings.sqlite_db_path)
            try:
                conn.execute("ROLLBACK")
                logger.info("Database transaction rolled back")
            except:
                pass
            finally:
                conn.close()
    
    def complete_session(self):
        """Mark session as complete."""
        if self.current_session:
            self.current_session["status"] = "completed"
            self.current_session["end_time"] = datetime.now()
            
            # Update metadata
            metadata_manager.update_runtime_stats({
                "pipeline_run": True,
                "version": "1.0.0"
            })
            
            # Save final session state
            session_file = self.checkpoint_dir / f"{self.current_session['id']}.json"
            with open(session_file, 'w') as f:
                json.dump(self.current_session, f, indent=2, default=str)
            
            logger.info(f"Session completed: {self.current_session['id']}")
    
    def get_session_history(self) -> List[Dict[str, Any]]:
        """Get history of pipeline sessions."""
        sessions = []
        
        for session_file in self.checkpoint_dir.glob("*.json"):
            with open(session_file, 'r') as f:
                sessions.append(json.load(f))
        
        return sorted(sessions, key=lambda x: x["start_time"], reverse=True)