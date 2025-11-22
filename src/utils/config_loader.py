"""
Configuration Loader
Utility for loading and managing configuration
"""

import yaml
import os
from typing import Dict, Any


class ConfigLoader:
    """Load and manage configuration"""
    
    def __init__(self, config_path: str = 'config.yaml'):
        self.config_path = config_path
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get(self, key: str, default=None):
        """Get configuration value by key"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self.config.get('pipeline', {})
    
    def get_pubsub_config(self) -> Dict[str, Any]:
        """Get Pub/Sub configuration"""
        return self.config.get('pubsub', {})
    
    def get_bigquery_config(self) -> Dict[str, Any]:
        """Get BigQuery configuration"""
        return self.config.get('bigquery', {})
    
    def get_data_config(self) -> Dict[str, Any]:
        """Get data configuration"""
        return self.config.get('data', {})
