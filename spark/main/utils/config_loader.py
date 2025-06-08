"""
Configuration Management Module

Loads YAML configuration files for the ETL pipeline, containing database connections,
Spark settings, and processing parameters.
"""

import yaml


def load_config():
    """
    Load configuration settings from main/configs/config.yml.
    
    Returns:
        dict: Configuration dictionary with database, spark, and processing settings
        
    Raises:
        Exception: If config file cannot be loaded or parsed
        
    Usage:
        config = load_config()
        db_host = config['database']['host']
    """
    try:
        with open("main/configs/config.yml", "r") as f:
            configs = yaml.safe_load(f)  # Safe YAML parsing
        return configs
    except Exception as e:
        raise Exception("Failed to load configs") from e