import yaml
import os
from typing import Dict, Any

class ConfigLoader:
    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        config_path = os.path.join(project_root, 'config.yml')
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

def load_config():
    return ConfigLoader().config

CONFIG = load_config()