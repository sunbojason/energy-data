import json
import os
import pytest
import logging
import warnings

@pytest.fixture(scope="session", autouse=True)
def load_azure_settings():
    """
    Automatically runs before any tests.
    Parses local.settings.json and injects 'Values' into os.environ.
    """
    # Locate local.settings.json relative to this file
    base_dir = os.path.dirname(os.path.dirname(__file__))
    settings_path = os.path.join(base_dir, 'local.settings.json')
    
    if os.path.exists(settings_path):
        try:
            with open(settings_path, 'r') as f:
                settings = json.load(f)
                values = settings.get("Values", {})
                for key, value in values.items():
                    # Only set if not already in environment to avoid conflicts
                    if key not in os.environ:
                        os.environ[key] = str(value)
        except Exception as e:
            logging.error(f"Failed to parse local.settings.json: {e}")
    else:
        logging.warning(f"local.settings.json not found at {settings_path}")