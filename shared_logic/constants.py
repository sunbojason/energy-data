"""
System-wide constants for the energy data pipeline.
Centralizing these values ensures consistency across extraction and processing modules.
"""

# Core Strategy Defaults
DEFAULT_COUNTRY = 'NL'
DEFAULT_FREQ_GRID = '15min'

# Cross-Border Dynamics
# Fixed neighbors for NL (Removed 'FR', added BE, NO, DK)
NL_NEIGHBORS = ['BE', 'DE_LU', 'GB', 'NO', 'DK']

# Execution Environments
DEFAULT_TIMEZONE = 'Europe/Amsterdam'

# API Resilience Configuration
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 2
RETRY_WAIT_MAX = 10
