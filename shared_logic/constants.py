"""
System-wide constants for the energy data pipeline.
Centralizing these values ensures consistency across extraction and processing modules.
"""

# Core Strategy Defaults
DEFAULT_COUNTRY = 'BE'
DEFAULT_FREQ_GRID = '15min'

# Cross-Border Dynamics
# Fixed neighbors for BE
BE_NEIGHBORS = ['FR', 'NL', 'DE_LU', 'GB']

# Default cross-border pair for NTC / scheduled exchange queries
DEFAULT_FLOW_FROM = 'FR'
DEFAULT_FLOW_TO = 'DE_LU'

# Execution Environments
DEFAULT_TIMEZONE = 'Europe/Brussels'

# API Resilience Configuration
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 2
RETRY_WAIT_MAX = 10

# Balancing / Reserve Query Parameters
# A51 = Manual Frequency Restoration Reserve (mFRR)
DEFAULT_PROCESS_TYPE = 'A51'
# A01 = Day Ahead
DEFAULT_MARKET_AGREEMENT_TYPE = 'A01'
