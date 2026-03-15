[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/)
[![Azure Functions](https://img.shields.io/badge/azure-functions-purple)](https://docs.microsoft.com/azure/azure-functions/)
[![License](https://img.shields.io/badge/license-MIT-green)](#)

# ⚡ Netherlands (NL) Energy Market Data Pipeline

An automated, serverless data pipeline on **Azure** that collects, cleans, and stores comprehensive energy market data for the **Netherlands (NL)** from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).

---

## 🚀 Quick Start

1. **Fork or clone** the repository.
2. **Create a Python 3.12+ virtual environment** and activate it.
3. **Install dependencies** with `pip install -r requirements.txt`.
4. **Populate `local.settings.json`** with your storage and ENTSO-E API credentials.
5. **Run locally** via `func start` and observe CSVs being processed into the `cleaned-data` container.
6. **Visualize data** by running `python scripts/visualize_prices.py` to see dual-axis charts of NL Day-Ahead prices and actual system load.

For detailed setup instructions, see the **Getting Started** section below.

---

## 📐 Architecture Overview

This project follows a **Medallion-lite architecture** orchestrated by Azure Functions:

```text
ENTSO-E REST API
       │
       ▼  (Timer Trigger — scheduled daily at 02:00 AM)
┌─────────────────────┐
│  Ingestion Function │  ── fetches comprehensive NL market data via entsoe-py
└─────────────────────┘
       │
       ▼  CSV → raw-data (Blob Storage)
┌─────────────────────┐
│ Processing Function │  ── triggered by Blob arrival; resamples to 15-min freq
└─────────────────────┘
       │
       ▼  CSV → cleaned-data (Blob Storage)
┌─────────────────────┐
│ Azure Data Factory  │  ── upserts cleaned CSVs
└─────────────────────┘
       │
       ▼
  Azure SQL Database
  └── table: entsoe
```

### Data Flow

| Stage | Trigger | Input | Output |
|---|---|---|---|
| **Ingestion** | Timer (02:00 AM daily) | ENTSO-E REST API | `raw-data/` container (CSV) |
| **Processing** | Blob Trigger (new file) | `raw-data/` CSV | `cleaned-data/` container (CSV, 15-min resolution) |
| **Warehousing** | ADF Pipeline | `cleaned-data/` CSV | `entsoe` SQL table (upsert) |

---

## 🗂️ Project Structure

```text
energy-data/
├── function_app.py          # Azure Functions app entry point (V2 model)
├── shared_logic/
│   ├── entsoe_client.py     # ENTSO-E API fetching (prices, load, flows) with retry logic
│   ├── cleaning_service.py  # Data cleaning, timezone enforcement, 15-min resampling
│   ├── constants.py         # Shared constants (container names, regions, etc.)
│   └── __init__.py
├── scripts/
│   ├── visualize_prices.py  # Utility script for data visualization (Matplotlib)
│   └── __init__.py
├── tests/                   # Unit tests and integration tests
│   ├── conftest.py          # pytest configuration and fixtures
│   ├── test_entsoe_client.py
│   ├── test_cleaning_service.py
│   ├── test_function_app.py
│   ├── test_integration_test_api.py
│   ├── test_upload.py
│   └── __init__.py
├── .vscode/
│   └── extensions.json      # Recommended VS Code extensions
├── host.json                # Azure Functions host configuration
├── local.settings.json      # Local environment variables (not committed)
├── pyproject.toml           # Python project config and pytest settings
├── requirements.txt         # Python dependencies
└── .funcignore              # Files ignored by Azure Functions deployment
```

> ⚙️ Business logic is intentionally isolated from Azure bindings to simplify testing and reusability.

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| **Language** | Python 3.12+ |
| **Serverless Framework** | Azure Functions — Python V2 Programming Model |
| **API Client** | `entsoe-py` |
| **Data Processing & Viz**| `pandas`, `numpy`, `matplotlib` |
| **Network Resilience** | `tenacity` |
| **Cloud Storage I/O** | `azure-storage-blob` |
| **Identity & Auth** | `azure-identity` (Managed Identity) |
| **Warehouse Ingestion** | Azure Data Factory |
| **Database** | Azure SQL Database |
| **Hosting Plan** | Flex Consumption |
| **Local Dev** | VS Code + Azure Functions Core Tools |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local)
- An active [ENTSO-E API key](https://transparency.entsoe.eu/usrm/user/myAccountSettings)
- Azure Storage Account with `raw-data` and `cleaned-data` containers

### Local Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/sunbojason/energy-data.git
   cd energy-data
   ```

2. **Create and activate a virtual environment**

   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure local settings**

   Populate `local.settings.json` with your Azure Storage and ENTSO-E credentials:

   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "FUNCTIONS_WORKER_RUNTIME": "python",
       "AzureWebJobsStorage": "<your-storage-account-connection-string>",
       "STORAGE_ACCOUNT_NAME": "<your-storage-account-name>",
       "RAW_DATA_CONTAINER": "raw-data",
       "ENTSOE_API_KEY": "<your-entsoe-api-key>"
     }
   }
   ```

   **Configuration keys:**
   - `FUNCTIONS_WORKER_RUNTIME` — Python runtime identifier
   - `AzureWebJobsStorage` — Full connection string to your storage account
   - `STORAGE_ACCOUNT_NAME` — Storage account name (used for blob client initialization)
   - `RAW_DATA_CONTAINER` — Container name for raw ingested data
   - `ENTSOE_API_KEY` — Your ENTSO-E Transparency Platform API key

   > ⚠️ `local.settings.json` is git-ignored. Never commit secrets or connection strings to source control.

5. **Run locally**

   ```bash
   func start
   ```
   Since the ingestion is normally triggered by a timer, use the following Administrative API call to trigger it immediately for testing:
   ```bash
   curl -v -X POST http://localhost:7071/admin/functions/timer_trigger_entsoe_ingestion \
      -H "Content-Type: application/json" \
      -d "{}"
   ``` 

---

## 📊 Analytics and Visualizations

A visualization script is included to quickly inspect the comprehensive market data logic without running the full Azure Function. 

```bash
python scripts/visualize_prices.py
```

This outputs a dual-axis Matplotlib chart showing **NL Day-Ahead Prices** and **Actual System Load** over the last 3 days, formatted to a 15-minute resolution timeline.

---

## 🔐 Security

This project uses **Managed Identity (System-Assigned)** in production, meaning:

- No connection strings or credentials are stored in application code or environment variables on Azure.
- The Function App authenticates to Azure Storage and Azure SQL via its managed identity, with RBAC roles assigned in the Azure portal.
- `local.settings.json` is used **only** for local development and is excluded from source control via `.gitignore`.

---

## 🧹 Key Development Principles

### 1. Comprehensive Data Scope
The pipeline collects a unified master dataset for the Netherlands, fetching:
- Day-Ahead Prices
- Actual & Forecasted System Load
- Cross-Border physical flows (with FR, GB, DE_LU)

### 2. Time-Series Integrity & Alignment
Due to discrepancies in payload frequency (e.g., Load might be 15-min, DA Price is hourly), the pipeline uses `pandas` to upsample the master dataset to a strict **15-minute interval**.
- **Prices** use step-function filling (forward-fill up to 3 intervals).
- **Physical flows and load** use time-based interpolation for missing periods.

### 3. API Resilience
Network failure mitigation is implemented using the `tenacity` library, allowing exponential backoffs and multiple retry attempts for unstable upstream ENTSO-E endpoints. Queries are structured with isolated `try/except` blocks to prevent single-metric failures from aborting the entire payload.

### 4. Modular Logic
Business logic is decoupled from Azure trigger bindings:
- `entsoe_client.py` — API fetching and retries (testable in isolation)
- `cleaning_service.py` — Data cleaning, resampling, and alignment (testable in isolation)
- `function_app.py` — Azure trigger wiring only

---

## 🧪 Testing

Tests are located in the `tests/` directory. Run them with:

```bash
pytest tests/
```

> 🔍 The test suite covers individual services as well as end-to-end scenarios via `test_integration_test_api.py`.

---

## ☁️ Deployment

Deploy to your Azure Function App using the Azure Functions Core Tools:

```bash
func azure functionapp publish <your-function-app-name>
```

Or use the **Azure Functions** VS Code extension for GUI-driven deployment.

> 💡 Make sure the Function App has the appropriate managed identity RBAC roles assigned to the storage accounts and SQL database.

---

## 📡 Data Source

All electricity transmission and generation data is sourced from the **ENTSO-E Transparency Platform** — the authoritative source for European energy market data.

- Platform: [transparency.entsoe.eu](https://transparency.entsoe.eu)
- API Docs: [ENTSO-E REST API](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
- Python client: [`entsoe-py`](https://github.com/EnergieID/entsoe-py)

---

## 📄 License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.
