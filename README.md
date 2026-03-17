[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/)
[![Azure Functions](https://img.shields.io/badge/azure-functions-purple)](https://docs.microsoft.com/azure/azure-functions/)
[![License](https://img.shields.io/badge/license-MIT-green)](#)

# ⚡ European Energy Data Pipeline

An automated, serverless data pipeline on **Azure** that collects, cleans, and stores European energy market data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/).

---

## 🚀 Quick Start

1. **Fork or clone** the repository.
2. **Create a Python 3.12+ virtual environment** and activate it.
3. **Install dependencies** with `pip install -r requirements.txt`.
4. **Populate `local.settings.json`** with your storage and ENTSO-E API credentials.
5. **Run locally** via `func start` and observe CSVs being processed into the `cleaned-data` container.

For detailed setup instructions, see the **Getting Started** section below.

---

## 📐 Architecture Overview

This project follows a **Medallion-lite architecture** orchestrated entirely by Azure Functions:

```
ENTSO-E REST API
       │
       ▼  (Timer Trigger — scheduled)
┌──────────────────────┐
│  Ingestion Function  │
└──────────────────────┘
       │
       ▼  CSV → raw-data (Blob Storage)
┌──────────────────────┐
│   Cleaning Function  │  ── triggered by Blob arrival
└──────────────────────┘
       │
       ▼  CSV → cleaned-data (Blob Storage)
┌─────────────────────────┐
│  Warehousing Function   │  ── triggered by Blob arrival
└─────────────────────────┘
       │
       ▼  (SQLAlchemy)
  Azure SQL Database
  └── table: entsoe
```

### Data Flow

| Stage | Trigger | Input | Output |
|---|---|---|---|
| **Ingestion** | Timer (scheduled) | ENTSO-E REST API | `raw-data/` container (CSV) |
| **Cleaning** | Blob Trigger (new file) | `raw-data/` CSV | `cleaned-data/` container (CSV) |
| **Warehousing** | Blob Trigger (new file) | `cleaned-data/` CSV | `entsoe` SQL table (append) |

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| **Language** | Python 3.12+ |
| **Serverless Framework** | Azure Functions — Python V2 Programming Model |
| **API Client** | `entsoe-py` |
| **Data Processing** | `pandas`, `numpy` |
| **Database ORM** | `SQLAlchemy` |
| **Database Driver**| `pyodbc` |
| **Cloud Storage I/O** | `azure-storage-blob` |
| **Identity & Auth** | `azure-identity` (Managed Identity) |
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
- Azure SQL Database

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

   Populate `local.settings.json` with your credentials:

   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "FUNCTIONS_WORKER_RUNTIME": "python",
       "AzureWebJobsStorage": "<your-storage-account-connection-string>",
       "STORAGE_ACCOUNT_NAME": "<your-storage-account-name>",
       "RAW_DATA_CONTAINER": "raw-data",
       "ENTSOE_API_KEY": "<your-entsoe-api-key>",
       "SQL_SERVER_NAME": "<your-sql-server-name>.database.windows.net",
       "SQL_DATABASE_NAME": "<your-sql-database-name>"
     }
   }
   ```
   > ⚠️ `local.settings.json` is git-ignored. Never commit secrets or connection strings to source control

5. **Run locally**

   ```bash
   func start
   ```
   Since the ingestion is normally triggered by a timer, use the following API call to trigger it immediately for testing:
   ```bash
   curl http://localhost:7071/api/manual_run
   ``` 
   ```bash
   curl "http://localhost:7071/api/manual_run?date=2026-03-12"
   ``` 

---

## 🔐 Security

This project uses **Managed Identity (System-Assigned)** in production, meaning:

- No connection strings or credentials are stored in application code or environment variables on Azure.
- The Function App authenticates to Azure Storage and Azure SQL via its managed identity, with RBAC roles assigned in the Azure portal.
- `local.settings.json` is used **only** for local development and is excluded from source control via `.gitignore`.

---

## 🧹 Key Development Principles

### 1. Time-Series Integrity
Timestamps are preserved as the DataFrame index during all CSV transformations (`index=True`). This ensures continuity and correctness for downstream quantitative models.

### 2. Modular Logic
Business logic is decoupled from Azure trigger bindings:

- `entsoe_client.py` — API fetching
- `cleaning_service.py` — Data cleaning
- `database_service.py` — SQL warehousing
- `function_app.py` — Azure trigger wiring only

This separation facilitates unit testing.

### 3. Identity-Based Security
Managed Identity is used throughout, eliminating hardcoded credentials and reducing the attack surface in production.

---

## 🧪 Testing

Tests are located in the `tests/` directory. Run them with:

```bash
pytest tests/
```

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

All electricity transmission and generation data is sourced from the **ENTSO-E Transparency Platform**.

- Platform: [transparency.entsoe.eu](https://transparency.entsoe.eu)
- API Docs: [ENTSO-E REST API](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html)
- Python client: [`entsoe-py`](https://github.com/EnergieID/entsoe-py)

---

## 📄 License

This project is released under the MIT License.
