# Netflix Data Lake on Azure

![Delta Live Tables Architecture](netproject.jpeg)

A fully‑automated, end‑to‑end data lake for Netflix metadata built on Azure Data Factory, Azure Data Lake Storage Gen2, and Azure Databricks (including Delta Live Tables).

---

## Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Tech Stack](#tech-stack)
* [Prerequisites](#prerequisites)
* [Setup & Configuration](#setup--configuration)

  * [1. Clone the repo](#1-clone-the-repo)
  * [2. Configure Azure resources](#2-configure-azure-resources)
  * [3. Deploy Data Factory pipeline](#3-deploy-data-factory-pipeline)
  * [4. Configure Databricks notebooks](#4-configure-databricks-notebooks)
  * [5. Run Jobs & Pipelines](#5-run-jobs--pipelines)
* [Directory Structure](#directory-structure)
* [Parameters & Widgets](#parameters--widgets)
* [Data Quality & Delta Live Tables](#data-quality--delta-live-tables)
* [Scheduling & Orchestration](#scheduling--orchestration)
* [Monitoring & Observability](#monitoring--observability)
* [Contributing](#contributing)
* [License](#license)

---

## Overview

This project ingests raw Netflix CSV metadata from GitHub, processes it through a medallion architecture (`bronze` → `silver` → `gold`), and applies data‑quality rules via Delta Live Tables. It demonstrates:

* Structured streaming ingestion with Autoloader
* Custom orchestration in Azure Data Factory & Databricks Jobs
* Parameterized notebooks and dynamic branching
* Automated data‑quality enforcement (expectations & drop policies)

---

## Architecture

![Delta Live Tables Architecture](netproject.jpeg)

```plaintext
GitHub CSVs
    ↓ (ADF Web → Validation)
Azure Data Factory
    ↓ (ForEach copy)
ADLS Gen2 ──► bronze/ (raw CSVs via Autoloader)
    ↓ (Structured Streaming notebooks)
bronze/ ──► silver/ (cleaned & enriched via DLT)
    ↓ (DLT pipeline)
silver/ ──► gold/ (curated business tables)
```

Key steps:

1. **Web activity** fetches latest GitHub metadata JSON
2. **Validation** custom activity checks schema
3. **ForEach** loop copies each CSV folder into **bronze/**
4. **Databricks Autoloader** ingests streaming files every 10 s
5. **Delta Live Tables** builds `silver` + `gold` tables with expectations

---

## Tech Stack

* **Orchestration**: Azure Data Factory, Databricks Jobs & Pipelines
* **Storage**: Azure Data Lake Storage Gen2 (ADLS Gen2)
* **Compute**: Azure Databricks (Standard\_D4s\_v3 single node)
* **Streaming**: Autoloader (trigger = `processingTime='10 seconds'`)
* **Tables**: Delta Lake & Delta Live Tables (DLT)
* **Languages**: Python (PySpark), JSON & YAML for configurations

---

## Prerequisites

* Azure subscription with:

  * Data Factory (V2)
  * Databricks workspace & cluster
  * ADLS Gen2 storage account
* Git & GitHub account
* Databricks CLI & Azure CLI installed
* Service principal or Managed Identity with RBAC access to all resources

---

## Setup & Configuration

### 1. Clone the repo

```bash
git clone https://github.com/your-username/netflix-datalake-azure.git
cd netflix-datalake-azure
```

### 2. Configure Azure resources

* Create or assign an existing **Resource Group**.
* Provision:

  * **Storage account** (`netflixdatalakeilyas`)
  * **Data Factory** (`azuredatfactory-netflix`)
  * **Databricks workspace** (`netflix-databricks-ilyas`)
* Grant your Databricks cluster’s managed identity read/write to your ADLS Gen2 containers.

### 3. Deploy Data Factory pipeline

```bash
az login
az group deployment create \
  --resource-group ilyasRessourceGroupNetflix \
  --template-file infrastructure/azure-data-factory.json \
  --parameters @infrastructure/parameters.json
```

### 4. Configure Databricks notebooks

* Import notebooks under `/notebooks` into your workspace.
* Update the **DBFS** root path and storage account name in the config cell.
* Install any required libraries (e.g. `dbutils`, Delta Tables).

### 5. Run Jobs & Pipelines

* **Data Factory**: validate & publish your pipeline, then trigger manually or schedule.
* **Databricks**:

  * Create a multi-task job:

    * **WeekdayLookup** → **IfWeekDay** branching → **SilverMasterData** / **FalseNotebook**
    * **lookup\_locations** → **silverNotebook\_iteration**
    * DLT pipeline named `DLT_GOLD`
  * Set schedules (e.g. daily at 4 PM) in the Jobs UI.

---

## Directory Structure

```
.
├── infrastructure/        # ARM templates & parameter files
├── notebooks/             # Databricks notebooks
│   ├── 1‑Autoloader.scala
│   ├── 2‑StreamingCleanup.py
│   ├── 3‑lookupNotebook.py
│   ├── 4‑silver_notebook.py
│   ├── 5‑lookupNotebook.py
│   ├── 6‑falseNotebook.py
│   ├── 7‑DLT_notebook.py
│   └── utils/             # shared helper modules
├── scripts/               # helper scripts & CLI wrappers
├── tutorials/             # reference diagrams & how‑to guides
└── README.md
```

---

## Parameters & Widgets

Every core notebook uses `dbutils.widgets`:

```python
dbutils.widgets.text("sourceFolder",  "netflix_directors")
dbutils.widgets.text("targetFolder",  "netflix_directors")
dbutils.widgets.dropdown("weekday", "7", [str(i) for i in range(1,8)])
```

Task‑to‑task communication leverages:

```python
# write
dbutils.jobs.taskValues.set(key="weekoutput", value=var)
# read
dbutils.jobs.taskValues.get(taskKey="WeekdayLookup", key="weekoutput")
```

---

## Data Quality & Delta Live Tables

Delta Live Tables enforces expectations:

```python
@dlt.expect_or_drop("rule_non_null_show_id", "show_id IS NOT NULL")
def load_silver_catalog():
    return spark.readStream.format("delta").load(bronze_path)
```

DLT pipeline stages:

1. **Creating update**
2. **Waiting for resources**
3. **Initializing**
4. **Setting up tables**
5. **Rendering graph**

---

## Scheduling & Orchestration

* **Azure Data Factory** trigger: daily at 3 AM
* **Databricks Job** schedule: daily at 4 PM
* Combined, they ensure fresh data lands in **gold/** by business hours.

---

## Monitoring & Observability

* **ADF**: Activity run history & alerts
* **Databricks**:

  * Jobs UI → Graph / Timeline / List views
  * Structured Streaming Dashboard (input vs processing, batch durations)
  * DLT pipeline health & event log

---

## Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/xyz`)
3. Commit your changes & push (`git push origin feature/xyz`)
4. Open a pull request for review

---

## License

This project is licensed under the [MIT License](LICENSE).
