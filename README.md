# Influencer Marketing & Sponsorship Analytics Platform

An end-to-end **synthetic-data portfolio project** combining a role-based influencer/sponsor web application, operational analytics, event streaming, a Microsoft Fabric PySpark cleaning/transformation notebook, and a five-page Power BI report.

> **Data notice:** User, campaign, and ad-request records in the shared analytics dataset are synthetic. Payment confirmation in the application is simulated, not an integration with a real payment processor.

## Architecture

```text
Sponsors / Influencers / Admins
           |
  FastAPI + Jinja2 application
           |
       PostgreSQL ---- Redis (analytics cache)
           |
     Ad-request events -> Kafka -> PySpark streaming (local test)
           |
  Raw data -> Microsoft Fabric notebook -> Silver -> Gold dimension/fact tables
           |
                   Power BI
```

The local Kafka streaming test and Microsoft Fabric notebook are separate workflow components. This repository does **not** assume they run as one automatically deployed end-to-end job; setup and orchestration depend on the environment.

## What's inside

- Root-level `api.py` and helper modules, `templates/`, `static/` — FastAPI application, role-based interfaces, authentication, business workflows, and analytics modules.
- `pipeline/` — PostgreSQL data loading/migration utilities, Kafka/PySpark streaming test, raw-data-quality notebook.
- `fabric/Influencer_Data_Cleaning.ipynb` — PySpark data cleaning and transformation notebook (outputs removed for safe, readable publication).
- `data/powerbi/` — sample synthetic Gold-style dimension/fact CSVs and two snapshot/helper-table exports.
- `powerbi/marketing_platform_project.pbix` — Power BI Desktop report.

## Power BI report — five pages

1. **Executive Overview** — campaigns, users, request activity, completion/payment rate and trends.
2. **Campaign Performance** — requests by campaign, public/private visibility, duration distribution and campaign details.
3. **Budget & Financial Analytics** — campaign budgets, simulated paid amounts, utilization and outlier analysis.
4. **Engagement & Workflow Analytics** — sponsor/influencer rankings, request lifecycle and recorded amounts by status.
5. **Interactive Analytics Hub** — filter-driven combined view with campaign date, visibility, request status, campaign and user slicers.

### Interpreting monetary values

- **Actual Paid Amount** counts records currently marked `paid` and represents simulated payment confirmation.
- **Total Payments All Status Combined** sums recorded `payment_amount` values across statuses. It is **not** actual revenue or cash received.
- `pending` is a sent request awaiting response; `completed` means work finished but not necessarily paid.
- Synthetic data deliberately includes extreme budget-utilization cases. Charts can filter those out when the title explains the threshold.

## Running the application locally

Prerequisites: Python 3.11, PostgreSQL; Redis via Docker is supported. Kafka is optional for basic application behavior but needed to observe events.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env`, then supply your own valid `DATABASE_URL` and `SESSION_SECRET`. Do **not** commit `.env`.

```bash
uvicorn api:app --reload
```

**Note:** `compose.yaml` assumes external PostgreSQL and Kafka services configured for your machine; Redis is included. The app and full pipeline have **not been run end-to-end in this environment**.

## Fabric and Power BI

1. Open `fabric/Influencer_Data_Cleaning.ipynb` in a Fabric notebook with appropriate attached Lakehouse tables (`raw_users`, `raw_campaigns`, `raw_ad_requests`).
2. Review the notebook's table names and environment-specific assumptions before running.
3. Open `powerbi/marketing_platform_project.pbix` in Power BI Desktop. If refresh or credentials are requested, update sources to your local copies or Fabric environment.
4. Use `data/powerbi/*.csv` to inspect the synthetic datasets and the dimensions used in the report.

## Data Pipeline Orchestration — Microsoft Fabric

Implemented a Microsoft Fabric Data Pipeline to orchestrate the execution of the PySpark data-processing notebook.

The notebook processes influencer marketing data through a Medallion Architecture:

- **Bronze Layer:** Raw data ingestion and storage.
- **Silver Layer:** Data cleaning, validation, and transformation using PySpark.
- **Gold Layer:** Analytics-ready dimension and fact tables for Power BI reporting.

The Fabric pipeline coordinates notebook execution, supporting a repeatable data-processing workflow from raw data to analytics-ready datasets.

**Technologies:** Microsoft Fabric, Fabric Data Pipelines, PySpark, Delta Lake, Medallion Architecture, Power BI.

## Security and reproducibility

- Credentials, machine-specific run notes, logs, Spark checkpoints, compiled Python files, and generated Delta/Parquet fragments were intentionally excluded.
- The original uploaded project contained embedded database credentials in a local loader and run-notes file. The public loader has been changed to require `DATABASE_URL` from the environment. **Rotate those original credentials before publishing.**
- This is a portfolio/demo project, not a production-hardened payment system.

## Future improvements

- A reproducible local Docker Compose setup for PostgreSQL, Redis, Kafka and the backend.
- Tests for authentication, request state transitions, analytics aggregations, and sponsor/influencer Power BI filter behavior.
- Architecture diagram and screenshots of the finished five-page dashboard.
- Data quality and synthetic data generation documentation.
