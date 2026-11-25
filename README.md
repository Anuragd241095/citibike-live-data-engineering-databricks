# Citi Bike Live Data Engineering Pipeline | Databricks

An end-to-end, near-real-time data engineering project that ingests NYC Citi Bike station availability data through public REST APIs, processes it using Databricks and PySpark, and delivers operational insights through interactive AI/BI dashboards.

The project demonstrates a production-style Medallion Architecture with automated ingestion, incremental processing, Delta Lake storage, data quality validation, monitoring, and scheduled analytics refreshes.

## 1. Project Overview

Citi Bike operates a bike-sharing network across New York City. Station availability changes throughout the day, creating opportunities to monitor bike inventory, identify low-availability stations, and support redistribution decisions.

This project builds an automated analytics pipeline to answer questions such as:

- How many bikes and e-bikes are available across the network?
- How many stations are actively renting bikes?
- Which stations have fewer than 20% of their bike spaces occupied by available bikes?
- Which locations may require bike redistribution?
- How recently was the underlying data refreshed?

## 2. Technologies Used

| Technology | Purpose |
|---|---|
| Python | REST API integration and ingestion logic |
| Requests | Retrieving live Citi Bike JSON data |
| PySpark | Distributed data transformation and validation |
| Databricks | Data engineering development and orchestration |
| Delta Lake | Reliable storage, incremental MERGE, and table updates |
| Databricks Jobs | Automated pipeline execution |
| SQL | Data analysis and validation |
| Databricks AI/BI | Interactive operational dashboards |
| GitHub | Source control and project documentation |

## 3. Data Architecture

The solution follows a three-layer Medallion Architecture.

```text
Citi Bike Public REST APIs
            |
            v
BRONZE — Raw Data Ingestion
workspace.bronze.citibike_station_status
            |
            v
SILVER — Cleaned and Enriched Data
workspace.silver.citibike_station_enriched
            |
            v
GOLD — Business Analytics
workspace.gold.citibike_network_kpis
workspace.gold.citibike_low_availability
            |
            v
Databricks AI/BI Dashboard
```

### Bronze Layer — Raw Data Ingestion

- Retrieves station availability data from the Citi Bike GBFS API.
- Converts JSON records into PySpark DataFrames.
- Assigns a unique snapshot identifier.
- Captures ingestion timestamps.
- Appends each snapshot to a Delta table, preserving historical observations.

### Silver Layer — Data Transformation

- Identifies unprocessed Bronze snapshots.
- Renames and standardizes columns.
- Converts timestamp and Boolean fields.
- Validates missing IDs, missing values, negative counts, and duplicate station records.
- Enriches status records with station metadata from the Citi Bike Station Information API.
- Calculates bike availability percentage.
- Uses Delta Lake MERGE to insert new station observations without re-inserting existing snapshot/station combinations.

### Gold Layer — Business Analytics

Produces two analytical datasets:

**Network KPIs**

- Total available bikes
- Total available e-bikes
- Total available docks
- Number of active stations
- Latest snapshot timestamp

**Low-Availability Stations**

Identifies actively renting stations with valid positive capacity and bike availability below 20%, helping highlight potential redistribution priorities.

Gold tables are refreshed using the latest Silver snapshot.

## 4. Incremental Data Processing

The pipeline uses a unique `snapshot_id` to distinguish each API ingestion.

For Silver incremental loading, records are matched using:

```sql
target.snapshot_id = source.snapshot_id
AND target.station_id = source.station_id
```

Delta Lake MERGE inserts records whose snapshot/station combination is not already present in Silver.

This approach preserves historical station observations and helps prevent duplicate insertion when processing the same snapshot again.

## 5. Data Quality and Freshness Checks

The pipeline includes automated validation for:

- Missing station identifiers and availability values
- Negative bike, dock, or e-bike counts
- Duplicate station records within snapshots
- Missing station metadata
- Unexpected record-count changes during enrichment
- Invalid availability calculations
- Gold KPI row count
- Low-availability station business rules
- Stale Silver data using a 45-minute freshness threshold

Validation failures raise exceptions to stop the affected processing task.

Databricks Data Quality Monitoring is also enabled for the project's Medallion schemas, with anomaly detection baselines established over time.

## 6. Workflow Automation

The Databricks Job `CitiBike_Live_Data_Pipeline` executes the following tasks sequentially:

```text
Bronze_Ingestion
       |
       v
Silver_Transformation
       |
       v
Gold_Analytics
```

**Execution frequency:** Every 30 minutes.

Each downstream task depends on successful completion of the preceding task.

The published AI/BI dashboard also has a 30-minute refresh schedule. Job failure email notifications are configured to support operational monitoring.

## 7. Interactive Dashboard

The Databricks AI/BI dashboard, **Citi Bike | Live Operations Analytics**, includes:

- Total available bikes
- Total available e-bikes
- Available docks
- Active stations
- Stations with no available bikes
- Bike redistribution priority indicators
- Low-bike-availability station comparisons
- Geographic visualization of station availability

The dashboard supports operational awareness and helps identify areas where bike supply may be insufficient.

## 8. Repository Structure

```text
citibike-live-data-engineering-databricks/
|
|-- 02_CitiBike_Bronze_Ingestion.ipynb
|-- 03_CitiBike_Silver_Transformation.ipynb
|-- 04_CitiBike_Gold_Analytics.ipynb
|-- README.md
|-- .gitignore
|-- LICENSE
```

## 9. Data Sources

Citi Bike public GBFS feeds:

**Station Status API**

https://gbfs.citibikenyc.com/gbfs/en/station_status.json

**Station Information API**

https://gbfs.citibikenyc.com/gbfs/en/station_information.json

The feeds provide station availability and metadata. The project accesses these publicly available endpoints without embedding API credentials.

## 10. Running the Project

1. Configure a Databricks workspace with access to the public Citi Bike APIs.
2. Create the required Bronze, Silver, and Gold schemas.
3. Create the Delta tables with the expected schemas.
4. Run the Bronze ingestion notebook.
5. Run the Silver transformation notebook to validate and enrich new observations.
6. Run the Gold analytics notebook to generate operational summaries.
7. Configure a Databricks Job with Bronze → Silver → Gold dependencies.
8. Connect the Gold tables to a Databricks AI/BI dashboard.

The notebooks were developed against existing project tables. Initial table setup and workspace configuration may be required when reproducing the project in a new environment.

## 11. Key Data Engineering Concepts Demonstrated

- REST API data ingestion
- JSON parsing and PySpark DataFrames
- Medallion Architecture
- Delta Lake tables
- Incremental ETL
- Idempotent snapshot-level MERGE logic
- Data enrichment and joins
- Data quality validation
- Data freshness monitoring
- Workflow orchestration
- Scheduled dashboard refresh
- Git-based version control

## 12. Future Improvements

- Introduce an explicit snapshot-level ingestion audit table.
- Add structured pipeline logs and monitoring metrics.
- Implement retries and handling for temporary API failures.
- Expand historical trend analytics.
- Add automated data quality tests.
- Improve deployment reproducibility using configuration files and infrastructure automation.

- ## Dashboard Preview

![Citi Bike Live Operations Dashboard](dashboard.png)

---

**Project type:** Data Engineering Portfolio Project

**Domain:** Urban Mobility / Bike-Sharing Operations

**Platform:** Databricks

**Data source:** Citi Bike Public GBFS API

*This is an independent educational portfolio project and is not affiliated with or endorsed by Citi Bike.*
