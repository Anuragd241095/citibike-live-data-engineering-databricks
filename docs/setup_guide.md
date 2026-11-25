
# Citi Bike Live Data Engineering — Setup Guide

## 1. Project Overview

This guide explains how to set up and run the Citi Bike Live Data Engineering Pipeline using Databricks.

The project implements a Bronze–Silver–Gold Medallion Architecture to ingest public Citi Bike station status data, transform and enrich records, generate operational analytics, and support a Databricks AI/BI dashboard.

The pipeline runs automatically every 30 minutes.

### Technology Stack

- Python
- REST APIs
- PySpark
- Apache Spark
- Delta Lake
- Databricks SQL
- Databricks Jobs
- Databricks AI/BI Dashboards
- GitHub

---

## 2. Prerequisites

Before starting, ensure you have:

1. Access to a Databricks workspace with Unity Catalog support.
2. Permission to create schemas and tables.
3. Permission to create and execute notebooks.
4. Access to supported Databricks compute.
5. Permission to create and schedule Databricks Jobs.
6. Internet access to the public Citi Bike APIs.
7. A GitHub account, if you want to clone or version-control the notebooks.

This project was developed using Databricks Free Edition with Serverless compute.

Feature availability and workspace limits may vary.

---

## 3. Data Sources

This project uses two public Citi Bike GBFS API endpoints.

### Station Status API

https://gbfs.citibikenyc.com/gbfs/en/station_status.json

Provides station-level bike availability, dock availability, electric bike counts, and operational status.

### Station Information API

https://gbfs.citibikenyc.com/gbfs/en/station_information.json

Provides station names, geographic locations, capacities, and station metadata.

API access requires no project-specific authentication credentials.

The pipeline depends on these public endpoints remaining available and compatible with the notebook parsing logic.

---

## 4. Clone or Download the Repository

Repository:

`citibike-live-data-engineering-databricks`

You can download the repository as a ZIP file from GitHub or clone it using Git.

```bash
git clone https://github.com/YOUR_USERNAME/citibike-live-data-engineering-databricks.git
```

Replace `YOUR_USERNAME` with your GitHub username.

If using Databricks Git folders, connect your Git provider and clone the repository into the workspace.

---

## 5. Prepare the Unity Catalog Schemas

The implemented project uses the `workspace` catalog.

Create the schemas if they do not already exist:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
```

Run these commands in a Databricks SQL editor or notebook SQL cell.

If your workspace uses a different catalog, update all fully qualified table names in the notebooks before execution.

### Project Tables

| Layer | Table |
|---|---|
| Bronze | `workspace.bronze.citibike_station_status` |
| Silver | `workspace.silver.citibike_station_enriched` |
| Gold | `workspace.gold.citibike_network_kpis` |
| Gold | `workspace.gold.citibike_low_availability` |

The notebooks create or update these Delta tables during execution, according to their implemented write logic.

---

## 6. Import the Notebooks

Import or open the following notebooks from the repository:

```text
02_CitiBike_Bronze_Ingestion.ipynb
03_CitiBike_Silver_Transformation.ipynb
04_CitiBike_Gold_Analytics.ipynb
```

These notebooks represent the three Medallion Architecture layers.

Keep their execution order unchanged.

### Important

The exported notebooks do not include the original execution outputs.

They are intended to be executed in your own Databricks environment.

Review the catalog names, table references, permissions, and available compute before running.

---

## 7. Run Bronze Ingestion

Open:

`02_CitiBike_Bronze_Ingestion`

Execute the notebook using supported Databricks compute.

### Expected Processing

1. Connect to the Station Status API.
2. Parse the JSON response.
3. Generate a unique ingestion snapshot ID.
4. Add the ingestion timestamp.
5. Append station records to Bronze.

### Verify Bronze

```sql
SELECT COUNT(*) AS total_records
FROM workspace.bronze.citibike_station_status;
```

Check the latest ingestion:

```sql
SELECT
    snapshot_id,
    COUNT(*) AS station_count,
    MAX(ingestion_timestamp) AS ingestion_time
FROM workspace.bronze.citibike_station_status
GROUP BY snapshot_id
ORDER BY ingestion_time DESC
LIMIT 5;
```

Each successful ingestion should add a new snapshot.

---

## 8. Run Silver Transformation

Open:

`03_CitiBike_Silver_Transformation`

Run the notebook after Bronze ingestion succeeds.

### Expected Processing

1. Identify Bronze snapshots not yet processed in Silver.
2. Standardize station status columns and data types.
3. Validate required fields and availability counts.
4. Retrieve station metadata from the Station Information API.
5. Join metadata using `station_id`.
6. Calculate availability percentage.
7. Insert new observations with Delta MERGE.
8. Check for duplicate snapshot/station combinations.

### Verify Silver

```sql
SELECT COUNT(*) AS total_records
FROM workspace.silver.citibike_station_enriched;
```

Inspect sample records:

```sql
SELECT *
FROM workspace.silver.citibike_station_enriched
LIMIT 10;
```

Check for duplicate station observations:

```sql
SELECT
    snapshot_id,
    station_id,
    COUNT(*) AS duplicate_count
FROM workspace.silver.citibike_station_enriched
GROUP BY snapshot_id, station_id
HAVING COUNT(*) > 1;
```

Expected result: no duplicate combinations.

---

## 9. Run Gold Analytics

Open:

`04_CitiBike_Gold_Analytics`

Run the notebook after Silver completes.

### Expected Processing

1. Read the latest Silver snapshot.
2. Validate data freshness.
3. Calculate network-level operational KPIs.
4. Write the network KPI Gold table.
5. Identify low-bike-availability stations.
6. Write the low-availability Gold table.
7. Validate the Gold outputs.

### Verify Gold KPIs

```sql
SELECT *
FROM workspace.gold.citibike_network_kpis;
```

Expected result: one current network KPI summary row.

### Verify Low-Availability Stations

```sql
SELECT *
FROM workspace.gold.citibike_low_availability
LIMIT 20;
```

These records represent stations meeting the project's low-availability filtering rules.

### Check Invalid Low-Availability Records

```sql
SELECT COUNT(*) AS invalid_records
FROM workspace.gold.citibike_low_availability
WHERE availability_percentage IS NULL
   OR availability_percentage >= 20
   OR capacity IS NULL
   OR capacity <= 0
   OR is_renting IS NULL
   OR is_renting = FALSE;
```

Expected result: zero invalid records.

---

## 10. Create the Databricks Job

Navigate to:

**Databricks → Jobs & Pipelines → Create Job**

Use this Job name:

`CitiBike_Live_Data_Pipeline`

Create three notebook tasks.

| Task | Notebook | Depends on |
|---|---|---|
| `Bronze_Ingestion` | `02_CitiBike_Bronze_Ingestion` | None |
| `Silver_Transformation` | `03_CitiBike_Silver_Transformation` | `Bronze_Ingestion` |
| `Gold_Analytics` | `04_CitiBike_Gold_Analytics` | `Silver_Transformation` |

Select supported Serverless compute if available.

Configure downstream tasks to run only when their upstream dependencies succeed.

### Schedule

Set the Job to execute every 30 minutes.

For the original project, the scheduled run times are approximately:

- `:26`
- `:56`

Use the appropriate workspace time zone.

### Notifications

Configure failure email notifications under the Job's settings.

Choose the relevant notification destination for your own workspace.

---

## 11. Test the Complete Workflow

Before relying on scheduled execution:

1. Open `CitiBike_Live_Data_Pipeline`.
2. Click **Run now**.
3. Wait for the three tasks to complete.
4. Review the Job run details.
5. Confirm each task reports success.
6. Check that new Bronze and Silver records exist.
7. Verify the Gold KPI and low-availability tables.

A successful test establishes that the configured Job can execute the pipeline in dependency order.

It does not guarantee future runs will succeed if the external API or workspace conditions change.

---

## 12. Create the Databricks AI/BI Dashboard

Navigate to:

**Databricks → Dashboards → Create Dashboard**

Build visualizations using the two Gold tables.

### Recommended Visualizations

| Visualization | Data |
|---|---|
| Total available bikes | Gold network KPI table |
| Total available e-bikes | Gold network KPI table |
| Available docks | Gold network KPI table |
| Active stations | Gold network KPI table |
| Low-availability station chart | Gold low-availability table |
| Geographic station map | Gold low-availability table |

Create dashboard datasets using SQL queries against the Gold tables.

Configure the appropriate permissions for your dashboard viewers.

### Dashboard Refresh

In the original project, dashboard refresh runs every 30 minutes, approximately at:

- `:05`
- `:35`

This timing is intentionally offset from the pipeline schedule.

Dashboard refresh is configured separately from Job execution.

---

## 13. Data Quality and Freshness

The pipeline includes validation at multiple stages.

### Bronze

- API response handling
- Snapshot identification
- Ingestion timestamp tracking

### Silver

- Required-field validation
- Nonnegative count validation
- Duplicate snapshot/station detection
- Station metadata enrichment checks
- Incremental MERGE

### Gold

- Latest-data selection
- Freshness validation
- Network KPI row-count validation
- Low-availability business rule validation

### Freshness Threshold

The original Gold notebook checks for data older than 45 minutes.

When the check fails, Gold execution raises an exception.

This is an execution-time validation, not an always-running freshness monitor.

---

## 14. Troubleshooting

| Issue | Recommended Check |
|---|---|
| API request fails | Verify endpoint accessibility and inspect HTTP errors |
| Bronze table unavailable | Check catalog, schema permissions, and notebook execution |
| Silver reports `NO_NEW_DATA` | Confirm new Bronze snapshots exist |
| Delta MERGE fails | Check source and target schema compatibility |
| Duplicate observations | Inspect the `(snapshot_id, station_id)` composite key |
| Gold freshness failure | Inspect the latest Silver ingestion timestamp |
| Job task fails | Review the failed task's notebook output |
| Dashboard shows stale values | Check Job success, Gold updates, and dashboard refresh |
| Permission denied | Review Unity Catalog and workspace permissions |

---

## 15. Notes on Reproducibility

This repository provides the notebook logic and documentation, but some Databricks workspace resources must be configured manually.

These include:

- Unity Catalog permissions
- Serverless compute access
- Job creation and scheduling
- Email notification destinations
- Dashboard datasets and visualizations
- Dashboard sharing and refresh settings

A dashboard screenshot is included for reference, but it is not an importable dashboard definition.

The project uses scheduled API polling every 30 minutes, not continuous streaming.

For production deployment, consider API retry policies, environment-specific configuration, secrets management when needed, automated tests, and infrastructure-as-code.

---

## 16. Documentation

Additional project documentation:

- [Main README](../README.md)
- [Architecture](architecture.md)
- [Data Dictionary](data_dictionary.md)
- [Pipeline Workflow](pipeline_workflow.md)

---

**Project:** Citi Bike Live Data Engineering Pipeline

**Architecture:** Bronze → Silver → Gold

**Platform:** Databricks

**Data source:** Public Citi Bike GBFS APIs

**Purpose:** Demonstrate API ingestion, incremental Delta Lake processing, data quality validation, workflow orchestration, and operational analytics.
