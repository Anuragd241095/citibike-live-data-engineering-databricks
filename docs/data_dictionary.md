# Citi Bike Live Data Engineering — Data Dictionary

## 1. Overview

This data dictionary documents the Delta Lake tables used in the Citi Bike Live Data Engineering Project developed with Databricks, Python, PySpark, SQL, and Delta Lake.

The project follows the Medallion Architecture:

- **Bronze:** Raw station status data ingested from the Citi Bike REST API.
- **Silver:** Cleaned, validated, and enriched station data.
- **Gold:** Business-ready KPIs and low-bike-availability analytics.

The pipeline runs every 30 minutes through Databricks Jobs.

---

## 2. Bronze Layer — Raw Station Status

**Table:** `workspace.bronze.citibike_station_status`

**Purpose:** Store historical snapshots of station availability information retrieved from the Citi Bike Station Status API.

**Source:** `https://gbfs.citibikenyc.com/gbfs/en/station_status.json`

### Key columns

| Column | Description |
|---|---|
| `station_id` | Unique identifier assigned to each Citi Bike station |
| `num_bikes_available` | Number of bikes currently available at the station |
| `num_docks_available` | Number of docks available to return bikes |
| `num_ebikes_available` | Number of available electric bikes |
| `last_reported` | Timestamp of the latest station status report, expressed as Unix time |
| `is_renting` | Indicates whether the station is currently allowing bike rentals |
| `is_returning` | Indicates whether the station is currently accepting bike returns |
| `snapshot_id` | Unique identifier generated for each API ingestion snapshot |
| `ingestion_timestamp` | Timestamp when the API snapshot was ingested |

The Bronze table may contain additional fields supplied by the API.

### Bronze processing rules

1. Retrieve station status records from the REST API.
2. Parse the JSON response.
3. Convert station records into a PySpark DataFrame.
4. Assign a unique `snapshot_id`.
5. Add an `ingestion_timestamp`.
6. Append the records to the Bronze Delta table.

**Storage strategy:** Append-only ingestion, preserving historical snapshots.

---

## 3. Silver Layer — Cleaned and Enriched Station Data

**Table:** `workspace.silver.citibike_station_enriched`

**Purpose:** Provide a validated analytical dataset combining station availability with station metadata.

**Sources:**

- `workspace.bronze.citibike_station_status`
- Citi Bike Station Information API

**Metadata API:** `https://gbfs.citibikenyc.com/gbfs/en/station_information.json`

### Silver table columns

| Column | Data type | Description |
|---|---|---|
| `station_id` | STRING | Unique station identifier |
| `bikes_available` | BIGINT | Number of available bikes |
| `docks_available` | BIGINT | Number of available docks |
| `ebikes_available` | BIGINT | Number of available electric bikes |
| `last_reported` | BIGINT | Original Unix timestamp from the API |
| `is_renting` | BOOLEAN | Whether bike rentals are enabled |
| `is_returning` | BOOLEAN | Whether bike returns are enabled |
| `snapshot_id` | STRING | Unique ingestion snapshot identifier |
| `ingestion_timestamp` | TIMESTAMP | Time when the snapshot was ingested |
| `last_reported_timestamp` | TIMESTAMP | Converted station report timestamp |
| `station_name` | STRING | Human-readable station name |
| `latitude` | DOUBLE | Geographic latitude |
| `longitude` | DOUBLE | Geographic longitude |
| `capacity` | BIGINT | Station dock capacity |
| `station_type` | STRING | Station classification |
| `availability_percentage` | DOUBLE | Available bikes divided by station capacity, multiplied by 100 |

*Data types are based on the implemented transformation and observed schema. Verify exact types using Databricks Catalog Explorer before treating this as a formal schema contract.*

### Transformation rules

**Column standardization**

| Bronze column | Silver column |
|---|---|
| `num_bikes_available` | `bikes_available` |
| `num_docks_available` | `docks_available` |
| `num_ebikes_available` | `ebikes_available` |

**Timestamp conversion**

The `last_reported` Unix timestamp is converted into `last_reported_timestamp` using PySpark's `timestamp_seconds()` function.

**Station metadata enrichment**

New Silver records are left-joined with Station Information API records using `station_id`.

The enrichment adds station names, locations, capacities, and station types.

### Availability percentage

The calculated availability percentage is:

\[
\text{Availability Percentage} =
\frac{\text{Bikes Available}}{\text{Station Capacity}}\times100
\]

The calculation is performed only when station capacity is greater than zero.

For zero or missing capacity, the value is NULL.

### Silver data quality validations

The notebook validates:

- Missing station identifiers
- Missing bike availability counts
- Negative bike, dock, or e-bike counts
- Duplicate combinations of `snapshot_id` and `station_id`
- Missing station metadata
- Unexpected record-count changes during enrichment
- Invalid availability percentage calculations

If validation fails, the Silver transformation raises an error rather than inserting the invalid batch.

### Incremental Delta MERGE

Silver uses the composite matching key:

`(snapshot_id, station_id)`

The incremental MERGE logic is:

```sql
MERGE INTO workspace.silver.citibike_station_enriched AS target
USING new_silver_records AS source
ON target.snapshot_id = source.snapshot_id
AND target.station_id = source.station_id
WHEN NOT MATCHED THEN INSERT *
```

The SQL above illustrates the implemented PySpark Delta MERGE. `new_silver_records` is a conceptual source name, not a separately created project table.

This preserves previously ingested observations while inserting new snapshot records.

---

## 4. Gold Layer — Network KPIs

**Table:** `workspace.gold.citibike_network_kpis`

**Purpose:** Store the most recent Citi Bike network summary for dashboards and operational reporting.

**Source:** `workspace.silver.citibike_station_enriched`

### Gold KPI columns

| Column | Description | Aggregation |
|---|---|---|
| `total_bikes_available` | Total available bikes across stations | SUM of `bikes_available` |
| `total_ebikes_available` | Total available electric bikes | SUM of `ebikes_available` |
| `total_docks_available` | Total available docks | SUM of `docks_available` |
| `active_stations` | Number of station records with renting enabled | SUM of conditional indicators for `is_renting = TRUE` |
| `snapshot_timestamp` | Timestamp identifying the snapshot used for the KPIs | Latest Silver ingestion timestamp |

### Processing logic

1. Read the enriched Silver table.
2. Identify the most recent ingestion timestamp.
3. Select the records belonging to that latest timestamp.
4. Calculate network-level totals.
5. Replace the previous Gold KPI summary.
6. Verify that the Gold KPI table contains exactly one row.

**Storage strategy:** Overwrite the latest KPI summary on each successful Gold processing run.

---

## 5. Gold Layer — Low Bike Availability

**Table:** `workspace.gold.citibike_low_availability`

**Purpose:** Identify stations with low available bike inventory to support potential redistribution planning.

**Source:** Latest records from `workspace.silver.citibike_station_enriched`

### Key analytical columns

| Column | Description |
|---|---|
| `station_id` | Unique station identifier |
| `station_name` | Station name |
| `bikes_available` | Available bike count |
| `capacity` | Station dock capacity |
| `availability_percentage` | Percentage of capacity represented by available bikes |
| `is_renting` | Whether bike rentals are enabled |
| `latitude` | Geographic latitude |
| `longitude` | Geographic longitude |
| `snapshot_id` | Source snapshot identifier |
| `ingestion_timestamp` | Snapshot ingestion timestamp |

The table also retains other Silver columns because it is created by filtering the latest enriched Silver dataset.

### Business rules

A station is included when all three conditions hold:

```sql
availability_percentage < 20
AND capacity > 0
AND is_renting = TRUE
```

### Validation rules

Saved records must satisfy:

- Availability percentage is not NULL.
- Availability percentage is below 20%.
- Station capacity is positive and non-NULL.
- Renting is enabled.

If any saved record violates these conditions, the Gold validation raises an exception.

**Storage strategy:** Replace previous low-availability results with the latest qualifying station records.

---

## 6. Snapshot Identification and Historical Data

Two fields are essential to the project:

| Field | Purpose |
|---|---|
| `snapshot_id` | Identifies a particular ingestion batch |
| `ingestion_timestamp` | Records when the batch was captured |

Bronze and Silver retain multiple snapshots, enabling historical analysis.

Gold currently focuses on the latest snapshot.

A new `snapshot_id` is generated for each ingestion run, even if the API data has changed very little.

---

## 7. Data Freshness Monitoring

The Gold notebook checks how recently Silver data was ingested.

**Current freshness threshold:** 45 minutes.

The check compares the latest Silver ingestion timestamp with the current time.

The Gold task raises an error when:

- The latest ingestion timestamp is missing.
- Data age exceeds 45 minutes.
- The ingestion timestamp is more than five minutes in the future.

This helps identify stale data in a pipeline scheduled every 30 minutes.

The check runs when the Gold notebook executes; it is not a continuous background check.

---

## 8. Scheduling and Monitoring

**Databricks Job:** `CitiBike_Live_Data_Pipeline`

**Schedule:** Every 30 minutes

**Task sequence:**

1. `Bronze_Ingestion`
2. `Silver_Transformation`
3. `Gold_Analytics`

Each downstream task runs after successful completion of the previous task.

Failure notifications are configured, and Databricks Data Quality Monitoring is enabled for the Medallion schemas.

The published AI/BI dashboard has a separate 30-minute refresh schedule.

---

## 9. Data Consumers

The Gold tables are used by the Databricks AI/BI dashboard to present operational metrics, including:

- Network-wide bike and e-bike inventory
- Available docks
- Active renting stations
- Stations with no available bikes
- Low-availability locations
- Geographic station availability

These metrics support descriptive and operational analytics rather than predictive redistribution optimization.

---

## 10. Future Data Model Improvements

Potential improvements include:

- A station dimension table
- Historical hourly and daily Gold aggregates
- Explicit schema contracts and automated schema-drift detection
- Pipeline audit and snapshot metadata tables
- Automated unit and integration tests
- Historical trend analysis and demand forecasting
- Improved ingestion idempotency across API retries

---

*Project: Citi Bike Live Data Engineering Pipeline*

*Platform: Databricks*

*Architecture: Bronze — Silver — Gold*

*Source: Public Citi Bike GBFS APIs*
