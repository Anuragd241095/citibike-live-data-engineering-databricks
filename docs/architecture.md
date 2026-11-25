
# Citi Bike Data Engineering Architecture

This project uses Databricks, PySpark, Delta Lake,
and a Bronze-Silver-Gold Medallion Architecture.

## Architecture Diagram

```mermaid
flowchart TD
    A["Citi Bike Station Status REST API"]
    B["Bronze: Raw Station Status Delta Table"]
    C["Silver: Validated and Enriched Delta Table"]
    D["Gold: Network KPI Table"]
    E["Gold: Low Availability Table"]
    F["Databricks AI/BI Dashboard"]
    M["Citi Bike Station Information API"]
    J["Databricks Job: Every 30 Minutes"]

    A --> B
    B --> C
    M --> C
    C --> D
    C --> E
    D --> F
    E --> F
    J -.-> B
    J -.-> C
    J -.-> D
    J -.-> E
```

## Pipeline Features

- Bronze: REST API ingestion with snapshot IDs and timestamps
- Silver: Data cleaning, quality checks, station metadata enrichment, and incremental Delta MERGE
- Gold: Latest network KPIs and low-availability analytics
- Orchestration: Databricks Jobs every 30 minutes
- Monitoring: Freshness validation and failure notifications
- Visualization: Databricks AI/BI dashboard

