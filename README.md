# ✈️ Flight Tracking — Near Real-Time Pipeline

## Overview

This project is an end-to-end data engineering pipeline that collects, processes, and analyzes **live aircraft positions over France** from the **OpenSky Network API**. It demonstrates how to build a pipeline with:

- Near real-time ADS-B data ingestion every 10 minutes
- Workflow orchestration with Apache Airflow (two independent DAGs)
- Data warehousing in Snowflake (RAW layer)
- ELT transformation & automated testing with dbt (star schema)
- Layered data quality checks with Great Expectations

---

## Architecture

```mermaid
flowchart TD
    A["OpenSky API\nADS-B Data"] --> B["Airflow\nDAG Ingestion\n⏱ every 10 min"]
    B --> C["Snowflake\nRAW Layer"]
    C --> D["Airflow\nDAG dbt\n⏱ every hour"]
    D --> E["dbt\nStar Schema\nMARTS Layer"]
    E --> F["Great Expectations\nQuality Checks"]
    E --> G["Streamlit\nDashboard"]
    style A fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#333
    style B fill:#E3F2FD,stroke:#01579B,stroke-width:2px,color:#333
    style C fill:#E8F5E9,stroke:#1B5E20,stroke-width:2px,color:#333
    style D fill:#E3F2FD,stroke:#01579B,stroke-width:2px,color:#333
    style E fill:#FFEBEE,stroke:#BF360C,stroke-width:2px,color:#333
    style F fill:#FFF8E1,stroke:#F57F17,stroke-width:2px,color:#333
    style G fill:#F3E5F5,stroke:#6A1B9A,stroke-width:2px,color:#333
```

---

## Data Pipeline

### 1. Data Collection – OpenSky Network API
- Fetches live aircraft positions over **metropolitan France** every 10 minutes
- OAuth2 authentication with automatic token renewal and retry logic
- SHA256-based deduplication key generated per position

### 2. Data Loading – Snowflake (RAW Layer)
- Loads positions via a **MERGE INTO** pattern — fully idempotent, zero duplicates
- Returns ingestion stats: inserted, skipped, total

### 3. Data Quality – Great Expectations (RAW)
- Validates raw positions: non-null coordinates, valid ranges
- Non-blocking — logs warnings but never stops the pipeline

### 4. Data Transformation – dbt (ELT)
- **Staging** → cleans and standardizes raw positions
- **Intermediate** → deduplication logic
- **Marts** → analytics-ready star schema
- Automated dbt tests and source freshness checks on every run

### 5. Data Quality – Great Expectations (MARTS)
- Validates analytical tables against quality contracts
- Blocking — pipeline stops if checks fail

---

## Analytics Layer (dbt Models)

### dbt Lineage Graph

[![ft3.png](https://i.postimg.cc/rspSJpCZ/ft3.png)](https://postimg.cc/1nLnRsdD)

### Staging – `stg_positions`
Cleans and standardizes raw ADS-B data:
- Unique position ID validation
- Non-null checks on icao24, latitude, longitude, timestamp
- Speed and altitude range validation

### Intermediate – `int_positions_deduped`
Deduplication layer before marts:
- Removes duplicate positions based on SHA256 key
- Ensures clean input for analytical models

### Marts

#### `fact_positions`
All aircraft position points over France:
- Coordinates (latitude, longitude)
- Altitude (barometric and geometric)
- Speed, heading, climb rate
- Timestamp and flight reference

#### `dim_vols`
Flight dimension:
- ICAO24 unique identifier
- Callsign and origin country
- Flight metadata

---

## Business Insights

This pipeline enables answering questions such as:
- How many aircraft are flying over France at any given moment?
- What are the most active flight corridors over French territory?
- Which countries generate the most air traffic over France?
- How does traffic volume evolve throughout the day and week?

---
## Dashboard
 
An interactive **Streamlit** dashboard provides near real-time monitoring of aircraft over France:
 
- **Live KPIs** — number of aircraft in flight, active airlines, average altitude and speed, active anomalies
- **Interactive map** — aircraft positions with heading, altitude, speed and flight phase, color-coded by phase
- **Anomaly detection** — flags aircraft with suspicious altitude variation (mismatch between computed and reported climb rate)
---

### 1. Configure Airflow Variables

In the Airflow UI (`Admin → Variables`), add:

```
OPENSKY_CLIENT_ID=your_opensky_client_id
OPENSKY_CLIENT_SECRET=your_opensky_client_secret
```

### 2. Configure Airflow Connection

Create a connection named `FT_snowflake_default` with:

| Field | Value |
|-------|-------|
| Conn Type | Snowflake |
| Host | your Snowflake account |
| Login | your Snowflake username |
| Password | your Snowflake password |
| Extra (JSON) | `{"database": "FLIGHT_TRACKING", "schema": "RAW", "warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}` |

### 3. Run the DAGs

- Activate `dag_ingest_flights` → runs every **10 minutes**
- Activate `dag_dbt_transform_main` → runs every **hour**

### 4. Run the dashboard

Create a `.env` file in the streamlit folder with your Snowflake credentials:
 
```
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=FLIGHT_TRACKING
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN
```
Install the requirements and run the dashboard
```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

---

## Results

### Airflow DAGS
[![ft1.png](https://i.postimg.cc/NMdX5cqm/ft1.png)](https://postimg.cc/ZBynMXq5)
[![ft2.png](https://i.postimg.cc/pXwF9C79/ft2.png)](https://postimg.cc/XpgJmfn3)

### Snowflake Tables
[![ft4.png](https://i.postimg.cc/qRrfgPXB/ft4.png)](https://postimg.cc/B8m7VwVR)
[![ft5.png](https://i.postimg.cc/MTmCB9dJ/ft5.png)](https://postimg.cc/47n2kbDW)
[![ft6.png](https://i.postimg.cc/85m2vZtD/ft6.png)](https://postimg.cc/JDtYVcfd)

### Streamlit Dashboard 
[Watch Demo](https://github.com/user-attachments/assets/f0ed8859-9416-4977-9b7a-a074a2f693cf)
