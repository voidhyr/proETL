# proETL: Enterprise Multi-Stream Weather & Air Quality ETL Pipeline

[![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%2B-336791.svg)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg)](https://airflow.apache.org/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An enterprise-grade, multi-stream Extract-Validate-Transform-Load (EVTL) pipeline designed for real-time meteorological and atmospheric telemetry ingestion. `proETL` addresses the critical flaw of silent data corruption in production data warehouses by replacing naive implicit data pruning with an auditable **4-Pillar Validation Engine** delivering **100% anomaly visibility**.

---

## 1. Research Problem & Core Contribution

In standard production data engineering workflows, data cleaning is frequently treated as an implicit byproduct of data transformation (e.g., executing `df.dropna()`, `df.fillna()`, or silent type coercion). While this prevents downstream pipeline crashes, it introduces catastrophic data engineering anti-patterns:

* **Silent Data Loss:** Malformed, out-of-bound, or unmapped telemetry records are dropped without trace or alerting.
* **Zero Anomaly Visibility:** Upstream sensor malfunctions, API schema drifts, and coordinate errors remain completely hidden from data engineers and analytics teams.
* **Corrupted Analytical Dashboards:** Sneaked edge-case anomalies distort historical aggregation metrics, rolling averages, and statistical predictions.

### The proETL Paradigm Shift: EVTL Architecture

`proETL` decouples validation from transformation by introducing **First-Class 4-Pillar Data Quality Validation**:

```
Traditional ETL:  Extract ------------> Transform (df.dropna) ------> Load (Silent Loss)
proETL (EVTL):   Extract -> Validate -> Transform -----------------> Load (Star Schema)
                               |
                               +------> data_quality_log (100% Audit Visibility)
```

Every incoming telemetry record is rigorously audited across four formal validation pillars. Malformed records are quarantined and inserted into the `data_quality_log` relational audit table with full contextual metadata (`rule_failed`, `invalid_value`, `failure_reason`, `pipeline_run_id`, `logged_at`). Clean records proceed downstream to schema-enforced star dimensional modeling.

---

## 2. System Architecture

`proETL` orchestrates a dynamic multi-source ingestion workflow combining OpenWeather Current Weather and Air Pollution REST APIs, transforming raw nested JSON into an enterprise Star Schema warehouse connected to Data Studio BI.

```
+---------------------------------------------------------------------------------------+
|                                DATA EXTRACTION STAGE                                  |
|                                                                                       |
|   +----------------------------+             +----------------------------------+     |
|   | OpenWeather Current Weather|             | OpenWeather Air Pollution API    |     |
|   | (Temp, Humidity, Pressure) |             | (AQI, PM2.5, PM10, CO, NO2)      |     |
|   +--------------+-------------+             +----------------+-----------------+     |
|                  |                                            |                       |
|                  +---------------------+----------------------+                       |
|                                        |                                              |
|                                        v                                              |
|                         [ Dynamic Geo-Coord Resolution ]                              |
|                         [ Multi-Stream Record Flattening]                             |
+----------------------------------------+----------------------------------------------+
                                         |
                                         v
+---------------------------------------------------------------------------------------+
|                       4-PILLAR DATA QUALITY VALIDATION ENGINE                         |
|                                                                                       |
|  [Pillar 1: Completeness]  [Pillar 2: Uniqueness]  [Pillar 3: Range]  [Pillar 4: Ref] |
+-------------------+----------------------------------------------------+--------------+
                    |                                                    |
       (Clean / Passed Records)                              (Malformed Records)
                    |                                                    |
                    v                                                    v
+-----------------------------------------+   +-----------------------------------------+
|          PANDAS TRANSFORMATION          |   |          DATA QUALITY AUDIT LOG         |
|                                         |   |                                         |
|  - Deduplicated Dimension Building      |   |  - Table: data_quality_log              |
|  - Calendar Expansion (dim_date)        |   |  - Stores: Run ID, City, Rule Failed,   |
|  - Strict Numeric Type Coercion         |   |            Invalid Value, Failure Cause |
+-------------------+---------------------+   +--------------------+--------------------+
                    |                                              |
                    v                                              v
+---------------------------------------------------------------------------------------+
|                             POSTGRESQL 16+ DATA WAREHOUSE                             |
|                                                                                       |
|  +-------------------+      +----------------------+      +------------------------+  |
|  |     dim_city      |      |     fact_weather     |      |        dim_date        |  |
|  +-------------------+      +----------------------+      +------------------------+  |
|  | city_name (PK/UQ) |<---->| fact_id (PK)         |<---->| date_id (PK)           |  |
|  | country           |      | city_name (FK)       |      | full_date              |  |
|  | latitude          |      | date_id (FK)         |      | day, month, year       |  |
|  | longitude         |      | temp, pressure, aqi  |      | day_of_week            |  |
|  +-------------------+      | pm2_5, pm10, ...     |      | is_weekend             |  |
|                             +----------------------+      +------------------------+  |
+----------------------------------------+----------------------------------------------+
                                         |
                                         v
+---------------------------------------------------------------------------------------+
|                               ANALYTICS & VISUALIZATION                               |
|                                                                                       |
|    +-----------------------------+               +-------------------------------+    |
|    |    vw_weather_analytics     |               |         vw_dq_summary         |    |
|    +--------------+--------------+               +---------------+---------------+    |
|                   |                                              |                    |
|                   +----------------------+-----------------------+                    |
|                                          |                                            |
|                                          v                                            |
|                         [ Google Data Studio BI Dashboard ]                           |
+---------------------------------------------------------------------------------------+
```

---

## 3. Data Quality Pillars & Boundary Rules

Each ingested record is subjected to declarative validation rules before entering the transformation layer:

| Validation Pillar | Target Attributes | Validation Logic & Physical Boundary | Failure Classification |
| :--- | :--- | :--- | :--- |
| **Pillar 1: Completeness** | `city_name`, `date_id`, `temperature`, `humidity`, `pressure` | Must not contain `NULL`, `NaN`, or missing key mappings. | `COMPLETENESS` |
| **Pillar 2: Uniqueness** | `city_name` + `date_id` | Composite primary key deduplication; prevents duplicate daily observations per city. | `UNIQUENESS` |
| **Pillar 3: Range Validity (Weather)** | `temperature`<br>`humidity`<br>`pressure` | $-50.0^\circ\text{C} \le \text{temperature} \le 60.0^\circ\text{C}$<br>$0\% \le \text{humidity} \le 100\%$<br>$800\text{ hPa} \le \text{pressure} \le 1100\text{ hPa}$ | `RANGE_VALIDITY_TEMP`<br>`RANGE_VALIDITY_HUMIDITY`<br>`RANGE_VALIDITY_PRESSURE` |
| **Pillar 3b: Range Validity (Air Quality)** | `aqi`<br>`pm2_5`<br>`pm10` | $1 \le \text{AQI Index} \le 5$<br>$\text{PM2.5} \ge 0.0\ \mu\text{g/m}^3$<br>$\text{PM10} \ge 0.0\ \mu\text{g/m}^3$ | `RANGE_VALIDITY_AQI`<br>`RANGE_VALIDITY_PM25`<br>`RANGE_VALIDITY_PM10` |
| **Pillar 4: Referential Integrity** | `city_name` | City identifier must strictly match the registered monitoring targets (`TARGET_CITIES`). | `REFERENTIAL_INTEGRITY` |

---

## 4. Empirical Evaluation: Baseline vs. proETL

To evaluate the operational resilience and observability of `proETL`, a comparative controlled experiment was conducted against a standard baseline pipeline using implicit data cleaning (`df.dropna()`):

| Evaluation Metric | Pipeline A (Baseline ETL) | Pipeline B (proETL Engine) |
| :--- | :--- | :--- |
| **Data Cleaning Approach** | Implicit (`df.dropna()`, `drop_duplicates()`) | Explicit (Declarative 4-Pillar Validation) |
| **Anomaly Visibility** | **0.0%** (Anomalies silently dropped) | **100.0%** (Persisted to `data_quality_log`) |
| **Audit Logging & Lineage** | Non-existent | Relational Audit Table with timestamp and Run ID |
| **Root Cause Diagnosability** | None (Engineering blind spot) | Granular (`rule_failed`, `invalid_value`, `reason`) |
| **Air Pollution Metrics** | Ignored / Single stream | Multi-stream combined ingestion (AQI, PM2.5, PM10) |
| **Warehouse Integrity** | High risk of distorted metric aggregations | Guaranteed schema & foreign-key referential integrity |
| **Airflow Orchestration** | Monolithic single task | Decoupled DAG: `Extract` $\to$ `Validate` $\to$ `Transform` $\to$ `Load` |

---

## 5. Local Setup & Execution Guide

### Prerequisites
* Python 3.12+
* PostgreSQL 16+
* [uv](https://github.com/astral-sh/uv) (Fast Python package installer and resolver) or standard `venv`
* OpenWeatherMap API Key

### Step 1: Clone Repository & Set Up Virtual Environment

```bash
git clone https://github.com/voidhyr/proETL.git
cd proETL

# Create and activate virtual environment using uv
uv venv .venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

### Step 2: Configure Environment Variables

Create a `.env` file in the project root:

```ini
# OpenWeather API Configuration
OPENWEATHER_API_KEY=your_openweather_api_key_here
OPENWEATHER_BASE_URL=https://api.openweathermap.org/data/2.5/weather
OPENWEATHER_AIR_POLLUTION_URL=https://api.openweathermap.org/data/2.5/air_pollution

# PostgreSQL Warehouse Credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_warehouse
DB_USER=etl_user
DB_PASSWORD=your_secure_password
```

### Step 3: Database & Warehouse Initialization

Ensure PostgreSQL is running, then initialize the database and views:

```bash
# Create the PostgreSQL database (if not exists)
createdb -U postgres -h localhost weather_warehouse

# Run database connectivity and DQ log table initialization
python config.py

# Create analytical views for Data Studio BI
psql -U etl_user -d weather_warehouse -f sql/create_views.sql
```

### Step 4: Manual Pipeline Execution

You can run individual pipeline modules sequentially for debugging and unit verification:

```bash
# 1. Multi-stream extraction (Weather + Pollution)
python extract.py

# 2. 4-Pillar validation & DQ logging test
python validate.py

# 3. Pandas Star Schema transformation
python transform.py

# 4. Warehouse loading (Idempotent UPSERT into dim_city, dim_date, fact_weather)
python -c "
from config import get_db_engine
from extract import extract_all_weather
from validate import validate_weather_records
from transform import transform_weather_data
from load import init_warehouse_schema, load_dimensions, load_facts

engine = get_db_engine()
init_warehouse_schema(engine)
records = extract_all_weather()
valid, invalid = validate_weather_records(records)
df_city, df_date, df_facts = transform_weather_data(valid)
load_dimensions(df_city, df_date, engine)
load_facts(df_facts, engine)
print('Pipeline executed successfully!')
"
```

### Step 5: Apache Airflow Orchestration & Testing

To run the pipeline on an automated daily schedule (`@daily`) with retries and task separation:

```bash
# Set Airflow home directory
export AIRFLOW_HOME=~/airflow

# Copy or symlink DAG into your Airflow DAGs folder
mkdir -p $AIRFLOW_HOME/dags
cp dags/weather_etl_dag.py $AIRFLOW_HOME/dags/

# Test individual DAG tasks from CLI
airflow tasks test weather_etl_pipeline extract_weather_data 2026-09-01
airflow tasks test weather_etl_pipeline validate_weather_data 2026-09-01
airflow tasks test weather_etl_pipeline transform_weather_data 2026-09-01
airflow tasks test weather_etl_pipeline load_weather_warehouse 2026-09-01
```

---

## 6. Business Intelligence & Reporting Views

The data warehouse exposes two optimized SQL views for visualization in **Google Data Studio**:

1. **`vw_weather_analytics`**: Consolidates dimensional context with atmospheric and pollution facts:
   ```sql
   SELECT city_name, country, latitude, longitude, full_date, day_of_week,
          temperature, humidity, pressure, wind_speed, aqi, pm2_5, pm10
   FROM vw_weather_analytics;
   ```
2. **`vw_dq_summary`**: Aggregates daily data quality rule failures for operational monitoring:
   ```sql
   SELECT log_date, rule_failed, total_violations
   FROM vw_dq_summary;
   ```

---

## 7. Project Metadata

* **Project Title:** proETL: Production Multi-Stream Weather & Air Quality ETL Pipeline
* **Author:** Dani Sam
