# pheonix-etl
for phoenix masterclass
# Data Engineering Workshop — API to Snowflake Pipeline

End-to-end ETL pipelines that extract data from public APIs, save to CSV, load into Snowflake and apply SCD2 (Slowly Changing Dimension Type 2) to track historical changes.

---

## Project Structure

```
class/
├── movies_pipeline.ipynb          # Extract movies → CSV → SCD2 (local)
├── weather_pipeline.ipynb         # Extract weather → CSV → SCD2 (local)
├── movie_snowflake.ipynb          # Extract movies → CSV → Snowflake Bronze → SCD2
├── weather_snowflake.ipynb        # Extract weather → CSV → Snowflake Bronze → SCD2
├── Class_02_Simple_Transformations_in_Python.ipynb
├── movies_raw.csv                 # Raw extract from TMDB API
├── weather_raw.csv                # Raw extract from OpenWeatherMap API
├── movies_scd2.csv                # SCD2 output (local version)
├── weather_scd2.csv               # SCD2 output (local version)
└── README.md
```

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│   TMDB API          │────▶│  movies_raw.csv  │────▶│  BRONZE.MOVIES       │
│   (1000 movies)     │     │                  │     │  (full load)         │
└─────────────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                               │
                                                               ▼
                                                    ┌──────────────────────┐
                                                    │  SILVER.MOVIES_SCD2  │
                                                    │  (MERGE on movie_id) │
                                                    └──────────────────────┘

┌─────────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│  OpenWeatherMap API  │────▶│ weather_raw.csv  │────▶│  BRONZE.WEATHER      │
│  (40 cities)         │     │                  │     │  (full load)         │
└─────────────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                               │
                                                               ▼
                                                    ┌──────────────────────┐
                                                    │  SILVER.WEATHER_SCD2 │
                                                    │  (MERGE on city)     │
                                                    └──────────────────────┘
```

---

## Part 1 — Local Setup (CSV pipelines)

### Prerequisites

- Python 3.11 or 3.12 (not 3.14 — SSL issues with Snowflake connector)
- Free API keys:
  - **TMDB:** https://www.themoviedb.org/settings/api
  - **OpenWeatherMap:** https://openweathermap.org/api

### Create environment

**Windows:**

```bash
python -m venv env
env\Scripts\activate
pip install requests pandas numpy jupyter
```

**Linux / macOS:**

```bash
python3 -m venv env
source env/bin/activate
pip install requests pandas numpy jupyter
```

### Run the local pipelines

1. Open `movies_pipeline.ipynb` — paste your TMDB API key in Cell 1 — Run All
2. Open `weather_pipeline.ipynb` — paste your OpenWeatherMap key in Cell 1 — Run All

Each notebook:
- Extracts data from the API
- Saves a raw CSV (Bronze layer)
- Applies SCD2 and saves to a separate CSV

Run them again later to see SCD2 history build up — expired rows get `is_current = False` with a `valid_to` timestamp.

---

## Part 2 — Snowflake Setup

### Prerequisites

- A Snowflake account (free trial works: https://signup.snowflake.com)
- Note your account identifier from the Snowflake URL: `https://app.snowflake.com/<org>/<account>/`
  - Your account string is `<org>-<account>` (e.g. `ftyokgn-cy22613`)

### Install Snowflake connector

Standard install:

```bash
pip install snowflake-connector-python
```

If you're on a corporate laptop with SSL inspection (e.g. SCI):

```bash
pip install snowflake-connector-python --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
```

### Corporate SSL fix (skip if not on a corporate network)

If your laptop intercepts HTTPS traffic, you need to export the corporate CA certificates. Run this once in a Python cell:

```python
import ssl, os

pem_path = os.path.expanduser("~/Downloads/corp_certs.pem")
ctx = ssl.create_default_context()
certs = ctx.get_ca_certs(binary_form=True)

with open(pem_path, "wb") as f:
    for cert in certs:
        f.write(ssl.DER_cert_to_PEM_cert(cert).encode())

print(f"Saved {len(certs)} certs to {pem_path}")
```

Then set these environment variables at the top of every notebook before importing `snowflake.connector`:

```python
import os
os.environ["REQUESTS_CA_BUNDLE"] = r"C:\Users\your_username\Downloads\corp_certs.pem"
os.environ["SSL_CERT_FILE"] = r"C:\Users\your_username\Downloads\corp_certs.pem"
```

### Connect to Snowflake

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account   = "your-account-id",
    user      = "your_username",
    password  = "your_password",
    database  = "ETL_WORKSHOP",
    warehouse = "COMPUTE_WH",
)
cur = conn.cursor()
print("Connected")
```

### Create database and tables

Run this SQL in a Snowflake worksheet or via the Python connector:

```sql
CREATE DATABASE IF NOT EXISTS ETL_WORKSHOP;
USE DATABASE ETL_WORKSHOP;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;

-- Bronze tables (raw landing zone)
CREATE TABLE IF NOT EXISTS BRONZE.MOVIES (
    movie_id       INT,
    title          VARCHAR(500),
    release_date   VARCHAR(20),
    popularity     FLOAT,
    vote_average   FLOAT,
    vote_count     INT,
    language       VARCHAR(10),
    overview       VARCHAR(1000),
    extracted_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.WEATHER (
    city           VARCHAR(100),
    country        VARCHAR(10),
    temp_c         FLOAT,
    feels_like_c   FLOAT,
    humidity_pct   INT,
    pressure_hpa   INT,
    wind_speed_ms  FLOAT,
    weather        VARCHAR(50),
    description    VARCHAR(200),
    visibility_m   INT,
    latitude       FLOAT,
    longitude      FLOAT,
    reading_time   TIMESTAMP,
    extracted_at   TIMESTAMP
);

-- Silver SCD2 tables (historical tracking)
CREATE TABLE IF NOT EXISTS SILVER.MOVIES_SCD2 (
    movie_id       INT,
    title          VARCHAR(500),
    release_date   VARCHAR(20),
    popularity     FLOAT,
    vote_average   FLOAT,
    vote_count     INT,
    language       VARCHAR(10),
    overview       VARCHAR(1000),
    extracted_at   TIMESTAMP,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN
);

CREATE TABLE IF NOT EXISTS SILVER.WEATHER_SCD2 (
    city           VARCHAR(100),
    country        VARCHAR(10),
    temp_c         FLOAT,
    feels_like_c   FLOAT,
    humidity_pct   INT,
    pressure_hpa   INT,
    wind_speed_ms  FLOAT,
    weather        VARCHAR(50),
    description    VARCHAR(200),
    visibility_m   INT,
    latitude       FLOAT,
    longitude      FLOAT,
    reading_time   TIMESTAMP,
    extracted_at   TIMESTAMP,
    valid_from     TIMESTAMP,
    valid_to       TIMESTAMP,
    is_current     BOOLEAN
);
```

---

## Part 3 — Running the Snowflake Pipelines

### Movies pipeline (`movie_snowflake.ipynb`)

| Cell | What it does |
|------|-------------|
| 1 | Connect to Snowflake (with SSL fix) |
| 2 | Extract 1000 movies from TMDB API (50 pages) |
| 3 | Clean data and save to `movies_raw.csv` |
| 4 | Truncate and load CSV into `BRONZE.MOVIES` |
| 5 | SCD2 merge into `SILVER.MOVIES_SCD2` |
| 6 | Close connection |

### Weather pipeline (`weather_snowflake.ipynb`)

| Cell | What it does |
|------|-------------|
| 1 | Connect to Snowflake (with SSL fix) |
| 2 | Extract weather for 40 cities from OpenWeatherMap |
| 3 | Clean data and save to `weather_raw.csv` |
| 4 | Truncate and load CSV into `BRONZE.WEATHER` |
| 5 | SCD2 merge into `SILVER.WEATHER_SCD2` |
| 6 | Close connection |

### How the SCD2 merge works

The SCD2 step does two things:

**1. Expire changed rows** — uses `MERGE` on the business key (`movie_id` or `city`):

```sql
MERGE INTO SILVER.MOVIES_SCD2 tgt
USING (deduplicated bronze) src
ON src.movie_id = tgt.movie_id AND tgt.is_current = TRUE
WHEN MATCHED AND (tracked columns changed)
THEN UPDATE SET valid_to = now, is_current = FALSE
```

**2. Insert new + changed rows** — any `movie_id` with no current row gets a new one:

```sql
INSERT INTO SILVER.MOVIES_SCD2
SELECT ..., now AS valid_from, NULL AS valid_to, TRUE AS is_current
FROM (deduplicated bronze) src
LEFT JOIN SILVER.MOVIES_SCD2 tgt
    ON src.movie_id = tgt.movie_id AND tgt.is_current = TRUE
WHERE tgt.movie_id IS NULL
```

**Tracked columns:**
- Movies: `popularity`, `vote_average`, `vote_count`
- Weather: `temp_c`, `humidity_pct`, `weather`

### Seeing SCD2 in action

First run → all rows are `is_current = TRUE`, zero historical.

Run again later (an hour for weather, a day for movies) → changed rows get expired (`is_current = FALSE`, `valid_to` filled) and new current rows are inserted.

Query to see changes:

```sql
SELECT *
FROM SILVER.MOVIES_SCD2
WHERE MOVIE_ID IN (
    SELECT MOVIE_ID FROM SILVER.MOVIES_SCD2
    GROUP BY MOVIE_ID HAVING COUNT(*) > 1
)
ORDER BY MOVIE_ID, VALID_FROM;
```

---

## Concepts Covered

| Concept | Where |
|---------|-------|
| API extraction | Cells 2–3 in each notebook |
| Bronze layer (raw landing) | `BRONZE.MOVIES`, `BRONZE.WEATHER` |
| Silver layer (SCD2) | `SILVER.MOVIES_SCD2`, `SILVER.WEATHER_SCD2` |
| SCD2 (Slowly Changing Dimension Type 2) | Expire old rows, insert new versions |
| MERGE statement | Match on business key, update only changed rows |
| ROW_NUMBER() deduplication | Handle duplicate keys from API pagination |
| Incremental history | Run multiple times to build up change history |

---

## APIs Used

| API | Free tier | Sign up |
|-----|-----------|---------|
| TMDB (movies) | 1000 requests/day | https://www.themoviedb.org/settings/api |
| OpenWeatherMap | 60 calls/minute | https://openweathermap.org/api |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `InsecureRequestWarning` | Add `urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)` |
| `invalid identifier 'NAN'` when loading to Snowflake | Add `df = df.replace({np.nan: None})` before building tuples |
| `Insert value list does not match column list` | Don't use `SELECT *` with `ROW_NUMBER()` — list columns explicitly |
| `Cursor is closed` | You ran `conn.close()` — reconnect first |
| SSL `RecursionError` with Snowflake connector | Use Python 3.11 or 3.12, not 3.14 |
| `pip install` fails on corporate network | Add `--trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org` |
