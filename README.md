# World Cup xG Analytics Pipeline

An end-to-end ETL pipeline that ingests shot-level event data from the 2022 FIFA World Cup, transforms it into a clean analytical model, and loads it into a queryable database — orchestrated with Apache Airflow and containerized with Docker.

Built to extend a prior transit-data ETL project onto a richer, messier, nested dataset: football event data with expected-goals (xG) values for every shot in the tournament.

## What it does

The pipeline runs in three orchestrated stages:

1. **Extract** — pulls shot events from all 64 matches of the 2022 World Cup via the StatsBomb open dataset (`statsbombpy`) and saves the raw output to Parquet.
2. **Transform** — flattens nested fields (pitch-coordinate arrays, shot metadata) into a clean, columnar shots table using pandas, and derives an `is_goal` flag from shot outcomes.
3. **Load** — writes the clean table into a DuckDB database for fast SQL analytics.

All three stages are wrapped as tasks in an Airflow DAG (TaskFlow API), passing data between stages via files — the correct pattern for isolated task processes.

## Architecture

```
StatsBomb API ──▶ [Extract] ──▶ shots_raw.parquet
                                      │
                                      ▼
                                 [Transform] ──▶ shots_clean.parquet
                                      │
                                      ▼
                                  [Load] ──▶ worldcup.duckdb
```

Orchestrated by Apache Airflow, running in Docker.

## Sample insights

Once loaded, the data answers real analytical questions with a few lines of SQL. From **1,494 shots across 64 matches**:

**xG overperformers** — teams that scored more than their shot quality predicted:

| Team | xG | Goals | Over/Under |
|------|-----|-------|------------|
| Portugal | 7.3 | 12 | +4.7 |
| England | 8.7 | 13 | +4.3 |
| Netherlands | 8.9 | 13 | +4.1 |
| France | 15.0 | 18 | +3.0 |
| Argentina | 21.0 | 23 | +2.0 |

**Most clinical finishers:**

| Player | Goals | xG |
|--------|-------|-----|
| Lionel Messi | 9 | 7.60 |
| Kylian Mbappé | 9 | 5.02 |
| Olivier Giroud | 4 | 3.04 |
| Julián Álvarez | 4 | 1.91 |

Mbappé scored nearly 4 goals more than expected — the tournament's most clinical output relative to chance quality.

**Goals by play pattern:**

| Pattern | Goals |
|---------|-------|
| Open play | 51 |
| From free kick | 29 |
| From throw-in | 26 |
| From corner | 17 |

## Tech stack

- **Python** — pipeline logic
- **statsbombpy** — data extraction from StatsBomb open data
- **pandas** — transformation and flattening of nested event data
- **DuckDB** — analytical data store (in-process OLAP)
- **Apache Airflow** — workflow orchestration (TaskFlow API)
- **Docker** — containerized Airflow environment

## Repo structure

```
worldcup-xg-pipeline/
├── README.md
├── requirements.txt
├── wc_extract_starter.py     # Stage 1: Extract
├── wc_transform_load.py       # Stages 2 & 3: Transform + Load
├── dags/
│   └── worldcup_dag.py        # Airflow DAG wiring all three stages
└── .gitignore
```

## Running it

**Standalone (no orchestration):**

```bash
pip install -r requirements.txt
python wc_extract_starter.py     # produces shots_raw.parquet
python wc_transform_load.py       # produces worldcup.duckdb + prints insights
```

**With Airflow (Docker):**

Place `worldcup_dag.py` in your Airflow `dags/` folder, add the project
dependencies to the Airflow image, then trigger `worldcup_etl` from the
Airflow UI at `localhost:8080`.

## Data source

Event data from [StatsBomb Open Data](https://github.com/statsbomb/open-data),
provided free for research and education. All analysis credits StatsBomb as the
data provider.
