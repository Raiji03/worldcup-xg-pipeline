"""
World Cup ETL - orchestrated with Airflow's TaskFlow API.

Adapted from two standalone scripts (WorldCup.py, WorldCupTransform.py).
Each task writes its output to a parquet/DuckDB file under /opt/airflow/output
(mounted from ./output on the host) and passes only the *file path* through
XCom, since a pandas DataFrame is not JSON-serializable.
"""

import warnings

import pandas as pd
import pendulum

from airflow.sdk import dag, task

warnings.filterwarnings("ignore")

COMPETITION_ID = 43  # 2022 Men's World Cup
SEASON_ID = 106

OUTPUT_DIR = "/opt/airflow/output"
RAW_FILE = f"{OUTPUT_DIR}/wc2022_shots_raw.parquet"
CLEAN_FILE = f"{OUTPUT_DIR}/wc2022_shots_clean.parquet"
DB_FILE = f"{OUTPUT_DIR}/worldcup.duckdb"


@dag(
    dag_id="worldcup_etl",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["worldcup", "statsbomb"],
)
def worldcup_etl():
    @task
    def extract() -> str:
        from statsbombpy import sb

        matches = sb.matches(competition_id=COMPETITION_ID, season_id=SEASON_ID)

        frames = []
        for match_id in matches["match_id"]:
            events = sb.events(match_id=match_id)
            shots = events[events["type"] == "Shot"].copy()
            shots["match_id"] = match_id
            frames.append(shots)

        all_shots = pd.concat(frames, ignore_index=True)
        all_shots.to_parquet(RAW_FILE)
        return RAW_FILE

    @task
    def transform(raw_path: str) -> str:
        raw = pd.read_parquet(raw_path)

        df = pd.DataFrame()
        df["match_id"] = raw["match_id"]
        df["minute"] = raw["minute"]
        df["team"] = raw["team"]
        df["player"] = raw["player"]
        df["x"] = raw["location"].apply(lambda v: v[0] if isinstance(v, list) else None)
        df["y"] = raw["location"].apply(lambda v: v[1] if isinstance(v, list) else None)
        df["xg"] = raw["shot_statsbomb_xg"]
        df["outcome"] = raw["shot_outcome"]
        df["is_goal"] = (raw["shot_outcome"] == "Goal").astype(int)
        df["body_part"] = raw["shot_body_part"]
        df["play_pattern"] = raw["play_pattern"]

        df.to_parquet(CLEAN_FILE)
        return CLEAN_FILE

    @task
    def load(clean_path: str) -> str:
        import duckdb

        df = pd.read_parquet(clean_path)
        con = duckdb.connect(DB_FILE)
        con.execute("CREATE OR REPLACE TABLE shots AS SELECT * FROM df")
        con.close()
        return DB_FILE

    load(transform(extract()))


worldcup_etl()
