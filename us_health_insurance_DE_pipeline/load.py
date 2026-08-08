import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_to_sqlite(
    df: pd.DataFrame,
    table_name: str,
    db_path: str = "data/insurance.db",
    incremental: bool = False,
    unique_key: str = "row_id",
) -> None:
    conn = sqlite3.connect(db_path)

    if not incremental:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"Full refresh: wrote {len(df)} rows to {table_name}")
        conn.close()
        return

    # incremental: only load rows not already present
    existing_tables = pd.read_sql(
        "select name from sqlite_master where type='table'", conn
    )["name"].tolist()

    if table_name not in existing_tables:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"Initial load: wrote {len(df)} rows to {table_name}")
        conn.close()
        return

    existing_cols = pd.read_sql(f"pragma table_info({table_name})", conn)["name"].tolist()
    if unique_key not in existing_cols:
        raise ValueError(
            f"'{unique_key}' not found in existing '{table_name}' table. "
            f"Schema may have changed — consider dropping and reloading the table."
        )

    existing_keys = pd.read_sql(f"select {unique_key} from {table_name}", conn)[unique_key]
    new_rows = df[~df[unique_key].isin(existing_keys)]

    if new_rows.empty:
        logger.info(f"No new rows for {table_name}")
    else:
        new_rows.to_sql(table_name, conn, if_exists="append", index=False)
        logger.info(f"Incremental load: appended {len(new_rows)} new rows to {table_name}")

    conn.close()
