import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def load_table(table_name: str, db_path: str = "data/insurance.db") -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"select * from {table_name}", conn)
    conn.close()
    return df


def test_not_null(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        assert df[col].notna().all(), f"FAIL: {col} has nulls"
    logger.info("PASS: not_null")


def test_unique(df: pd.DataFrame, column: str) -> None:
    assert df[column].is_unique, f"FAIL: {column} has duplicates"
    logger.info("PASS: unique")


def test_accepted_values(df: pd.DataFrame, column: str, allowed: list) -> None:
    invalid = df[~df[column].isin(allowed)]
    assert invalid.empty, f"FAIL: {column} has invalid values: {invalid[column].unique()}"
    logger.info("PASS: accepted_values")
