import logging

from extractor import extract_insurance
from transform import clean_insurance, summary_by_region_smoker
from load import load_to_sqlite
from tests import load_table, test_not_null, test_unique, test_accepted_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    logger.info("Starting pipeline")

    df_raw = extract_insurance("data/insurance.csv")
    df_clean = clean_insurance(df_raw)
    df_summary = summary_by_region_smoker(df_clean)

    load_to_sqlite(df_clean, "stg_insurance", incremental=True, unique_key="row_id")
    load_to_sqlite(df_summary, "mart_insurance_summary")  # always full refresh — it's an aggregate

    logger.info("Running tests")
    df_stg = load_table("stg_insurance")
    test_not_null(df_stg, ["row_id", "age", "sex", "bmi", "children", "smoker", "region", "charges"])
    test_unique(df_stg, "row_id")
    test_accepted_values(df_stg, "sex", ["male", "female"])
    test_accepted_values(df_stg, "region", ["northeast", "northwest", "southeast", "southwest"])

    logger.info("Pipeline complete")


if __name__ == "__main__":
    run_pipeline()
