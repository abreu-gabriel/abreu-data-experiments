import pandas as pd

def clean_insurance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.dropna(subset=["age", "sex", "bmi", "children", "smoker", "region", "charges"])

    df["age"] = df["age"].astype(int)
    df["bmi"] = df["bmi"].astype(float)
    df["children"] = df["children"].astype(int)
    df["charges"] = df["charges"].astype(float)

    df["is_smoker"] = df["smoker"].str.lower().eq("yes")
    df["sex"] = df["sex"].str.lower()
    df["region"] = df["region"].str.lower()

    # ASSUMPTION: synthetic PK — source has no natural unique identifier
    df = df.reset_index(drop=True)
    df.insert(0, "row_id", df.index)

    return df

def summary_by_region_smoker(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["region", "is_smoker"])
        .agg(
            member_count=("age", "count"),
            avg_age=("age", "mean"),
            avg_bmi=("bmi", "mean"),
            avg_charges=("charges", "mean"),
        )
        .reset_index()
    )
    return summary
