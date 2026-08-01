import pandas as pd

def extract_trips(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df