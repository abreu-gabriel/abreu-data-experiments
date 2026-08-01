from extractor import extract_trips
from transform import clean_insurance, summary_by_region_smoker

df_raw = extract_trips("data/insurance.csv")
df_clean = clean_insurance(df_raw)
print(df_clean.dtypes)
print(summary_by_region_smoker(df_clean))