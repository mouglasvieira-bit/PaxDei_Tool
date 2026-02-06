
import pandas as pd
import os

file_path = r"d:\PaxDei_Tool\data\selene_latest.parquet"
if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
    print("Columns:", df.columns.tolist())
    print("Head:", df.head().to_string())
else:
    print("File not found")
