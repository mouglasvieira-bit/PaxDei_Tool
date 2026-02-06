import pandas as pd
import sys

target = "Bronze Sheet"
path = r"D:\PaxDei_Tool\data\selene_latest.parquet"

try:
    df = pd.read_parquet(path)
    match = df[df['Item'] == target]
    
    if match.empty:
        print(f"No listings found for '{target}' exactly.")
        # Try fuzzy search
        fuzzy = df[df['Item'].str.contains("Bronze", case=False)]
        print("\nSimilar items found:")
        print(fuzzy['Item'].unique())
    else:
        print(f"Found {len(match)} listings for '{target}':")
        print(match[['Price', 'Amount', 'Zone', 'SellerHash', 'LastSeen']].to_string())
except Exception as e:
    print(e)
