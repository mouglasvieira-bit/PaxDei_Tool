
import pandas as pd
import os

LATEST_FILE = r"d:\PaxDei_Tool\data\selene_latest.parquet"
ITEM_NAME = "Wool Fiber"
TARGET_ZONE = "ulaid"
REGION = "merrie"

def analyze_wool_fiber():
    if not os.path.exists(LATEST_FILE):
        print("Market data not found.")
        return

    df = pd.read_parquet(LATEST_FILE)
    
    # Filter for Wool Fiber
    mask_item = df['Item'].astype(str).str.contains(ITEM_NAME, case=False)
    df_item = df[mask_item].copy()
    
    if df_item.empty:
        print(f"No listings found for {ITEM_NAME}")
        return

    median_price = df_item['Price'].median()
    print(f"--- {ITEM_NAME} Analysis ---")
    print(f"Global Median Price: {median_price:.2f}")
    print(f"Global Total Stock: {df_item['Amount'].sum()}")
    
    # Filter for Ulaid or nearby (Merrie region)
    # We'll show specific Ulaid listings first, then other Merrie zones sorted by price
    
    df_item['Is_Ulaid'] = df_item['Zone'].str.contains(TARGET_ZONE, case=False)
    df_item['Is_Region'] = df_item['Zone'].str.contains(REGION, case=False)
    
    # Subsets
    ulaid_listings = df_item[df_item['Is_Ulaid']].sort_values('Price')
    nearby_listings = df_item[df_item['Is_Region'] & ~df_item['Is_Ulaid']].sort_values('Price')
    
    print(f"\n[ULAID LISTINGS]")
    if not ulaid_listings.empty:
        cols = ['Price', 'Amount', 'Zone', 'SellerHash', 'LastSeen']
        print(ulaid_listings[cols].to_string(index=False))
    else:
        print("No listings found specifically in Ulaid.")
        
    print(f"\n[NEARBY MERRIE REGION - BEST PRICES]")
    if not nearby_listings.empty:
        # Show top 10 cheapest nearby
        cols = ['Price', 'Amount', 'Zone', 'SellerHash']
        print(nearby_listings.head(10)[cols].to_string(index=False))
    else:
        print("No nearby listings found in Merrie region.")
        
    # Check massive stock holders globally just in case
    print(f"\n[TOP STOCK HOLDERS GLOBALLY]")
    stock_holders = df_item.groupby(['SellerHash', 'Zone']).agg({'Amount': 'sum', 'Price': 'mean'}).reset_index().sort_values('Amount', ascending=False).head(5)
    print(stock_holders.to_string(index=False))

if __name__ == "__main__":
    analyze_wool_fiber()
