import pandas as pd
import os
import glob
from datetime import datetime

def load_snapshot(filepath):
    """Loads a parquet snapshot and returns a processed DataFrame."""
    try:
        df = pd.read_parquet(filepath)
        # Ensure numeric columns
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
        return df
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def main():
    # Define the specific snapshots we want to analyze (Latest of each day)
    snapshots = {
        "2026-01-31": r"d:\PaxDei_Tool\data\history\year=2026\month=01\market_2026-01-31_21-08.parquet",
        "2026-02-01": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-01_17-07.parquet",
        "2026-02-02": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-02_14-42.parquet",
        "2026-02-03": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-03_20-20.parquet"
    }

    print(f"--- ECONOMY OVERVIEW (Last 4 Days) ---")
    print(f"Analyzing snapshots: {', '.join(snapshots.keys())}\n")

    daily_stats = []
    dfs = {}

    # 1. Load Data and Basic Stats
    for date_str, filepath in snapshots.items():
        if not os.path.exists(filepath):
            print(f"Warning: File not found for {date_str}: {filepath}")
            continue
            
        df = load_snapshot(filepath)
        if df is None: 
            continue
            
        dfs[date_str] = df
        
        total_value = (df['Price']).sum() # Total listing price (Price is usually total for the stack? Or unit? Tool fetched 'Price' which seemed to be total listing price)
        # Verify if 'Price' is total or unit. In fetch_market_prices: unit_price = price / quantity. So 'Price' is total listing value.
        
        total_listings = len(df)
        total_items_qty = df['Amount'].sum()
        unique_items = df['Item'].nunique()
        
        daily_stats.append({
            'Date': date_str,
            'TotalValue': total_value,
            'Listings': total_listings,
            'VolumeQty': total_items_qty,
            'UniqueItems': unique_items
        })

    stats_df = pd.DataFrame(daily_stats)
    
    # 2. Print General Trend
    print("### 1. Market Cap & Activity Trend")
    print(stats_df.to_string(index=False, formatters={
        'TotalValue': '{:,.0f}'.format,
        'VolumeQty': '{:,.0f}'.format,
        'Listings': '{:,.0f}'.format
    }))
    print("\n")

    # 3. Liquidity / Churn Analysis (crude approx)
    print("### 2. Daily Churn (Items Sold or Expired)")
    dates = sorted(list(dfs.keys()))
    for i in range(len(dates) - 1):
        d1 = dates[i]
        d2 = dates[i+1]
        
        df1 = dfs[d1]
        df2 = dfs[d2]
        
        # Identify listings that disappeared
        # We need a unique identifier. 'ListingID' is ideal.
        if 'ListingID' in df1.columns and 'ListingID' in df2.columns:
            ids1 = set(df1['ListingID'].dropna().unique())
            ids2 = set(df2['ListingID'].dropna().unique())
            
            disappeared_ids = ids1 - ids2
            new_ids = ids2 - ids1
            
            # Estimate value of disappeared items
            sold_or_expired_df = df1[df1['ListingID'].isin(disappeared_ids)]
            churn_value = sold_or_expired_df['Price'].sum()
            
            print(f"From {d1} to {d2}:")
            print(f"  - Listings Removed: {len(disappeared_ids)} (Value: {churn_value:,.0f}g)")
            print(f"  - New Listings:     {len(new_ids)}")
            
            # Top 3 items by value that disappeared (Potential Sales)
            top_churn_items = sold_or_expired_df.groupby('Item')['Price'].sum().sort_values(ascending=False).head(3)
            print(f"  - Top Removed Items (Value): {', '.join([f'{i} ({v:,.0f}g)' for i, v in top_churn_items.items()])}")
        else:
            print(f"Cannot calculate churn for {d1}->{d2} (Missing ListingID)")
    print("\n")

    # 4. Price Inflation/Deflation (Top Traded Items)
    print("### 3. Price Trends (Top 10 Common Items)")
    # Get items present in all snapshots with decent volume
    common_items = set(dfs[dates[0]]['Item'].unique())
    for d in dates[1:]:
        common_items &= set(dfs[d]['Item'].unique())
        
    # Calculate weighted average price for each item each day
    price_tracking = {}
    
    for d in dates:
        df = dfs[d]
        # Weighted Average Unit Price = Sum(Price) / Sum(Amount) per item
        grouped = df.groupby('Item').apply(lambda x: x['Price'].sum() / x['Amount'].sum() if x['Amount'].sum() > 0 else 0)
        price_tracking[d] = grouped

    price_df = pd.DataFrame(price_tracking)
    
    # Filter for high volume items (present in first day with > 50 listings approx count is hard, let's use global popularity)
    # Just take top items by Listing Count in the latest snapshot
    top_items_latest = dfs[dates[-1]]['Item'].value_counts().head(10).index.tolist()
    
    trend_data = []
    for item in top_items_latest:
        if item in price_df.index:
            row = price_df.loc[item]
            start_price = row[dates[0]]
            end_price = row[dates[-1]]
            change = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
            trend_data.append({
                'Item': item,
                'StartPrice': start_price,
                'EndPrice': end_price,
                'Change%': change
            })
            
    trend_df = pd.DataFrame(trend_data).sort_values('Change%', ascending=False)
    print(trend_df.to_string(index=False, formatters={
        'StartPrice': '{:.2f}'.format,
        'EndPrice': '{:.2f}'.format,
        'Change%': '{:+.2f}%'.format
    }))

if __name__ == "__main__":
    main()
