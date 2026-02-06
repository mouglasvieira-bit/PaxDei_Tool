import pandas as pd
import os
import glob
from datetime import datetime

def load_snapshot(filepath):
    try:
        df = pd.read_parquet(filepath)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
        return df
    except Exception as e:
        return None

def main():
    snapshots = {
        "2026-01-31": r"d:\PaxDei_Tool\data\history\year=2026\month=01\market_2026-01-31_21-08.parquet",
        "2026-02-01": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-01_17-07.parquet",
        "2026-02-02": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-02_14-42.parquet",
        "2026-02-03": r"d:\PaxDei_Tool\data\history\year=2026\month=02\market_2026-02-03_20-20.parquet"
    }

    print(f"--- END GAME & SUPPLIES TREND (Last 4 Days) ---")

    dfs = {}
    for date_str, filepath in snapshots.items():
        if os.path.exists(filepath):
            dfs[date_str] = load_snapshot(filepath)

    if not dfs:
        print("No data found.")
        return

    # Define End Game Keywords
    keywords = ["Steel", "Gold", "Silver", "Sanctified", "Plate", "Magic"]
    
    # helper to check if item matches any keyword
    def is_endgame(name):
        if not isinstance(name, str): return False
        return any(k in name for k in keywords)

    dates = sorted(list(dfs.keys()))
    start_date = dates[0]
    end_date = dates[-1]
    
    # Build a combined list of interesting items found in the latest snapshot
    latest_df = dfs[end_date]
    endgame_items = latest_df[latest_df['Item'].apply(is_endgame)]['Item'].unique()
    
    results = []

    for item in endgame_items:
        # Get Start Stats
        df_start = dfs[start_date]
        grouped_start = df_start[df_start['Item'] == item]
        
        # Get End Stats
        df_end = dfs[end_date]
        grouped_end = df_end[df_end['Item'] == item]
        
        if grouped_start.empty or grouped_end.empty:
            continue
            
        # Calc Unit Price (Weighted Avg)
        start_qty = grouped_start['Amount'].sum()
        start_price = grouped_start['Price'].sum() / start_qty if start_qty > 0 else 0
        
        end_qty = grouped_end['Amount'].sum()
        end_price = grouped_end['Price'].sum() / end_qty if end_qty > 0 else 0
        
        price_change = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
        supply_change = ((end_qty - start_qty) / start_qty * 100) if start_qty > 0 else 0
        
        # Filter noise (extremely low volume items)
        if start_qty < 5 and end_qty < 5:
            continue

        results.append({
            'Item': item,
            'Price (Start)': start_price,
            'Price (End)': end_price,
            'Price Change%': price_change,
            'Supply (End)': end_qty,
            'Supply Change%': supply_change
        })

    # Convert to DF
    res_df = pd.DataFrame(results)
    
    # Sort by Importance (maybe Price * Supply? or just Supply?)
    # Let's sort by Supply (End) to see the most active markets first
    res_df = res_df.sort_values('Supply (End)', ascending=False)
    
    print("\n### High Volume End-Game Items")
    print(res_df.head(20).to_string(index=False, formatters={
        'Price (Start)': '{:.2f}'.format,
        'Price (End)': '{:.2f}'.format,
        'Price Change%': '{:+,.1f}%'.format,
        'Supply (End)': '{:,.0f}'.format,
        'Supply Change%': '{:+,.1f}%'.format
    }))
    
    # Check for biggest price drops (Opportunities?)
    print("\n### Major Price Moves (Winners & Losers)")
    significant_moves = res_df[abs(res_df['Price Change%']) > 10].sort_values('Price Change%', ascending=True)
    if not significant_moves.empty:
         print(significant_moves.head(10).to_string(index=False, formatters={
            'Price (Start)': '{:.2f}'.format,
            'Price (End)': '{:.2f}'.format,
            'Price Change%': '{:+,.1f}%'.format,
            'Supply (End)': '{:,.0f}'.format,
            'Supply Change%': '{:+,.1f}%'.format
        }))
    else:
        print("No major price moves > 10% detected in this category.")

if __name__ == "__main__":
    main()
