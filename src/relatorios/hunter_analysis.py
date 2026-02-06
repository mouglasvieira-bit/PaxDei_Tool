import pandas as pd
import os
import glob
import json
from datetime import datetime, timedelta

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
OUTPUT_FILE = os.path.join(DATA_DIR, "relatorio_cacador.md")

def load_market_history(days=7):
    """Loads parquet files from the last N days."""
    print(f"Loading history for last {days} days...")
    
    search_path = os.path.join(HISTORY_DIR, "**", "*.parquet")
    files = glob.glob(search_path, recursive=True)
    
    if not files:
        return pd.DataFrame()
        
    dfs = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    for f in files:
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        if mtime < cutoff_date - timedelta(days=1): 
            continue
            
        try:
            df = pd.read_parquet(f)
            if 'SnapshotDate' not in df.columns:
                 try:
                     fname = os.path.basename(f)
                     ts_str = fname.replace("market_", "").replace(".parquet", "")[:10]
                     df['SnapshotDate'] = pd.to_datetime(ts_str)
                 except:
                     df['SnapshotDate'] = mtime
            else:
                df['SnapshotDate'] = pd.to_datetime(df['SnapshotDate'])

            df = df[df['SnapshotDate'] >= cutoff_date]
            
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(dfs, ignore_index=True)
    full_df['Date'] = full_df['SnapshotDate'].dt.date
    
    def map_region(zone):
        if str(zone).startswith('kerys'): return 'Kerry'
        if str(zone).startswith('merrie'): return 'Merrie'
        if str(zone).startswith('ancien'): return 'Ancien'
        if str(zone).startswith('inis'): return 'Inis Gallia'
        return 'Other'
        
    full_df['Region'] = full_df['Zone'].apply(map_region)
    return full_df

def calculate_stats(df, item_name, region="Kerry"):
    """Calculates median price history and True Churn (Sales Proxy)."""
    mask = df['Item'] == item_name
    if region:
        mask &= df['Region'] == region
    
    item_df = df[mask].copy()
    if item_df.empty:
        return None
        
    daily = item_df.groupby('Date')['Price'].median().reset_index()
    daily.rename(columns={'Price': 'Median_Price'}, inplace=True)
    
    if daily.empty:
        return None

    last_date = daily['Date'].max()
    current_price = daily[daily['Date'] == last_date]['Median_Price'].values[0]
    
    # --- TRUE CHURN LOGIC (Disappearance = Sales) ---
    dates = sorted(item_df['Date'].unique())
    total_sales_est = 0
    
    if len(dates) > 1:
        for i in range(len(dates) - 1):
            d1 = dates[i]
            d2 = dates[i+1]
            
            ids_d1 = set(item_df[item_df['Date'] == d1]['ListingID'])
            ids_d2 = set(item_df[item_df['Date'] == d2]['ListingID'])
            
            sold_ids = ids_d1 - ids_d2
            total_sales_est += len(sold_ids)
    
    return {
        'current_price': current_price,
        'volume': total_sales_est
    }

def generate_hunter_report(df):
    print("Analyzing Hunter Items...")
    
    items_to_analyze = [
        "Rawhide", 
        "Nubuck Leather", 
        "Coarse Leather String", 
        "Fur Leather"
    ]
    
    results = []
    
    for item in items_to_analyze:
        stats = calculate_stats(df, item, "Kerry")
        
        if stats:
            price = stats['current_price']
            volume_7d = stats['volume']
            daily_rev = (price * volume_7d) / 7 if volume_7d > 0 else 0
            
            results.append({
                'item': item,
                'price': price,
                'volume_7d': volume_7d,
                'daily_rev': daily_rev
            })
        else:
            stats_global = calculate_stats(df, item, None) # Check global if Kerry is empty
            if stats_global:
                 # Penalty for not being in Kerry (travel cost/time), but show potential
                 price = stats_global['current_price']
                 volume_7d = stats_global['volume']
                 daily_rev = (price * volume_7d) / 7
                 results.append({
                    'item': f"{item} (Global)",
                    'price': price,
                    'volume_7d': volume_7d,
                    'daily_rev': daily_rev
                })
            else:
                results.append({'item': item, 'price': 0, 'volume_7d': 0, 'daily_rev': 0})

    # Sort by Daily Revenue
    results.sort(key=lambda x: x['daily_rev'], reverse=True)
    
    # Generate Markdown
    lines = []
    lines.append("# 🏹 Hunter's Profit Analysis Report")
    lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}\n")
    lines.append("This report analyzes which leather product offers the best combination of **Price** and **Sales Speed** (Liquidity).\n")
    
    lines.append("## 🏆 Recommendation")
    if results:
        winner = results[0]
        if winner['daily_rev'] > 0:
            lines.append(f"The best item to process and sell is **{winner['item']}**.")
            lines.append(f"- **Est. Daily Revenue:** {winner['daily_rev']:.1f}g")
            lines.append(f"- **Selling Price:** {winner['price']:.1f}g each")
            lines.append(f"- **Why?** It has the best balance of value and demand in the current market.\n")
        else:
            lines.append("No clear winner found. Market data might be sparse for these specific items in Kerry.\n")
    
    lines.append("## 📊 Detailed Breakdown")
    lines.append("| Item | Price (Median) | Est. Sales (7 Days) | Daily Revenue Potential |")
    lines.append("|---|---|---|---|")
    
    for r in results:
        lines.append(f"| **{r['item']}** | {r['price']:.1f}g | {r['volume_7d']} | **{r['daily_rev']:.1f}g** |")
        
    lines.append("\n> **Note:** 'Daily Revenue Potential' assumes you can supply enough to meet the daily demand. If demand is low, high prices won't generate actual gold.")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
        
    print(f"Report saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    df = load_market_history(days=7)
    if not df.empty:
        generate_hunter_report(df)
    else:
        print("No data found.")
