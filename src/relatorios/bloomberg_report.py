import pandas as pd
import os
import glob
import json
import requests
import matplotlib.pyplot as plt
import sys
from datetime import datetime, timedelta

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
OUTPUT_FILE = os.path.join(DATA_DIR, "relatorio_bloomberg.md")
CHARTS_DIR = os.path.join(DATA_DIR, "charts")
ITEMS_JSON_URL = "https://data-cdn.gaming.tools/paxdei/market/items.json"
ITEMS_JSON_PATH = os.path.join(DATA_DIR, "items.json")

# Ensure charts directory exists
os.makedirs(CHARTS_DIR, exist_ok=True)

def load_item_categories():
    """Fetches or loads item definitions to map names to categories."""
    if not os.path.exists(ITEMS_JSON_PATH):
        print("Fetching item definitions...")
        try:
            resp = requests.get(ITEMS_JSON_URL)
            if resp.status_code == 200:
                with open(ITEMS_JSON_PATH, 'w', encoding='utf-8') as f:
                    json.dump(resp.json(), f, indent=2)
            else:
                print(f"Failed to fetch items: {resp.status_code}")
                return {}
        except Exception as e:
            print(f"Error fetching items: {e}")
            return {}
    
    with open(ITEMS_JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Map Item Name (En) -> Category (e.g. 'Weapons')
    # The JSON structure varies. We look for 'Professions' or infer from Type.
    # Structure: { "itemId": { "name": {"En": "Name"}, "type": "Type", ... } }
    
    item_cats = {}
    for iid, details in data.items():
        name = details.get('name', {}).get('En')
        # Simple heuristic based on known types/professions if available
        # If 'stats' exist, it might be gear.
        # This is a simplification; we might need to rely on name keywords for some.
        if name:
            item_cats[name] = details # Store full details for flexibility
            
    return item_cats

def categorize_item(item_name, item_db):
    """ returns simple category: Weapon, Armor, Material, Potion """
    details = item_db.get(item_name)
    if not details:
        return "Unknown"
        
    # Heuristics based on name or detailed stats
    # Ideally we'd have a 'Profession' field but the API is raw.
    
    name_lower = item_name.lower()
    
    if "potion" in name_lower or "brew" in name_lower or "wine" in name_lower:
        return "Alchemy"
    if "ingot" in name_lower or "ore" in name_lower:
        return "Metal"
    if "hide" in name_lower or "leather" in name_lower:
        return "Leather"
    if "linen" in name_lower or "cloth" in name_lower or "fabric" in name_lower or "thread" in name_lower:
        return "Tailoring"
    
    # Weapons keywords
    weapons = ["sword", "axe", "mace", "spear", "bow", "shield", "blade", "point", "pommel"]
    if any(w in name_lower for w in weapons):
        return "Weapon"
        
    # Armor keywords
    armor = ["helmet", "chest", "gloves", "boots", "pants", "tunic", "gambeson", "chainmail", "plate"]
    if any(a in name_lower for a in armor) and "wrought" not in name_lower: # avoid materials
        return "Armor"
        
    return "Material"

def load_market_history(days=7):
    """Loads parquet files from the last N days."""
    print(f"Loading history for last {days} days...")
    
    # Find files
    # Simplification: Just load all history and filter by date column
    # Ideally we filter glob by date path to save time.
    search_path = os.path.join(HISTORY_DIR, "**", "*.parquet")
    files = glob.glob(search_path, recursive=True)
    
    if not files:
        return pd.DataFrame()
        
    dfs = []
    cutoff_date = datetime.now() - timedelta(days=days)
    
    for f in files:
        # Check file mtime first as basic optimization
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        if mtime < cutoff_date - timedelta(days=1): # Buffer
            continue
            
        try:
            df = pd.read_parquet(f)
            # Infer date if not in columns (older history might not have it)
            if 'SnapshotDate' not in df.columns:
                 # Try filename
                 fname = os.path.basename(f)
                 # Expect: market_2026-02-04_08-36.parquet
                 try:
                     ts_str = fname.replace("market_", "").replace(".parquet", "")[:10] # YYYY-MM-DD
                     df['SnapshotDate'] = pd.to_datetime(ts_str)
                 except:
                     df['SnapshotDate'] = mtime
            else:
                df['SnapshotDate'] = pd.to_datetime(df['SnapshotDate'])

            # Filter rows
            df = df[df['SnapshotDate'] >= cutoff_date]
            
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(dfs, ignore_index=True)
    
    # Normalize Date
    full_df['Date'] = full_df['SnapshotDate'].dt.date
    
    # Map Regions
    def map_region(zone):
        if str(zone).startswith('kerys'): return 'Kerry'
        if str(zone).startswith('merrie'): return 'Merrie'
        if str(zone).startswith('ancien'): return 'Ancien'
        if str(zone).startswith('inis'): return 'Inis Gallia'
        return 'Other'
        
    full_df['Region'] = full_df['Zone'].apply(map_region)
    return full_df

def calculate_stats(df, item_name, region=None, exclude_zone=None):
    """Calculates median price history and True Churn (Sales Proxy)."""
    mask = df['Item'] == item_name
    if region:
        mask &= df['Region'] == region
    if exclude_zone:
        mask &= df['Zone'] != exclude_zone
    
    item_df = df[mask].copy()
    if item_df.empty:
        return None
        
    daily = item_df.groupby('Date')['Price'].median().reset_index()
    daily.rename(columns={'Price': 'Median_Price'}, inplace=True)
    
    last_date = daily['Date'].max()
    current_price = daily[daily['Date'] == last_date]['Median_Price'].values[0]
    
    first_date = daily['Date'].min()
    start_price = daily[daily['Date'] == first_date]['Median_Price'].values[0]
    
    delta = current_price - start_price
    pct_change = (delta / start_price * 100) if start_price > 0 else 0
    
    # --- TRUE CHURN LOGIC (Disappearance = Sales) ---
    dates = sorted(item_df['Date'].unique())
    total_sales_est = 0
    
    if len(dates) > 1:
        for i in range(len(dates) - 1):
            d1 = dates[i]
            d2 = dates[i+1]
            
            # IDs on Day 1
            ids_d1 = set(item_df[item_df['Date'] == d1]['ListingID'])
            # IDs on Day 2
            ids_d2 = set(item_df[item_df['Date'] == d2]['ListingID'])
            
            # Missing IDs = Sold (or Expired)
            sold_ids = ids_d1 - ids_d2
            total_sales_est += len(sold_ids)
    
    # If no history gap, churn is 0
    
    return {
        'current_price': current_price,
        'start_price': start_price,
        'pct_change': pct_change,
        'history': daily,
        'volume': total_sales_est # NEW: Sales Estimate
    }

def generate_report(df, item_db):
    report_lines = []
    report_lines.append("# 📈 Market Intelligence Report (Bloomberg Style)\n")
    report_lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}\n")
    report_lines.append(f"**Scope:** Last 7 Days | **Analyst:** Antigravity AI\n")

    # 0. Market Pulse
    report_lines.append("## 0. 📰 Market Pulse (Intro)\n")
    
    # Benchmarks
    benchmarks = ["Iron Ingot", "Wrought Iron Ingot", "Steel Ingot", "Bronze Ingot", "Worn Out Locket", "Worn Out Sigil"]
    report_lines.append("| Benchmark | Global Median | Kerry Median | Top Region (Demand) |")
    report_lines.append("|-----------|---------------|--------------|---------------------|")
    
    for item in benchmarks:
        stats_global = calculate_stats(df, item)
        stats_kerry = calculate_stats(df, item, "Kerry")
        
        vol_by_region = df[df['Item'] == item].groupby('Region')['ListingID'].count().sort_values(ascending=False)
        top_region = vol_by_region.index[0] if not vol_by_region.empty else "N/A"
        
        # Global Str
        if stats_global:
            g_price = stats_global['current_price']
            g_chg = stats_global['pct_change']
            g_sign = "+" if g_chg > 0 else ""
            g_str = f"{g_price:.1f}g ({g_sign}{g_chg:.1f}%)"
        else:
            g_str = "N/A"

        # Kerry Str
        if stats_kerry:
            k_price = stats_kerry['current_price']
            k_chg = stats_kerry['pct_change']
            k_sign = "+" if k_chg > 0 else ""
            k_str = f"{k_price:.1f}g" # Just price for compactness, or both? User said "put Kerry info alongside".
            # Let's verify space. Markdown tables wrap. Let's do Price + %.
            k_str = f"{k_price:.1f}g ({k_sign}{k_chg:.1f}%)"
        else:
            k_str = "N/A"
            
        report_lines.append(f"| **{item}** | {g_str} | {k_str} | {top_region} |")

    report_lines.append("\n**Executive Summary:** Market activity in Kerry shows mixed signals compared to the global averages. The following sections detail key sector movements.\n")

    # 1. CSI Index
    report_lines.append("## 1. 🏭 The Coal-Steel Index (CSI)\n")
    charcoal_stats = calculate_stats(df, "Charcoal", "Kerry")
    steel_stats = calculate_stats(df, "Steel Ingot", "Kerry")
    
    if charcoal_stats and steel_stats:
        c_hist = charcoal_stats['history'].set_index('Date')['Median_Price'].rename("Charcoal")
        s_hist = steel_stats['history'].set_index('Date')['Median_Price'].rename("Steel")
        csi_df = pd.concat([c_hist, s_hist], axis=1).dropna()
        csi_df['Ratio'] = csi_df['Steel'] / csi_df['Charcoal']
        
        current_ratio = csi_df['Ratio'].iloc[-1]
        start_ratio = csi_df['Ratio'].iloc[0]
        
        report_lines.append(f"**Purchasing Power Parity:**")
        report_lines.append(f"- **Today:** 1 Steel Ingot = **{current_ratio:.1f}** Charcoal")
        report_lines.append(f"- **7 Days Ago:** 1 Steel Ingot = **{start_ratio:.1f}** Charcoal")
        
        plt.figure(figsize=(10, 5))
        plt.plot(charcoal_stats['history']['Date'], charcoal_stats['history']['Median_Price'], marker='o', linestyle='-', color='black')
        plt.title('Median Charcoal Price (Kerry) - Last 7 Days')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        chart_path = os.path.join(CHARTS_DIR, "csi_charcoal.png")
        plt.savefig(chart_path)
        plt.close()
        report_lines.append(f"\n![Charcoal Trend](charts/csi_charcoal.png)\n")

    # Helper for Weapon/Armor Analysis
    def analyze_sector(sector_name, category_filter):
        report_lines.append(f"## {sector_name}\n")
        items = [i for i in df['Item'].unique() if categorize_item(i, item_db) == category_filter]
        
        if not items:
            report_lines.append("No data found.\n")
            return

        # 1. Churn Rate (Sales) - Top 2
        # Calculate volume for all items in Kerry
        kerry_volumes = []
        for i in items:
            s_kerry = calculate_stats(df, i, "Kerry")
            if s_kerry and s_kerry['volume'] > 0:
                kerry_volumes.append((i, s_kerry['volume'], s_kerry['current_price'], s_kerry['pct_change']))
        
        # Sort by Volume Descending
        kerry_volumes.sort(key=lambda x: x[1], reverse=True)
        top_churn = kerry_volumes[:2]
        
        report_lines.append(f"### 🏆 Top Churn (Kerry Sales Est.)")
        if top_churn:
            for rank, (item, vol, price, chg) in enumerate(top_churn, 1):
                sign = "+" if chg > 0 else ""
                report_lines.append(f"{rank}. **{item}**: ~{vol} Sold | Est. Price: {price:.1f}g ({sign}{chg:.1f}% 7d)")
        else:
            report_lines.append(f"No sales detected.")
        
        # 2. Inflation Leaders (Top 2)
        growth_list = []
        for i in items:
            s_kerry = calculate_stats(df, i, "Kerry")
            if s_kerry:
                growth_list.append((i, s_kerry['pct_change'], s_kerry['current_price']))
        
        growth_list.sort(key=lambda x: x[1], reverse=True)
        top_inflation = growth_list[:2]
        
        report_lines.append(f"\n### 🚀 High Inflation (Opportunity)")
        if top_inflation:
            for item, chg, price in top_inflation:
                report_lines.append(f"- **{item}**: +{chg:.1f}% (Current: {price:.1f}g)")
        else:
            report_lines.append("- None")

        # 3. Global Context
        # Start with simple volume proxy for region selection, then refine?
        # Only recalculate true churn for the winning region would be expensive.
        # Let's keep the region selection simple (Listing Count) for now -> "Availability/Activity"
        # OR attempt crude churn sum. Listing Count is safer for "Hub" identification.
        
        global_df = df[(df['Region'] != 'Kerry') & (df['Item'].isin(items))]
        if not global_df.empty:
            vol_by_region = global_df.groupby('Region')['ListingID'].count().sort_values(ascending=False)
            top_region = vol_by_region.index[0]
            
            # Find top churn item in that region? Expensive loop.
            # Fallback to Listing Volume for the "Driver" item in text, but label it.
            top_region_items = global_df[global_df['Region']==top_region]
            top_item = top_region_items.groupby('Item')['ListingID'].count().idxmax()
            report_lines.append(f"\n**Global Hotspot:** **{top_region}** leads activity, driven by **{top_item}**.\n")

    analyze_sector("2. ⚔️ Weaponsmithing", "Weapon")
    analyze_sector("3. 🛡️ Armorsmithing", "Armor")

    # 4. Tailoring
    report_lines.append("## 4. 🧵 Tailoring (The 'Linen' Index)\n")
    linen_stats = calculate_stats(df, "Linen String", "Kerry")
    if linen_stats:
        sign = "+" if linen_stats['pct_change'] > 0 else ""
        report_lines.append(f"- **Raw Material:** Linen String inflation is **{sign}{linen_stats['pct_change']:.1f}%** ({linen_stats['current_price']:.1f}g) in Kerry.")
    
    kerry_linen = df[(df['Region'] == 'Kerry') & (df['Item'] == "Linen String")]
    if not kerry_linen.empty:
        top_zone = kerry_linen.groupby('Zone')['ListingID'].count().idxmax()
        # Compare with Rest of Server
        server_stats = calculate_stats(df, "Linen String", exclude_zone=top_zone)
        server_price = server_stats['current_price'] if server_stats else 0
        report_lines.append(f"- **Linen Hub:** **{top_zone}** (Global Avg: {server_price:.1f}g).")

    # Products Inflation
    tailoring_items = [i for i in df['Item'].unique() if categorize_item(i, item_db) == "Tailoring" and i != "Linen String"]
    best_inf = (-999, None)
    best_def = (999, None)
    
    for t in tailoring_items:
        s = calculate_stats(df, t, "Kerry")
        if s:
            if s['pct_change'] > best_inf[0]: best_inf = (s['pct_change'], t)
            if s['pct_change'] < best_def[0]: best_def = (s['pct_change'], t)
            
    if best_inf[1]: 
        s = calculate_stats(df, best_inf[1], "Kerry")
        report_lines.append(f"- **Top Opportunity (Sell):** **{best_inf[1]}** (+{best_inf[0]:.1f}% | {s['current_price']:.1f}g).")
    if best_def[1]: 
        s = calculate_stats(df, best_def[1], "Kerry")
        report_lines.append(f"- **Top Opportunity (Buy):** **{best_def[1]}** ({best_def[0]:.1f}% | {s['current_price']:.1f}g).")

    # 5. Leatherworking
    report_lines.append("\n## 5. 🎒 Leatherworking\n")
    leather_stats = calculate_stats(df, "Coarse Leather Band", "Kerry")
    if leather_stats:
        sign = "+" if leather_stats['pct_change'] > 0 else ""
        report_lines.append(f"- **Raw Material:** Coarse Leather Band inflation is **{sign}{leather_stats['pct_change']:.1f}%** ({leather_stats['current_price']:.1f}g).")

    # 6. Alchemy (3 Glasses)
    report_lines.append("\n## 6. ⚗️ Alchemy (Glass & Potions)")
    glasses = ['Rough Glass', 'Glass', 'Pure Glass']
    
    for g_item in glasses:
        g_stats = calculate_stats(df, g_item, "Kerry")
        if g_stats:
            sign = "+" if g_stats['pct_change'] > 0 else ""
            
            # Find Top Supply Zone
            g_df = df[(df['Item'] == g_item) & (df['Region'] == 'Kerry')]
            if not g_df.empty:
                top_supply = g_df.groupby('Zone')['ListingID'].count().idxmax()
                
                # Compare with Rest of Server
                rest_stats = calculate_stats(df, g_item, exclude_zone=top_supply)
                rest_price = rest_stats['current_price'] if rest_stats else 0
                
                report_lines.append(f"\n**{g_item}**:")
                report_lines.append(f"- Inflation: **{sign}{g_stats['pct_change']:.1f}%** (Current: {g_stats['current_price']:.1f}g)")
                report_lines.append(f"- Best Supply: **{top_supply}** (Rest of Server: {rest_price:.1f}g)")
            else:
                report_lines.append(f"\n**{g_item}**: No supply in Kerry.")




    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join([line.strip() for line in report_lines]))
    
    print(f"Report generated: {OUTPUT_FILE}")

def main():
    print("Initializing Bloomberg Market Reporter...")
    
    # 1. Load Data
    categories = load_item_categories()
    df = load_market_history(days=7)
    
    if df.empty:
        print("No market history found! Please run 'python etl/fetch_market_prices.py' first.")
        return

    print(f"Loaded {len(df)} records from {df['SnapshotDate'].nunique()} snapshots.")
    
    # 2. Generate
    generate_report(df, categories)

if __name__ == "__main__":
    main()
