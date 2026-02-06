
import pandas as pd
import os
import glob
from datetime import datetime

# Configuration
ZONES_OF_INTEREST = ['ulaid', 'yarborne', 'ardbog', 'down', 'nene'] # Adjusted spelling approximations
DATA_DIR = r"d:\PaxDei_Tool\data"
LATEST_FILE = os.path.join(DATA_DIR, "selene_latest.parquet")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
CLIENT_ORDERS_FILE = os.path.join(DATA_DIR, "client_orders.csv")

def normalize_zone(zone_str):
    if not isinstance(zone_str, str): return ""
    return zone_str.lower()

def is_zone_match(zone_full, interests):
    norm = normalize_zone(zone_full)
    return any(z in norm for z in interests)

def get_client_items():
    if not os.path.exists(CLIENT_ORDERS_FILE):
        print("Client orders file not found.")
        return []
    df = pd.read_csv(CLIENT_ORDERS_FILE)
    return df['Item'].dropna().unique().tolist()

def analyze_current_market(client_items):
    print("\n--- ANALYZING CURRENT LISTINGS IN ROUTE ZONES ---")
    if not os.path.exists(LATEST_FILE):
        print("Latest market data not found.")
        return

    df = pd.read_parquet(LATEST_FILE)
    
    # Filter for client items
    mask_items = df['Item'].isin(client_items)
    df_items = df[mask_items].copy()
    
    if df_items.empty:
        print("No matches for client items in current market.")
        return

    # Calculate Median Price per Item (Server-wide)
    medians = df_items.groupby('Item')['Price'].median().reset_index().rename(columns={'Price': 'Median_Price'})
    
    # Merge median back
    df_items = df_items.merge(medians, on='Item')
    
    # Filter for Target Zones
    # "Ulaid, Yarborn, Ardbog, Down, Nene"
    # Matches: merrie-ulaid, merrie-ardbog, etc.
    df_items['In_Route'] = df_items['Zone'].apply(lambda x: is_zone_match(x, ZONES_OF_INTEREST))
    df_route = df_items[df_items['In_Route']].copy()
    
    # Filter for Discount (Price < Median)
    df_opportunities = df_route[df_route['Price'] < df_route['Median_Price']].copy()
    
    if df_opportunities.empty:
        print("No discounted items found in the target route zones.")
    else:
        df_opportunities['Discount_%'] = ((df_opportunities['Median_Price'] - df_opportunities['Price']) / df_opportunities['Median_Price']) * 100
        cols = ['Item', 'Price', 'Median_Price', 'Discount_%', 'Amount', 'Zone', 'SellerHash']
        print(df_opportunities.sort_values('Discount_%', ascending=False).head(20)[cols].to_string(index=False, float_format="%.2f"))

def analyze_historical_producers(client_items):
    print("\n--- ANALYZING HISTORICAL PRODUCERS FOR CLIENT ITEMS ---")
    search_path = os.path.join(HISTORY_DIR, "**", "*.parquet")
    files = glob.glob(search_path, recursive=True)
    
    if not files:
        print("No history files found.")
        return

    dfs = []
    # Load a sample or all? All might be heavy but let's try.
    # Optimization: Only load columns needed.
    for f in files:
        try:
            df = pd.read_parquet(f, columns=['Item', 'SellerHash', 'Zone', 'Price', 'ListingID', 'Amount'])
            # We don't need the date for this specific agg, just "historical relevance"
            dfs.append(df)
        except:
            pass
            
    if not dfs:
        return

    full_df = pd.concat(dfs, ignore_index=True)
    
    # Filter for client items
    mask = full_df['Item'].isin(client_items)
    relevant_df = full_df[mask].copy()
    
    if relevant_df.empty:
        print("No historical data for client items.")
        return
        
    # Group by Item and Seller to find "Big" producers
    # Relevance = Total Volume (Amount sum) + Consistency (Count listings)
    
    # 1. Total Volume per Seller per Item
    producer_stats = relevant_df.groupby(['Item', 'SellerHash']).agg({
        'Amount': 'sum',
        'ListingID': 'count',
        'Zone': lambda x: list(set(x))
    }).reset_index()
    
    producer_stats.rename(columns={'Amount': 'Total_Historical_Volume', 'ListingID': 'Frequency'}, inplace=True)
    
    # Sort and pick top 3 per item
    print(f"{'Item':<25} | {'SellerHash':<16} | {'Vol':<8} | {'Freq':<5} | {'Zones'}")
    print("-" * 80)
    
    items = sorted(client_items)
    for item in items:
        p_df = producer_stats[producer_stats['Item'] == item]
        if p_df.empty:
            continue
            
        top_producers = p_df.sort_values('Total_Historical_Volume', ascending=False).head(3)
        for _, row in top_producers.iterrows():
            zones = str(row['Zone'])[:30] # Truncate zones if too long
            print(f"{row['Item']:<25} | {row['SellerHash']:<16} | {row['Total_Historical_Volume']:<8.0f} | {row['Frequency']:<5} | {zones}")

if __name__ == "__main__":
    client_items = get_client_items()
    print(f"Loaded {len(client_items)} items from client orders.")
    
    analyze_current_market(client_items)
    analyze_historical_producers(client_items)
