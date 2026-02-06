
import sys
import os
import pandas as pd

# Fix path to include src to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.market import MarketAnalyzer

def analyze_sand():
    data_dir = r"d:\PaxDei_Tool\data"
    analyzer = MarketAnalyzer(data_dir)
    
    print("Loading historical data...")
    df = analyzer.load_all_history()
    
    if df.empty:
        print("No historical data found.")
        return

    # Filter for Sand
    mask = df['Item'].str.contains('sand', case=False, na=False)
    sand_df = df[mask].copy()
    
    if sand_df.empty:
        print("No historical listings found for Sand.")
        return
        
    print(f"Found {len(sand_df)} historical listings for Sand.")
    
    # Check if Amount exists
    if 'Amount' not in sand_df.columns:
        print("Column 'Amount' missing. Cannot calculate quantity stats.")
        # Fallback to counting listings
        stats = sand_df.groupby('Zone').size().reset_index(name='Listing_Count')
        stats = stats.sort_values('Listing_Count', ascending=False)
    else:
        # Aggregate by Zone
        stats = sand_df.groupby('Zone').agg(
            Total_Quantity=('Amount', 'sum'),
            Listing_Count=('ListingID', 'count'),
            Avg_Stack_Size=('Amount', 'mean'),
            Unique_Sellers=('SellerHash', 'nunique') if 'SellerHash' in sand_df.columns else ('ListingID', lambda x: 0)
        ).reset_index()
        
        # Sort by Total Quantity (Volume)
        stats = stats.sort_values('Total_Quantity', ascending=False)

    print("\n--- Top 3 Zones for Sand (Historical) ---")
    print(stats.head(3).to_string(index=False))

if __name__ == "__main__":
    analyze_sand()
