
import pandas as pd
import glob
import os

def check_bargains():
    # 1. Load Data
    try:
        df = pd.read_parquet('data/selene_latest.parquet')
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Define Context
    target_zones = ['merrie-gael', 'merrie-caster']
    
    # User requested items - mapped to likely In-Game names
    # Note: "Flax" often refers to Flax Fiber or Stalks. Including varieties.
    # "Linen" -> Linen Cloth.
    target_items = [
        'Barley Grain', 
        'Linen Cloth', 
        'Coarse Linen Cloth', 
        'Flax', 'Flax Fiber', 'Flax Stalks',
        'Antler', 
        'Wool Fiber', 
        'Wool Yarn', 'Coarse Wool Yarn',
        'Elderberry'
    ]

    # Normalize item names in data for easier matching if needed, 
    # but exact match is safer first. 
    # Let's filter df to only these items to save processing
    df_filtered = df[df['Item'].isin(target_items)].copy()

    # 3. Calculate References
    # Reference 1: Kerys Median (All Kerys zones)
    kerys_mask = df['Zone'].str.startswith('kerys-')
    df_kerys = df[kerys_mask & df['Item'].isin(target_items)]
    median_kerys = df_kerys.groupby('Item')['UnitPrice'].median()

    # Reference 2: Server Median (All zones)
    df_server = df[df['Item'].isin(target_items)]
    median_server = df_server.groupby('Item')['UnitPrice'].median()

    # 4. Filter for Target Opportunities
    # Only look at the zones the user is visiting
    opportunities = []

    # Filter target listings
    # We want listings in the target zones for the specific items
    target_listings = df_filtered[df_filtered['Zone'].isin(target_zones)]

    for index, row in target_listings.iterrows():
        item = row['Item']
        price = row['UnitPrice']
        zone = row['Zone']
        amount = row['Amount']
        
        # Get benchmarks
        k_med = median_kerys.get(item, float('inf')) # default to distinct high if no data
        s_med = median_server.get(item, float('inf'))
        
        # Logic: Price below Median of Kerrys OR Server
        # We treat 'NaN' medians (no data) as 'no benchmark', so we can't be 'below' it easily.
        # But if it's missing in Kerys, maybe ANY price is good? 
        # User said: "below median of kerrys OR server".
        # If Kerys has no data, we rely on Server.
        # If both empty, we can't judge.
        
        is_deal = False
        reasons = []

        # Check vs Kerys
        if pd.notna(k_med) and price < k_med:
            # Calculate discount
            disc = (1 - price/k_med) * 100
            reasons.append(f"Below Kerys ({k_med:.1f}, -{disc:.0f}%)")
            is_deal = True
        
        # Check vs Server
        if pd.notna(s_med) and price < s_med:
            disc = (1 - price/s_med) * 100
            reasons.append(f"Below Server ({s_med:.1f}, -{disc:.0f}%)")
            is_deal = True

        if is_deal:
            opportunities.append({
                'Item': item,
                'Zone': zone,
                'Price': price,
                'Amount': amount,
                'Ref_Kerys': k_med if pd.notna(k_med) else None,
                'Ref_Server': s_med,
                'Reason': ", ".join(reasons)
            })

    # 5. Output Results
    if not opportunities:
        print("No bargains found in Gael or Caster for the requested items.")
    else:
        results_df = pd.DataFrame(opportunities)
        # Sort by Item then Price
        results_df = results_df.sort_values(by=['Item', 'Price'])
        
        print(f"--- BARGAIN REPORT: {', '.join(target_zones)} ---")
        print(results_df.to_string(index=False))
        
        # CSV Export for user convenience
        output_path = 'data/trip_bargains.csv'
        results_df.to_csv(output_path, index=False)
        print(f"\nReport saved to {output_path}")

if __name__ == "__main__":
    check_bargains()
