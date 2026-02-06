
import pandas as pd
import os
import glob
from datetime import datetime

class MarketAnalyzer:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.history_dir = os.path.join(data_dir, "history")
        self.listings_file = os.path.join(data_dir, "selene_latest.parquet")

    def load_all_history(self):
        """Loads all parquet files from history into a single dataframe with a Date column."""
        search_path = os.path.join(self.history_dir, "**", "*.parquet")
        files = glob.glob(search_path, recursive=True)
        
        if not files:
            return pd.DataFrame()
            
        dfs = []
        for f in files:
            try:
                # Extract date from filename: market_YYYY-MM-DD_HH-MM.parquet
                basename = os.path.basename(f)
                timestamp_str = basename.replace("market_", "").replace(".parquet", "")
                try:
                    snapshot_date = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M")
                except ValueError:
                    snapshot_date = datetime.fromtimestamp(os.path.getmtime(f))
                
                df = pd.read_parquet(f)
                df['SnapshotDate'] = snapshot_date
                dfs.append(df)
            except Exception as e:
                print(f"Skipping {f}: {e}")
                
        if not dfs:
            return pd.DataFrame()
            
        return pd.concat(dfs, ignore_index=True)

    def get_item_history(self, item_name):
        """Analyzes a specific item across snapshots."""
        full_df = self.load_all_history()
        if full_df.empty:
            return None

        # Filter by substring (case insensitive)
        mask = full_df['Item'].astype(str).str.contains(item_name, case=False, na=False)
        item_df = full_df[mask].copy()
        
        if item_df.empty:
            return None
            
        stats = item_df.groupby('SnapshotDate').agg(
            Min_Price=('Price', 'min'),
            Avg_Price=('Price', 'mean'),
            Median_Price=('Price', 'median'),
            Stock_Count=('ListingID', 'count'),
            Zones=('Zone', lambda x: list(set(x)))
        ).sort_values('SnapshotDate')
        
        # Calculate Churn
        snapshots = sorted(item_df['SnapshotDate'].unique())
        churn_data = []
        
        for i in range(len(snapshots) - 1):
            t0 = snapshots[i]
            t1 = snapshots[i+1]
            
            ids_t0 = set(item_df[item_df['SnapshotDate'] == t0]['ListingID'])
            ids_t1 = set(item_df[item_df['SnapshotDate'] == t1]['ListingID'])
            
            sold_ids = ids_t0 - ids_t1
            units_sold = len(sold_ids)
            
            # Volume
            sold_price_sum = item_df[(item_df['SnapshotDate'] == t0) & (item_df['ListingID'].isin(sold_ids))]['Price'].sum()
            
            churn_data.append({
                'SnapshotDate': t1,
                'Units_Sold_Since_Last': units_sold,
                'Volume_Sold': sold_price_sum
            })
            
        churn_df = pd.DataFrame(churn_data)
        
        if not churn_df.empty:
            stats = stats.merge(churn_df, on='SnapshotDate', how='left')
        else:
            stats['Units_Sold_Since_Last'] = 0
            stats['Volume_Sold'] = 0
            
        return stats

    def check_liquidity(self):
        """Compares the last two snapshots to find sold items (churn)."""
        search_path = os.path.join(self.history_dir, "**", "*.parquet")
        files = glob.glob(search_path, recursive=True)
        files.sort()
        
        if len(files) < 2:
            return None
            
        old_file, new_file = files[-2], files[-1]
        
        try:
            df_old = pd.read_parquet(old_file)
            df_new = pd.read_parquet(new_file)
        except Exception as e:
            print(f"Error reading parquet files: {e}")
            return None
            
        old_ids = set(df_old['ListingID'].dropna().unique())
        new_ids = set(df_new['ListingID'].dropna().unique())
        
        sold_ids = old_ids - new_ids
        
        if not sold_ids:
            return pd.DataFrame()
            
        sold_df = df_old[df_old['ListingID'].isin(sold_ids)].copy()
        
        if 'Amount' in sold_df.columns:
            liquidity_stats = sold_df.groupby('Item').agg(
                Units_Sold=('Amount', 'sum'),
                Total_Volume=('Price', 'sum')
            ).reset_index()
        else:
            liquidity_stats = sold_df.groupby('Item').agg(
                Units_Sold=('ListingID', 'count'),
                Total_Volume=('Price', 'sum')
            ).reset_index()

        # Top Zone
        zone_stats = sold_df.groupby(['Item', 'Zone']).size().reset_index(name='Zone_Count')
        zone_stats = zone_stats.sort_values(['Item', 'Zone_Count'], ascending=[True, False])
        top_zones = zone_stats.drop_duplicates(subset=['Item'])[['Item', 'Zone', 'Zone_Count']]
        top_zones.columns = ['Item', 'Top_Zone', 'Top_Zone_Sales']
        
        liquidity_stats = liquidity_stats.merge(top_zones, on='Item', how='left')
        liquidity_stats = liquidity_stats.sort_values(by='Units_Sold', ascending=False)
        
        return liquidity_stats

    def get_producer_stats(self, item_name):
        """Finds zones with the most unique sellers for an item."""
        full_df = self.load_all_history()
        if full_df.empty:
            return None
            
        mask = full_df['Item'].astype(str).str.contains(item_name, case=False, na=False)
        item_df = full_df[mask].copy()
        
        if item_df.empty:
            return None
            
        if 'SellerHash' not in item_df.columns:
            return None
            
        stats = item_df.groupby('Zone').agg({
            'SellerHash': 'nunique',
            'ListingID': 'nunique'
        }).rename(columns={
            'SellerHash': 'Unique_Producers',
            'ListingID': 'Unique_Listings'
        })
        
        return stats.sort_values('Unique_Producers', ascending=False)

    def get_top_sellers(self, item_name):
        """Returns the top sellers for a given item based on volume (Current Snapshot)."""
        if os.path.exists(self.listings_file):
            df = pd.read_parquet(self.listings_file)
        else:
            return None
            
        mask = df['Item'].astype(str).str.contains(item_name, case=False, na=False)
        item_df = df[mask].copy()
        
        if item_df.empty:
            return None
            
        if 'SellerHash' not in item_df.columns:
            return None
            
        # Group by SellerHash
        stats = item_df.groupby('SellerHash').agg({
            'Amount': 'sum',
            'ListingID': 'count',
            'Price': 'mean',
            'Zone': lambda x: list(set(x))
        }).rename(columns={
            'Amount': 'Total_Stock',
            'ListingID': 'Listing_Count',
            'Price': 'Avg_Price'
        })
        
        return stats.sort_values('Total_Stock', ascending=False)

    def search_items(self, query):
        """Search for items matching the query in the latest snapshot."""
        if not os.path.exists(self.listings_file):
            return []
            
        try:
            # We only need unique Item names. Reading entire file is heavy but OK for now.
            # Optimization: Cache this.
            df = pd.read_parquet(self.listings_file, columns=['Item'])
            unique_items = df['Item'].dropna().unique()
            
            # Simple substring match
            matches = [item for item in unique_items if query.lower() in item.lower()]
            return sorted(matches)[:20] # Limit to 20 results
        except Exception as e:
            print(f"Search error: {e}")
            return []
