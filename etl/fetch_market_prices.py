import requests
import pandas as pd
import os
import time
import time
from datetime import datetime
try:
    from huggingface_hub import HfApi
except ImportError:
    HfApi = None

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

def main():
    if load_dotenv:
        load_dotenv()

    # Relative Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    
    # URL for items
    items_url = "https://data-cdn.gaming.tools/paxdei/market/items.json"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # 1. Fetch Item Mapping
    print("Fetching Item Database...")
    id_to_name = {}
    try:
        resp = requests.get(items_url, headers=headers)
        if resp.status_code == 200:
            items_data = resp.json()
            if isinstance(items_data, dict):
                for iid, data in items_data.items():
                    name_dict = data.get('name', {})
                    name_en = name_dict.get('En', iid)
                    id_to_name[iid] = name_en
            print(f"Loaded {len(id_to_name)} item definitions.")
        else:
            print(f"Failed to fetch items: {resp.status_code}")
            return
    except Exception as e:
        print(f"Error fetching items: {e}")
        return

    # 2. Process Market Zones
    # Integrated Index Fetching (No local file needed)
    index_url = "https://data-cdn.gaming.tools/paxdei/market/index.json"
    print(f"Fetching Market Index from {index_url}...")
    
    selene_urls = []
    try:
        r_index = requests.get(index_url, headers=headers)
        if r_index.status_code == 200:
            data_index = r_index.json()
            # data_index is a list of strings: ['https://...', ...]
            if isinstance(data_index, list):
                # Filter for Selene in memory
                selene_urls = [
                    url for url in data_index 
                    if isinstance(url, str) and '/selene/' in url.lower()
                ]
            else:
                 print("Index JSON format unexpected (not a list).")
                 return
        else:
            print(f"Failed to fetch index: {r_index.status_code}")
            return
            
    except Exception as e:
        print(f"Error fetching index: {e}")
        return

    if not selene_urls:
        print("No Selene zones found in index.")
        return
        
    print(f"Found {len(selene_urls)} market zones for Selene.")

    all_prices = []
    
    for i, url in enumerate(selene_urls):
        print(f"[{i+1}/{len(selene_urls)}] Fetching {url}...")
        try:
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                zone_data = r.json()
                if isinstance(zone_data, list):
                    for listing in zone_data:
                        iid = listing.get('item_id')
                        price = listing.get('price')
                        
                        if iid and price is not None:
                            item_name = id_to_name.get(iid, iid)
                            parts = url.split('/')
                            zone_file = parts[-1].replace('.json', '')
                            domain = parts[-2]
                            full_zone = f"{domain}-{zone_file}"
                            
                            
                            # Enhanced Data Collection
                            listing_id = listing.get('id')
                            seller_hash = listing.get('avatar_hash')
                            durability = listing.get('durability')
                            quality = listing.get('quality')
                            
                            creation_date_ts = listing.get('creation_date')
                            last_seen_ts = listing.get('last_seen')
                            lifetime_days = listing.get('lifetime') # Lifetime in days
                            
                            expiration_date = None
                            if last_seen_ts and lifetime_days is not None:
                                try:
                                    # Calculate approximate expiration
                                    last_seen_dt = datetime.fromtimestamp(last_seen_ts)
                                    expiration_date = last_seen_dt + pd.Timedelta(days=float(lifetime_days))
                                except Exception:
                                    pass

                            quantity = listing.get('quantity', 1)
                            unit_price = price / quantity if quantity else price

                            all_prices.append({
                                'Item': item_name,
                                'Price': price,
                                'Amount': quantity,
                                'UnitPrice': unit_price,
                                'Zone': full_zone,
                                'Server': 'Selene',
                                'Timestamp': datetime.now(),
                                'ListingID': listing_id,
                                'SellerHash': seller_hash,
                                'Durability': durability,
                                'Quality': quality,
                                'TimeRemaining': lifetime_days,
                                'CreationDate': datetime.fromtimestamp(creation_date_ts) if creation_date_ts else None,
                                'LastSeen': datetime.fromtimestamp(last_seen_ts) if last_seen_ts else None,
                                'ExpirationDate': expiration_date
                            })
            time.sleep(0.01)
        except Exception as e:
            print(f"Error: {e}")

    # 3. Save as Parquet
    if all_prices:
        df_out = pd.DataFrame(all_prices)
        
        # Prepare Partitioning
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        
        # History Path: data/history/year=YYYY/month=MM/market_YYYY-MM-DD_HH-MM.parquet
        history_dir = os.path.join(data_dir, "history", f"year={year}", f"month={month}")
        os.makedirs(history_dir, exist_ok=True)
        
        timestamp_str = now.strftime("%H-%M")
        history_file = os.path.join(history_dir, f"market_{year}-{month}-{day}_{timestamp_str}.parquet")
        latest_file = os.path.join(data_dir, "selene_latest.parquet")
        
        print(f"Saving snapshot to: {history_file}")
        df_out.to_parquet(history_file, compression='snappy')
        
        print(f"Updating latest pointer: {latest_file}")
        df_out.to_parquet(latest_file, compression='snappy')
        
        print(f"Success! Saved {len(df_out)} prices.")
        
        # 4. Upload to Hugging Face (Optional/Automated)
        hf_token = os.environ.get("HF_TOKEN")
        if hf_token and HfApi:
            print("Uploading to Hugging Face...")
            try:
                api = HfApi(token=hf_token)
                # Upload the specific snapshot
                repo_id = "your-username/paxdei-market-data" # Placeholder, will need to be configured or passed as env
                # Using a generic repo structure or specific one. 
                # Ideally, we upload to a dataset.
                
                # Check if REPO_ID env var exists, else warn
                repo_id = os.environ.get("HF_REPO_ID")
                
                if repo_id:
                    path_in_repo = f"history/year={year}/month={month}/market_{year}-{month}-{day}_{timestamp_str}.parquet"
                    api.upload_file(
                        path_or_fileobj=history_file,
                        path_in_repo=path_in_repo,
                        repo_id=repo_id,
                        repo_type="dataset"
                    )
                    print(f"Uploaded to {repo_id}/{path_in_repo}")
                else:
                    print("HF_REPO_ID not set. Skipping upload.")
                    exit(1) # Fail if config missing
            except Exception as e:
                print(f"Failed to upload to HF: {e}")
                exit(1) # Fail the workflow!
        elif not HfApi:
            print("huggingface_hub not installed. Skipping upload.")
        else:
            print("HF_TOKEN not set. Skipping upload.")
    else:
        print("No prices collected.")
        exit(1) # Fail if empty!

if __name__ == "__main__":
    main()
