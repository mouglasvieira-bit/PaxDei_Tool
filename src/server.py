
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import sys
import os
import pandas as pd
import json

# Add src to path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.market import MarketAnalyzer
from modules.crafting import CraftingAnalyzer
from modules.logistics import ArbitrageFinder

app = FastAPI(title="Pax Dei Advisor API")

# Allow CORS for local development (Frontend running on port 5173 usually)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, allow all. In prod, lock this down.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_data_dir():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data")

# Mount Static Files
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir)

app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
def read_root():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.get("/api/market/liquidity")
def get_liquidity():
    data_dir = get_data_dir()
    analyzer = MarketAnalyzer(data_dir)
    df = analyzer.check_liquidity()
    if df is None or df.empty:
        # Try to read generated CSV if live calculation returns nothing (e.g. no new snapshot turnover)
        csv_path = os.path.join(data_dir, "liquidez_diaria.csv")
        if os.path.exists(csv_path):
             df = pd.read_csv(csv_path)
        else:
            return []
            
    # Convert to list of dicts
    return df.head(50).to_dict(orient="records")

@app.get("/api/crafting/opportunities")
def get_crafting_opportunities(top: int = 20):
    data_dir = get_data_dir()
    analyzer = CraftingAnalyzer(data_dir)
    df = analyzer.analyze_profitability()
    
    if df is None or df.empty:
        # Fallback to CSV
        csv_path = os.path.join(data_dir, "analise_disparidade.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            # Ensure sort
            df = df.sort_values(by='Spread', ascending=False)
        else:
            return []
            
    return df.head(top).to_dict(orient="records")

@app.get("/api/logistics/arbitrage")
def get_arbitrage():
    data_dir = get_data_dir()
    finder = ArbitrageFinder(data_dir)
    df = finder.find_opportunities()
    
    if df is None or df.empty:
        return []
        
    # Deduplicate for variety
    df_dedup = df.drop_duplicates(subset=['Item']).head(20)
    return df_dedup.fillna(0).to_dict(orient="records")

@app.get("/api/market/search")
def search_items(query: str):
    data_dir = get_data_dir()
    # Simple search against latest parquet or catalogue
    # For speed, let's load the latest snapshot's unique items
    analyzer = MarketAnalyzer(data_dir)
    return analyzer.search_items(query)

@app.get("/api/market/item/{item_name}/history")
def get_item_history_api(item_name: str):
    data_dir = get_data_dir()
    analyzer = MarketAnalyzer(data_dir)
    stats = analyzer.get_item_history(item_name)
    if stats is None or stats.empty:
        return []
    
    # Reset index to get SnapshotDate as a column
    stats = stats.reset_index()
    # Convert timestamps to string for JSON serialization
    stats['SnapshotDate'] = stats['SnapshotDate'].astype(str)
    # Fill NaN with 0 to valid JSON error
    stats = stats.fillna(0)
    return stats.to_dict(orient="records")

@app.get("/api/market/item/{item_name}/producers")
def get_item_producers_api(item_name: str):
    data_dir = get_data_dir()
    analyzer = MarketAnalyzer(data_dir)
    stats = analyzer.get_producer_stats(item_name)
    if stats is None or stats.empty:
        return []
        
    stats = stats.reset_index()
    return stats.fillna(0).head(5).to_dict(orient="records")

@app.get("/api/logistics/suppliers")
def get_suppliers():
    data_dir = get_data_dir()
    csv_path = os.path.join(data_dir, "suppliers.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        return df.fillna("").to_dict(orient="records")
    return []

@app.get("/api/logistics/orders")
def get_orders():
    data_dir = get_data_dir()
    csv_path = os.path.join(data_dir, "client_orders.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        return df.fillna("").to_dict(orient="records")
    return []

@app.post("/api/admin/fetch-prices")
def trigger_fetch_prices():
    import subprocess
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "etl", "fetch_market_prices.py")
    python_exe = r"D:\py\python.exe" # Hardcoded as per environment
    
    try:
        # Run synchronous (blocking) for MVP simplicity to ensure it completes before UI feedback
        # In prod, this should be a background task
        result = subprocess.run([python_exe, script_path], capture_output=True, text=True, check=True)
        return {"status": "success", "message": "Market prices updated successfully.", "log": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "message": "Script execution failed.", "log": e.stderr}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("Starting API Server on http://localhost:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)
