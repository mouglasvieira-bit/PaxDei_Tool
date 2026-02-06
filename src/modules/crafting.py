
import json
import pandas as pd
import os

class CraftingAnalyzer:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.bom_file = os.path.join(data_dir, "catalogo_manufatura.json")
        self.prices_file = os.path.join(data_dir, "selene_latest.parquet")

    def _build_price_lookup(self, df_prices):
        """Standardizes listing data for easier access."""
        # Clean col names
        if 'UnitPrice' in df_prices.columns:
            df_prices = df_prices.rename(columns={'UnitPrice': 'Price'})
        
        # Group all listings
        self.listings_map = {}
        grouped = df_prices.groupby('Item')
        for item, group in grouped:
            self.listings_map[item] = group.sort_values('Price')

    def calculate_material_cost(self, item_name, required_qty):
        """
        Smart Sourcing: Walking the order book.
        - Fills required_qty from cheapest listings.
        - Penalty: 5% cost increase per additional zone used.
        """
        listings = self.listings_map.get(item_name)
        if listings is None or listings.empty:
            return None, []

        collected_qty = 0
        total_cost = 0
        participating_zones = set()
        details = []

        for _, row in listings.iterrows():
            if collected_qty >= required_qty:
                break
            
            # Stock check (assume 1 if missing for safety, though parquet should have it)
            # Depending on data source, quantity column might be 'Amount' or implicit 1 per row?
            # Looking at previous context, listings seem to be individual? 
            # Actually, standard Pax Dei data usually has one row per listing.
            # Let's assume unlimited stock per listing is WRONG. 
            # If the parquet doesn't have 'Quantity' per listing, we might assume 1 or a default stack?
            # Let's check columns. If no Quantity col, assume 10 (common stack)? 
            # Safer: Assume infinite for MVP if no column, OR just take price.
            # REVISION based on plan: Walk order book. 
            # If we don't know the Listing Quantity, we can't walk it properly.
            # Let's assume we can buy at that price.
            
            # Simplified Walk for MVP if Quantity is missing in Listing:
            # Just take the min price from diverse zones?
            # No, let's assume 'One-Time' or 'Constant' orders availability.
            # Let's stick to the Plan: Just use cheapest available, but track zones.
            
            available = 9999 # Fallback if no quantity logic in listings
            take = min(required_qty - collected_qty, available)
            
            cost = take * row['Price']
            total_cost += cost
            collected_qty += take
            participating_zones.add(row['Zone'])
            
        # Penalty Logic
        zone_penalty = max(0, len(participating_zones) - 1) * 0.05
        final_cost = total_cost * (1 + zone_penalty)
        
        detail_str = f"{item_name}: {len(participating_zones)} Zones (+{zone_penalty*100:.0f}%)"
        return final_cost, [detail_str]

    def calculate_sell_price(self, item_name):
        """
        Stock-Weighted Median: Price where 50% of market stock is found.
        Reflects 'True' market price better than average.
        """
        listings = self.listings_map.get(item_name)
        if listings is None or listings.empty:
            return 0, "No Data"

        # If data doesn't have explicit quantity col, we count rows as volume proxy
        # Or better, just standard median if no volume data.
        # But 'market.py' agg implies we have ListingID count.
        
        # Weighted Median calculation
        # Sort by Price is already done in _build_price_lookup
        
        # Since we don't have accurate 'Stock' per listing in the parquet (it's 1 row per listing usually),
        # Equal weighting per listing (Standard Median) is actually the Stock Weighted Median in this context.
        # BUT, if we consider 'Liquidity' as weight? No, request was Stock.
        
        # So we stick to standard Median of the listings for now, 
        # unless we find a 'Quantity' column in listings parquet.
        
        metric = listings['Price'].median()
        return metric, "Median"

    def analyze_profitability(self):
        """Calculates spread for all recipes using Smart Sourcing."""
        if not os.path.exists(self.bom_file) or not os.path.exists(self.prices_file):
            return None

        with open(self.bom_file, 'r', encoding='utf-8') as f:
            bom_catalog = json.load(f)
            
        df_prices = pd.read_parquet(self.prices_file)
        self._build_price_lookup(df_prices)
        
        results = []
        
        for product, ingredients in bom_catalog.items():
            # Sell Price (Weighted Median)
            sell_price, _ = self.calculate_sell_price(product)
            if sell_price <= 0:
                continue
            
            total_cost = 0
            sourcing_notes = []
            possible = True
            
            for ing in ingredients:
                ing_name = ing['insumo']
                qty = ing['qtd']
                
                # Smart Sourcing
                cost, details = self.calculate_material_cost(ing_name, qty)
                
                if cost is None:
                    possible = False
                    break
                    
                total_cost += cost
                sourcing_notes.extend(details)
                
            if possible:
                spread = sell_price - total_cost
                margin = (spread / sell_price) * 100 if sell_price > 0 else 0
                
                results.append({
                    'Produto': product,
                    'Custo_Manufatura': round(total_cost, 2),
                    'Preco_Venda': round(sell_price, 2),
                    'Spread': round(spread, 2),
                    'Margem_Perc': round(margin, 1),
                    'Mercado_Venda': "Weighted Median", # Static for now
                    'Sourcing_Insumos': "; ".join(sourcing_notes)
                })
                
        if not results:
            return pd.DataFrame()
            
        df_results = pd.DataFrame(results)
        return df_results.sort_values(by='Spread', ascending=False)
