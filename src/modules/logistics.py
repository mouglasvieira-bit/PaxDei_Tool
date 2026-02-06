
import networkx as nx
import pandas as pd
import os

class PaxLogistics:
    def __init__(self):
        self.full_graph = nx.DiGraph()
        self.hub_node = "Lyonesse_Hub"
        self._build_world()
        
    def _build_world(self):
        self.provinces = {
            "Kerys": {
                "petras": ["Aven", "Bronyr", "Dolavon", "Nerys", "Pladenn"],
                "frontier_portals": ["Tremen", "Urmoth"],
                "pvp_portals": ["Parzival Gate"]
            },
            "Inis Gallia": {
                "petras": ["Goibniu", "Jura", "Kitz", "Levanth", "Nantes", "Paula", "Pontus", "Sanctum", "Tyria", "Verdon"],
                "frontier_portals": ["Tyria", "Levanth"],
                "pvp_portals": ["Vigdis Gate"]
            },
            "Merrie": {
                "petras": ["Baden", "Demesne", "Downs", "Ham", "Lea", "Ostia", "Shire", "Tryst", "Wittan"],
                "frontier_portals": ["Ham", "Agria"],
                "pvp_portals": ["Cormac Gate"]
            },
            "Ancien": {
                "petras": ["Durness", "Egeb", "Keria", "Luden", "Sinking", "Vinland"],
                "frontier_portals": ["Tursan", "Byla"],
                "pvp_portals": ["Kellen Gate"]
            }
        }
        
        self.full_graph.add_node(self.hub_node, type="hub")
        
        # Build Intra-Province
        for prov_name, data in self.provinces.items():
            for p in data["petras"]:
                node_id = f"{prov_name}_{p}"
                self.full_graph.add_node(node_id, type="petra", province=prov_name, name=p)
            
            all_portals = data["frontier_portals"] + data["pvp_portals"]
            for gp in all_portals:
                node_id = f"{prov_name}_Portal_{gp}"
                self.full_graph.add_node(node_id, type="portal", province=prov_name, name=gp)
                
            for p in data["petras"]:
                p_id = f"{prov_name}_{p}"
                for gp in all_portals:
                    gp_id = f"{prov_name}_Portal_{gp}"
                    self.full_graph.add_edge(p_id, gp_id, weight=20)
                    self.full_graph.add_edge(gp_id, p_id, weight=20)

        # Build Inter-Province
        frontier_map = [
            ("Kerys_Portal_Tremen", "Inis Gallia_Portal_Tyria"),
            ("Kerys_Portal_Urmoth", "Ancien_Portal_Tursan"),
            ("Inis Gallia_Portal_Levanth", "Merrie_Portal_Ham"),
            ("Merrie_Portal_Agria", "Ancien_Portal_Byla")
        ]
        
        for p1, p2 in frontier_map:
            if self.full_graph.has_node(p1) and self.full_graph.has_node(p2):
                self.full_graph.add_edge(p1, p2, weight=0)
                self.full_graph.add_edge(p2, p1, weight=0)

        # Build PvP
        pvp_map = [
            ("Kerys_Portal_Parzival Gate", self.hub_node),
            ("Inis Gallia_Portal_Vigdis Gate", self.hub_node),
            ("Merrie_Portal_Cormac Gate", self.hub_node),
            ("Ancien_Portal_Kellen Gate", self.hub_node)
        ]
        
        for p1, hub in pvp_map:
            if self.full_graph.has_node(p1):
                self.full_graph.add_edge(p1, hub, weight=0)
                self.full_graph.add_edge(hub, p1, weight=0)

    def _resolve_node(self, partial_name):
        for node, data in self.full_graph.nodes(data=True):
            if data.get("type") == "petra" and data.get("name") == partial_name:
                return node
        return None

    def compare_routes(self, origin, destination):
        start_node = self._resolve_node(origin)
        end_node = self._resolve_node(destination)
        
        if not start_node or not end_node:
            return None

        # PvP Route
        try:
            pvp_cost = nx.shortest_path_length(self.full_graph, start_node, end_node, weight="weight")
            pvp_path = nx.shortest_path(self.full_graph, start_node, end_node, weight="weight")
        except nx.NetworkXNoPath:
            pvp_cost = -1
            pvp_path = []

        # Safe Route
        unsafe_nodes = [self.hub_node]
        for n, data in self.full_graph.nodes(data=True):
            if data.get("type") == "portal" and "Gate" in data.get("name", ""): 
                unsafe_nodes.append(n)
        
        safe_graph = self.full_graph.copy()
        safe_graph.remove_nodes_from(unsafe_nodes)
        
        try:
            safe_cost = nx.shortest_path_length(safe_graph, start_node, end_node, weight="weight")
            safe_path = nx.shortest_path(safe_graph, start_node, end_node, weight="weight")
        except nx.NetworkXNoPath:
            safe_cost = -1
            safe_path = []
            
        return {
            "origin": origin,
            "destination": destination,
            "safe_route": {"cost": safe_cost, "path": safe_path},
            "pvp_route": {"cost": pvp_cost, "path": pvp_path}
        }

class ArbitrageFinder:
    def __init__(self, data_dir):
        self.listings_file = os.path.join(data_dir, "selene_latest.parquet")
        self.liquidity_file = os.path.join(data_dir, "liquidez_diaria.csv")

    def find_opportunities(self, budget=2000.0, min_margin=15.0):
        if not os.path.exists(self.listings_file) or not os.path.exists(self.liquidity_file):
            return pd.DataFrame()

        df_listings = pd.read_parquet(self.listings_file)
        df_liquidity = pd.read_csv(self.liquidity_file)
        
        # Merge Liquidity Data
        # We need 'Units_Sold' (Daily Volume) to weight the opportunity
        active_liquidity = df_liquidity[df_liquidity['Units_Sold'] > 0].copy()
        
        if active_liquidity.empty:
            return pd.DataFrame()
            
        # Calc Avg Sale Price from Liquidity (Total Volume / Units Sold)
        active_liquidity['Avg_Sale_Price'] = active_liquidity['Total_Volume'] / active_liquidity['Units_Sold']
        
        # Merge with current listings
        df_merged = df_listings.merge(
            active_liquidity[['Item', 'Avg_Sale_Price', 'Top_Zone', 'Units_Sold']], 
            on='Item', 
            how='inner'
        )
        
        # Clean Price Col
        price_col = 'UnitPrice' if 'UnitPrice' in df_merged.columns else 'Price'
        
        # Budget Filter
        df_opps = df_merged[df_merged[price_col] <= budget].copy()
        df_opps['Buy_Price'] = df_opps[price_col]
        
        # Smart Profit Calculation
        # Profit per unit
        df_opps['Unit_Profit'] = df_opps['Avg_Sale_Price'] - df_opps['Buy_Price']
        df_opps['Margin'] = (df_opps['Unit_Profit'] / df_opps['Buy_Price']) * 100
        
        # Smart Score: Unit Profit * min(Stock, Daily_Volume)
        # Assuming 10 stock per listing if not present (simple heuristic if no col)
        # But wait, df_merged is listing level. 
        # Let's assume effectively 1 unit per row if no Quantity column.
        # Ideally we group by Item to find total available stock, but the UI expects individual listings.
        
        # Let's keep listing level but score it based on the Item's Liquidity.
        # Score = Unit_Profit * log(Units_Sold + 1) -> Dampen effect of massive volume?
        # User requested: "weight quantity and liquidity"
        
        # Score = Unit_Profit * Units_Sold (Daily)
        df_opps['Score'] = df_opps['Unit_Profit'] * df_opps['Units_Sold']
        
        # Filter Margin & Positive Profit
        valid = df_opps[
            (df_opps['Margin'] >= min_margin) & 
            (df_opps['Unit_Profit'] > 0)
        ].copy()
        
        # Sort by Score (Volume * Profit) instead of Margin
        return valid.sort_values(by='Score', ascending=False)
