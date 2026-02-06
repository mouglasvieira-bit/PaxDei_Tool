
import argparse
import sys
import os
import pandas as pd

# Add src to path just in case
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.market import MarketAnalyzer
from modules.crafting import CraftingAnalyzer
from modules.logistics import PaxLogistics, ArbitrageFinder

def get_data_dir():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data")

def handle_market(args):
    data_dir = get_data_dir()
    analyzer = MarketAnalyzer(data_dir)
    
    if args.liquidity:
        print("Running Liquidity Analysis...")
        df = analyzer.check_liquidity()
        if df is not None and not df.empty:
            print("\n--- TOP LIQUIDITY ITEMS (Recent Sales) ---")
            print(df.head(10)[['Item', 'Units_Sold', 'Total_Volume', 'Top_Zone']].to_string(index=False))
            # Save report
            out_file = os.path.join(data_dir, "liquidez_diaria.csv")
            df.to_csv(out_file, index=False)
            print(f"\nReport saved to {out_file}")
        else:
            print("No liquidity data found.")
            
    if args.sellers:
        print(f"Analyzing Producers for: {args.sellers}")
        # stats = analyzer.get_producer_stats(args.sellers)
        # if stats is not None:
        #     print(f"\n--- TOP ZONES FOR PRODUCERS ({args.sellers}) ---")
        #     print(stats.head(10).to_string())
        
        stats = analyzer.get_top_sellers(args.sellers)
        if stats is not None:
            print(f"\n--- TOP PRODUCERS (SELLERS) FOR {args.sellers} ---")
            print(stats.head(10).to_string())
        else:
            print(f"No seller data found for {args.sellers}")
            
    if args.history:
        print(f"Fetching history for: {args.history}")
        stats = analyzer.get_item_history(args.history)
        if stats is not None:
            print(stats[['Min_Price', 'Avg_Price', 'Stock_Count', 'Units_Sold_Since_Last']].to_string())
        else:
            print(f"No history found for {args.history}")

def handle_crafting(args):
    data_dir = get_data_dir()
    analyzer = CraftingAnalyzer(data_dir)
    
    print("Analyzing Crafting Profitability...")
    df = analyzer.analyze_profitability()
    
    if df is not None and not df.empty:
        print("\n--- TOP PROFITABLE RECIPES ---")
        cols = ['Produto', 'Spread', 'Margem_Perc', 'Mercado_Venda']
        print(df.head(args.top)[cols].to_string(index=False))
        
        out_file = os.path.join(data_dir, "analise_disparidade.csv")
        df.to_csv(out_file, index=False)
        print(f"\nFull report saved to {out_file}")
    else:
        print("No profitable recipes found.")

def handle_logistics(args):
    data_dir = get_data_dir()
    
    if args.route:
        # Expected format: start,end (comma separated) or just two args? 
        # Argparse 'nargs' can capture multiple. let's use nargs=2
        start, end = args.route
        logistics = PaxLogistics()
        print(f"Calculating route: {start} -> {end}")
        res = logistics.compare_routes(start, end)
        if res:
            print(f"\n--- Route Report: {start} -> {end} ---")
            print(f"Safe Route Cost: {res['safe_route']['cost']}")
            print(f"Safe Path: {' -> '.join(res['safe_route']['path'])}")
            print(f"PvP Route Cost: {res['pvp_route']['cost']}")
            print(f"PvP Path: {' -> '.join(res['pvp_route']['path'])}")
        else:
            print("Could not resolve locations.")
            
    if args.arbitrage:
        finder = ArbitrageFinder(data_dir)
        print("Scanning for Arbitrage Opportunities...")
        df = finder.find_opportunities()
        if not df.empty:
            print("\n--- TOP ARBITRAGE DEALS ---")
            # De-duplicate items
            df_dedup = df.drop_duplicates(subset=['Item']).head(10)
            cols = ['Item', 'Buy_Price', 'Avg_Sale_Price', 'Profit', 'Margin', 'Top_Zone']
            print(df_dedup[cols].to_string(index=False, float_format="%.1f"))
        else:
            print("No arbitrage opportunities found.")

def main():
    parser = argparse.ArgumentParser(description="Pax Dei Advisor - Unified Intelligence Tool")
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")
    
    # Market
    market_parser = subparsers.add_parser("market", help="Market Intelligence")
    market_parser.add_argument("--history", "-i", type=str, help="Get price history for an item")
    market_parser.add_argument("--liquidity", "-l", action="store_true", help="Check liquidity (churn)")
    market_parser.add_argument("--sellers", "-s", type=str, help="Analyze unique producers/sellers for an item")
    
    # Crafting
    crafting_parser = subparsers.add_parser("crafting", help="Crafting Analysis")
    crafting_parser.add_argument("--top", "-n", type=int, default=5, help="Number of top recipes to show")
    
    # Logistics
    logistics_parser = subparsers.add_parser("logistics", help="Logistics & Arbitrage")
    logistics_parser.add_argument("--route", nargs=2, metavar=('START', 'END'), help="Calculate route between two locations")
    logistics_parser.add_argument("--arbitrage", "-a", action="store_true", help="Find buy/sell arbitrage opportunities")
    
    args = parser.parse_args()
    
    if args.command == "market":
        handle_market(args)
    elif args.command == "crafting":
        handle_crafting(args)
    elif args.command == "logistics":
        handle_logistics(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
