import json
import os

def main():
    # Relative Paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    temp_dir = os.path.join(base_dir, "temp")
    data_dir = os.path.join(base_dir, "data")
    
    source_path = os.path.join(temp_dir, "pax_tools_data.json")
    output_path = os.path.join(data_dir, "catalogo_manufatura.json")
    
    if not os.path.exists(source_path):
        print(f"Source file missing: {source_path}")
        return

    # Check timestamps to avoid unnecessary processing
    if os.path.exists(output_path):
        src_mtime = os.path.getmtime(source_path)
        dst_mtime = os.path.getmtime(output_path)
        
        # If source is older than destination, we are up to date
        if src_mtime < dst_mtime:
            print(f"Catalog is up to date (Source: {os.path.basename(source_path)}). Skipping build.")
            print("Run with --force to overwrite if needed (not implemented yet, just delete the json).")
            return

        
    print("Loading raw data...")
    with open(source_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    items_map = data.get('items', {})
    recipes_raw = data.get('recipes', {})
    
    print(f"Loaded {len(items_map)} items and {len(recipes_raw)} recipes.")
    
    # 1. Convert Items List to Dict
    if isinstance(items_map, list):
        print("Converting Items list to lookup dict...")
        new_map = {}
        for item in items_map:
            iid = item.get('id')
            if iid:
                new_map[str(iid)] = item
        items_map = new_map
        
    def resolve_name(iid):
        key = str(iid)
        if key in items_map:
            return items_map[key].get('name', f"Unknown_{iid}")
        return f"Item_{iid}"

    catalog = {}
    count = 0
    
    # 2. Iterate Recipes
    if isinstance(recipes_raw, list):
        for r in recipes_raw:
            target_name = r.get('name', 'Unknown Recipe')
            
            # Inputs
            ing_list = r.get('ingredients', [])
            ingredients = []
            
            for ing in ing_list:
                iid = ing.get('item_id')
                qty = ing.get('qtt', 1)
                friendly_name = resolve_name(iid)
                
                ingredients.append({
                    "insumo": friendly_name,
                    "qtd": qty
                })
                
            if ingredients:
                catalog[target_name] = ingredients
                count += 1
    else:
        print("Recipes is not a list?")
        
    # 3. Save
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)
        
    print(f"Success! Converted {count} recipes to {output_path}")

if __name__ == "__main__":
    main()
