import json
import os

data_dir = r"d:\PaxDei_Tool\data"
items_path = os.path.join(data_dir, "items.json")

with open(items_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

matches = []
search_terms = ["bronze", "locket", "sigil"]

for item_id, details in data.items():
    name = details.get('name', {}).get('En', '')
    if any(term in name.lower() for term in search_terms):
        matches.append(name)

print("Found matches:")
for m in sorted(matches):
    print(m)
