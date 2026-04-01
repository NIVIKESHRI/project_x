import requests
import json

url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
resp = requests.get(url)
data = resp.json()

# Print first 3 entries to see structure
for i, item in enumerate(data[:3]):
    print(f"Record {i+1}:")
    for k, v in item.items():
        print(f"  {k}: {v}")
    print()

# Count how many have exch_seg = 'nse_fo' and symbol starting with 'NIFTY'
nifty_count = 0
for item in data:
    if item.get('exch_seg') == 'nse_fo' and item.get('symbol', '').startswith('NIFTY'):
        nifty_count += 1
print(f"Nifty derivatives count: {nifty_count}")