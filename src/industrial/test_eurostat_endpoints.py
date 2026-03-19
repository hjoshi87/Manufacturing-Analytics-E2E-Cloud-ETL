# simple_eurostat_test.py
"""
Simple test to find working Eurostat query
"""
import requests
import pandas as pd

print("Testing simple Eurostat queries...\n")

# Test 1: Most basic query - just the dataset
url1 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sts_inpr_m"

params1 = {
    'format': 'JSON',
    'geo': 'DE',
    'unit': 'I15',
    'nace_r2': 'B-D',
    's_adj': 'NSA'
}

print(f"Test 1: {url1}")
print(f"Params: {params1}\n")

try:
    response = requests.get(url1, params=params1, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Response size: {len(response.text)} bytes")
    
    if response.status_code == 200:
        data = response.json()
        print("[PASS] SUCCESS!")
        print(f"Data keys: {list(data.keys())}")
        
        if 'dataSets' in data:
            print(f"Number of dataSets: {len(data['dataSets'])}")
            if data['dataSets']:
                print(f"Series keys: {list(data['dataSets'][0].get('series', {}).keys())[:5]}")
    else:
        print(f"[FAIL] Error: {response.text[:300]}")
        
except Exception as e:
    print(f"[FAIL] Exception: {e}")

print("\n" + "="*70 + "\n")

# Test 2: Try with FREQ dimension
url2 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sts_inpr_m/M"

params2 = {
    'format': 'JSON',
    'startPeriod': '2019-01',
    'endPeriod': '2021-08',
    'geo': 'DE'
}

print(f"Test 2: {url2}")
print(f"Params: {params2}\n")

try:
    response = requests.get(url2, params=params2, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Response size: {len(response.text)} bytes")
    
    if response.status_code == 200:
        data = response.json()
        print("[PASS] SUCCESS!")
        print(f"Data keys: {list(data.keys())}")
    else:
        print(f"[FAIL] Error: {response.text[:300]}")
        
except Exception as e:
    print(f"[FAIL] Exception: {e}")