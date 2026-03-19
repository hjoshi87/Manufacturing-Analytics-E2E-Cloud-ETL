"""
Eurostat - Debug XML structure
"""
import requests
import xml.etree.ElementTree as ET

url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/sts_inpr_m"

params = {
    'geo': 'DE',
    'unit': 'I15',
    'nace_r2': 'B-D',
    's_adj': 'NSA',
    'freq': 'M',
    'startPeriod': '2019-01',
    'endPeriod': '2019-12'
}

print("Fetching data...")
response = requests.get(url, params=params, timeout=60)

print(f"Status: {response.status_code}")
print(f"Size: {len(response.text)} bytes")

if response.status_code == 200:
    # Save raw response
    with open('eurostat_response.xml', 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    print("✓ Saved to eurostat_response.xml")
    
    # Try to parse
    try:
        root = ET.fromstring(response.content)
        print(f"\nRoot tag: {root.tag}")
        print(f"Root attributes: {root.attrib}")
        
        # Print all elements
        print("\nAll elements in XML:")
        for elem in root.iter():
            if '}' in elem.tag:
                tag = elem.tag.split('}', 1)[1]
            else:
                tag = elem.tag
            print(f"  {tag}: {elem.attrib if elem.attrib else ''}")
        
        # Look for observations specifically
        print("\n\nSearching for observations...")
        obs_list = root.findall('.//Obs')
        print(f"Found {len(obs_list)} Obs elements")
        
        if obs_list:
            print(f"\nFirst observation: {obs_list[0].attrib}")
        
    except Exception as e:
        print(f"Parse error: {e}")
        print(f"\nFirst 500 chars of response:\n{response.text[:500]}")
else:
    print(f"Error: {response.text}")