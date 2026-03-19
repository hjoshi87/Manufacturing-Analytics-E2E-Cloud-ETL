"""
Eurostat Industrial Production Index - Correct XML Parsing
"""
import pandas as pd
import requests
import logging
import xml.etree.ElementTree as ET
import time

logger = logging.getLogger(__name__)

class EurostatClient:
    def __init__(self):
        """Initialize Eurostat client"""
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
        
    def get_industrial_production_index_real(self, country="DE", 
                                             start_year=2019, 
                                             end_year=2021):
        """
        Fetch industrial production index from Eurostat API
        Fetches year-by-year with correct XML parsing
        """
        
        print(f"\n[Eurostat] Fetching industrial production index for {country} ({start_year}-{end_year})...")
        
        all_records = []
        
        try:
            # Fetch year by year
            for year in range(start_year, end_year + 1):
                print(f"  Fetching {year}...", end=" ")
                
                url = f"{self.base_url}/sts_inpr_m"
                
                params = {
                    'geo': country,
                    'unit': 'I15',
                    'nace_r2': 'B-D',
                    's_adj': 'NSA',
                    'freq': 'M',
                    'startPeriod': f'{year}-01',
                    'endPeriod': f'{year}-12'
                }
                
                response = requests.get(url, params=params, timeout=60)
                
                if response.status_code != 200:
                    print(f"✗ Error {response.status_code}")
                    time.sleep(1)
                    continue
                
                # Parse XML
                try:
                    root = ET.fromstring(response.content)
                    
                    # Remove namespace
                    for elem in root.iter():
                        if '}' in elem.tag:
                            elem.tag = elem.tag.split('}', 1)[1]
                    
                    # Find all Series elements
                    obs_count = 0
                    for series in root.findall('.//Series'):
                        # Check if this series is for our country
                        series_key = series.find('SeriesKey')
                        is_target_country = False
                        
                        if series_key is not None:
                            for value in series_key.findall('Value'):
                                if value.get('id') == 'geo' and value.get('value') == country:
                                    is_target_country = True
                                    break
                        
                        if not is_target_country:
                            continue
                        
                        # Extract observations from this series
                        for obs in series.findall('Obs'):
                            try:
                                # Find ObsDimension (contains time period)
                                obs_dimension = obs.find('ObsDimension')
                                time_period = obs_dimension.get('value') if obs_dimension is not None else None
                                
                                # Find ObsValue (contains the actual value)
                                obs_value = obs.find('ObsValue')
                                value = obs_value.get('value') if obs_value is not None else None
                                
                                if time_period and value:
                                    date = pd.to_datetime(time_period + '-01')
                                    
                                    all_records.append({
                                        'date': date,
                                        'country_code': country,
                                        'industrial_production_index': float(value),
                                        'source': 'Eurostat',
                                        'unit': 'Index (2015=100)'
                                    })
                                    obs_count += 1
                            except (ValueError, AttributeError):
                                continue
                    
                    print(f"[PASS] {obs_count} records")
                    
                except ET.ParseError as e:
                    print(f"✗ Parse error: {e}")
                
                # Rate limit
                time.sleep(0.5)
            
            if all_records:
                df = pd.DataFrame(all_records).sort_values('date').reset_index(drop=True)
                
                print(f"\n[PASS] Retrieved {len(df)} total monthly records")
                print(f"  Date range: {df['date'].min().strftime('%Y-%m')} to {df['date'].max().strftime('%Y-%m')}")
                print(f"  Value range: {df['industrial_production_index'].min():.2f} to {df['industrial_production_index'].max():.2f}")
                
                return df
            else:
                print("\n✗ No records found")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


if __name__ == '__main__':
    client = EurostatClient()
    df = client.get_industrial_production_index_real("DE", start_year=2019, end_year=2021)
    
    if not df.empty:
        print("\n" + "="*60)
        print("Industrial Production Index (Germany)")
        print("="*60)
        print(df.head(12))
        print(f"\nStatistics:")
        print(df['industrial_production_index'].describe())
    else:
        print("Failed to retrieve data")