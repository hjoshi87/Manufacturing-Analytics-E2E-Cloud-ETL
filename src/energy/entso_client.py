import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import xml.etree.ElementTree as ET
import io
from pathlib import Path
from dotenv import load_dotenv
import os
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# Configure logging to keep things quiet but informative
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class ENTSOEClient:
    def __init__(self, security_token=None):
        self.base_url = "https://web-api.tp.entsoe.eu/api"
        # Using your verified token
        self.security_token = security_token or os.getenv("ENTSO_API_KEY")
        self.session = requests.Session()

    def get_day_ahead_prices(self, country_code="DE", start_date=None, end_date=None):
        if not start_date:
            start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Map "DE" to "DE-LU" automatically because DE (10Y1001A1001A63L) often returns no data
        target_code = "DE-LU" if country_code == "DE" else country_code
        domain = self.country_to_domain(target_code)

        print(f"\n[ENTSO-E] Requesting {target_code} ({domain}) for {start_date} to {end_date}")

        params = {
            "securityToken": self.security_token,
            "documentType": "A44",
            "in_Domain": domain,
            "out_Domain": domain,
            "periodStart": f"{start_date.replace('-', '')}0000",
            "periodEnd": f"{end_date.replace('-', '')}2300"
        }

        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            if response.status_code != 200:
                print(f"API Error {response.status_code}: {response.text}")
                return self.mock_prices(start_date, end_date, country_code)

            df = self._parse_xml(response.content)
            
            if df.empty:
                print(f"No data found for {target_code}. Falling back to Mock.")
                return self.mock_prices(start_date, end_date, country_code)

            df['country_code'] = target_code
            df['source'] = 'ENTSO-E'
            return df

        except Exception as e:
            print(f"Error fetching data: {e}")
            return self.mock_prices(start_date, end_date, country_code)

    def _parse_xml(self, xml_content):
        """Namespace-agnostic parser for any ENTSO-E response"""
        try:
            it = ET.iterparse(io.BytesIO(xml_content))
            for _, el in it:
                if '}' in el.tag: el.tag = el.tag.split('}', 1)[1]
            root = it.root

            data = []
            for ts in root.findall('.//TimeSeries'):
                period = ts.find('Period')
                if period is None: continue
                
                res = period.find('resolution').text
                min_step = 15 if "15M" in res else 60
                
                # Convert start time to local time (Europe/Berlin) for easier reading
                start_time = pd.to_datetime(period.find('timeInterval/start').text).tz_convert('Europe/Berlin')
                
                for pt in period.findall('Point'):
                    pos = int(pt.find('position').text)
                    price = float(pt.find('price.amount').text)
                    ts_point = start_time + timedelta(minutes=(pos - 1) * min_step)
                    
                    data.append({
                        'timestamp': ts_point,
                        'price_eur_mwh': price
                    })
            
            return pd.DataFrame(data).sort_values('timestamp')
        except Exception as e:
            print(f"Parsing failed: {e}")
            return pd.DataFrame()
    def download_historical_hourly(self, start_date="2019-02-01", end_date="2021-08-31"):
        """Downloads data in monthly chunks and resamples to 1-hour intervals"""
        import time
        
        months = pd.date_range(start=start_date, end=end_date, freq='MS')
        final_frames = []
        
        for start_month in months:
            end_month = start_month + pd.offsets.MonthEnd(0)
            s_str, e_str = start_month.strftime("%Y-%m-%d"), end_month.strftime("%Y-%m-%d")
            
            print(f"Fetching {s_str}...")
            df = self.get_day_ahead_prices("DE", s_str, e_str)
            
            if not df.empty:
                # Set index to timestamp to allow resampling
                df = df.set_index('timestamp')
                # Resample to Hourly ('H') and take the mean of the 15-min prices
                hourly_df = df['price_eur_mwh'].resample('H').mean().reset_index()
                final_frames.append(hourly_df)
                time.sleep(0.5)  # Avoid hitting API rate limits
        
        if final_frames:
            full_df = pd.concat(final_frames, ignore_index=True)
            # Remove timezone offset
            full_df['timestamp'] = full_df['timestamp'].dt.tz_localize(None)
            return full_df
        
        return pd.DataFrame()

    @staticmethod
    def country_to_domain(code):
        return {
            'DE': '10Y1001A1001A63L', 
            'DE-LU': '10Y1001A1001A82H',
            'FR': '10YFR-RTE------C'
        }.get(code, code)

    def mock_prices(self, start_date, end_date, country_code):
        dr = pd.date_range(start=start_date, end=end_date, freq="H")
        return pd.DataFrame({'timestamp': dr, 'country_code': country_code, 'price_eur_mwh': 50.0, 'source': 'MOCK'})

if __name__ == '__main__':
    client = ENTSOEClient()
    print("Starting historical download (Feb 2019 - Aug 2021)...")
    
    # Update dates to match Bosch dataset
    historical_df = client.download_historical_hourly(
        start_date="2019-02-01",
        end_date="2021-08-31"
    )
    
    if not historical_df.empty:
        # Save to CSV
        historical_df.to_csv("hourly_prices_2019_2021.csv", index=False)
        print("\n--- DONE ---")
        print(f"Total Hourly Records: {len(historical_df)}")
        print(f"Date range: {historical_df['timestamp'].min()} to {historical_df['timestamp'].max()}")
        print(historical_df.head())
    else:
        print("Failed to retrieve data.")