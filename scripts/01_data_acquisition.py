import requests
import sys
import os
import json
import logging
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.helpers import download_and_extract
load_dotenv()
today = datetime.now().strftime('%Y-%m-%d')

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# API KEYS
afdc_api_key = os.getenv("NREL_API_KEY")

# SOURCES
nhtsa_url = "https://static.nhtsa.gov/odi/ffdd/cmpl/FLAT_CMPL.zip"
afdc_url = "https://developer.nrel.gov/api/alt-fuel-stations/v1.json"
epa_url = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"

# DIRECTORY
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "..", "data", "01_raw", today)

# DOWNLOAD / API FUNCTIONS
def nhtsa_download(dir): 
    if os.path.exists(os.path.join(dir, 'FLAT_CMPL.txt')):
        logger.info('NHTSA data already exists, skipping downloading')
        return True
    
    success = download_and_extract(nhtsa_url, dir, "nhtsa.zip")
    if success:
        logger.info('NHTSA dataset ready')
        return True
    else:
        logger.error('NHTSA download failed')
        return False  

def afdc_download(dir):
    if os.path.exists(os.path.join(dir, 'afdc_stations.json')):
        logger.info('AFDC data already exists, skipping downloading')
        return True
    
    params = {"api_key": afdc_api_key, "fuel_type": "all", "limit": "all"}
    try:
        os.makedirs(dir, exist_ok=True)
        filepath = os.path.join(dir, "afdc_stations.json")
        response = requests.get(afdc_url, params=params)
        response.raise_for_status()
        data = response.json()
        with open(filepath, 'w') as f:
            json.dump(data, f)
        logger.info('AFDC dataset ready')
        return True
    except Exception as e:
        logger.error(f'AFDC dataset failed: {e}')
        return False
    
def epa_download(dir):
    if os.path.exists(os.path.join(dir, 'vehicles.csv')):
        logger.info('EPA data already exists, skipping downloading')
        return True
    
    success = download_and_extract(epa_url, dir, "epa.zip")
    if success:
        logger.info('EPA dataset ready')
        return True
    else:
        logger.error('EPA download failed')
        return False

# MAIN FUNCTION
def main():
    results = {}
    results["NHTSA"] = nhtsa_download(RAW_DIR)
    results["AFDC"] = afdc_download(RAW_DIR)
    results["EPA"] = epa_download(RAW_DIR)
    
    logger.info("Data Acquistion Complete!")
    for source, success in results.items():
        status = "OK" if success else "FAILED"
        logger.info(f'{source}:{status}')
    
if __name__ == '__main__':
    main()