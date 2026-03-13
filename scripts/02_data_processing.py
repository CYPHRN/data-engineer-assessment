import os
import sys
import json
import logging
import pandas as pd
from datetime import datetime
import numpy as np


sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
today = datetime.now().strftime('%Y-%m-%d')

# DIRECTORIES
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, '..', 'data', "01_raw", today)
PROCESSED_DIR = os.path.join(BASE_DIR, "..", 'data', "02_processed", today)

# COLUMNS
epa_cols = [
    'id', 'make', 'model', 'year', 'cylinders', 'displ',
    'drive', 'fuelType', 'atvType', 'VClass',
    'comb08', 'co2TailpipeGpm'
]

nhtsa_cols = [
    'CMPLID', 'ODINO', 'MFR_NAME', 'MAKETXT', 'MODELTXT', 'YEARTXT',
    'CRASH', 'FAILDATE', 'FIRE', 'INJURED', 'DEATHS', 'COMPDESC',
    'CITY', 'STATE', 'VIN', 'DATEA', 'LDATE', 'MILES', 'OCCURENCES',
    'CDESCR', 'CMPL_TYPE', 'POLICE_RPT_YN', 'PURCH_DT', 'ORIG_OWNER_YN',
    'ANTI_BRAKES_YN', 'CRUISE_CONT_YN', 'NUM_CYLS', 'DRIVE_TRAIN',
    'FUEL_SYS', 'FUEL_TYPE', 'TRANS_TYPE', 'VEH_SPEED', 'DOT',
    'TIRE_SIZE', 'LOC_OF_TIRE', 'TIRE_FAIL_TYPE', 'ORIG_EQUIP_YN',
    'MANUF_DT', 'SEAT_TYPE', 'RESTRAINT_TYPE', 'DEALER_NAME',
    'DEALER_TEL', 'DEALER_CITY', 'DEALER_STATE', 'DEALER_ZIP',
    'PROD_TYPE', 'REPAIRED_YN', 'MEDICAL_ATTN', 'VEHICLES_TOWED_YN'
]

nhtsa_keep = [
    'CMPLID', 'MFR_NAME', 'MAKETXT', 'MODELTXT', 'YEARTXT',
    'CRASH', 'FIRE', 'INJURED', 'DEATHS',
    'COMPDESC', 'LDATE',
    'FUEL_TYPE', 'NUM_CYLS'
]

afdc_cols = [
    'id', 'fuel_type_code', 'station_name', 'city', 'state',
    'open_date', 'status_code', 'access_code'
]

# PROCESSING FUNCTIONS
def epa_process(raw_dir, processed_dir):
    try:
        logger.info('Processing EPA Dataset')
        df = pd.read_csv(os.path.join(raw_dir, 'vehicles.csv'), low_memory=False)
        logger.info(f'EPA loaded: {len(df)} rows')
        
        df = df[epa_cols]
        df = df.dropna(subset=['make', 'model', 'year', 'fuelType'])
        df = df.drop_duplicates()
        df['atvType'] = df['atvType'].fillna('Standard')
        logger.info(f'EPA Dataset after clean: {len(df)} rows')
        
        df.to_csv(os.path.join(processed_dir, 'epa_processed.csv'), index=False)
        logger.info('EPA Dataset processed & saved')
        return True
    except Exception as e:
        logger.error(f'EPA Dataset processing failed: {e}')
        return False
        

def nhtsa_process(raw_dir, processed_dir):
    try:
        logger.info('Processing NHTSA Dataset')
        df = pd.read_csv(os.path.join(raw_dir, 'FLAT_CMPL.txt'), sep='\t', header=None, names=nhtsa_cols, on_bad_lines='skip', encoding='latin-1', low_memory=False)
        logger.info(f'NHTSA Dataset loaded: {len(df)} rows')
        
        # Filter Vehicles only, keep only important columns & clean bad values
        df = df[df['PROD_TYPE'] == 'V']
        df = df[nhtsa_keep]
        df = df[df['YEARTXT'] != 9999]
        df['FUEL_TYPE'] = df['FUEL_TYPE'].replace(' ', np.nan)
        df = df.dropna(subset=['MAKETXT', 'MODELTXT', 'YEARTXT'])
        df = df.drop_duplicates()
        
        logger.info(f'NHTSA Dataset after clean: {len(df)} rows')
        
        df.to_csv(os.path.join(processed_dir, 'nhtsa_processed.csv'), index=False)
        logger.info('NHTSA Dataset processed & saved')
        return True
    except Exception as e:
        logger.error(f'NHTSA Dataset processing failed: {e}')
        return False
    
    
def afdc_process(raw_dir, processed_dir):
    try:
        logger.info('Processing AFDC Dataset')
        with open(os.path.join(raw_dir, 'afdc_stations.json')) as f:
            raw = json.load(f)
        df = pd.DataFrame(raw['fuel_stations'])
        logger.info(f'AFDC Dataset loaded: {len(df)} rows')
        
        df = df[afdc_cols]
        df = df[df['status_code'] == 'E']
        df['open_date'] = pd.to_datetime(df['open_date'], errors='coerce' )
        df = df.dropna(subset=['fuel_type_code', 'state'])
        df = df.drop_duplicates()
        logger.info(f'AFDC Dataset after clean: {len(df)} rows')
        
        df.to_csv(os.path.join(processed_dir, 'afdc_processed.csv'), index=False)
        logger.info('AFDC Dataset processed & saved')
        return True    
    except Exception as e:
        logger.error(f'AFDC Dataset processing failed: {e}')
        return False

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    results = {}
    results['EPA'] = epa_process(RAW_DIR, PROCESSED_DIR)
    results['NHTSA'] = nhtsa_process(RAW_DIR, PROCESSED_DIR)
    results['AFDC'] = afdc_process(RAW_DIR, PROCESSED_DIR)
    
    logger.info('Data Processing Complete')
    for source, success in results.items():
        status = "OK" if success else "FAILED"
        logger.info(f'{source}: {status}')

if __name__ == '__main__':
    main()
