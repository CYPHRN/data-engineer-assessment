import os
import sys
import logging
import pandas as pd
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
today = datetime.now().strftime('%Y-%m-%d')

# DIRECTORIES
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, '..', 'data', '02_processed', today)
TRANSFORMED_DIR = os.path.join(BASE_DIR, '..', 'data', '03_transformed', today)

# TRANSFORMATION FUNCTIONS
def complaints_by_cylinders(processed_dir, transformed_dir):
    try:
        logger.info('Processing NHTSA Complaints')
        df = pd.read_csv(os.path.join(processed_dir, 'nhtsa_processed.csv'), low_memory=False)
        logger.info(f'NHTSA Processed Dataset loaded {len(df)} rows')

        df = df[df['NUM_CYLS'] <= 16]
        df = df.dropna(subset=['NUM_CYLS'])
        
        result = df.groupby('NUM_CYLS').size().reset_index(name='complaint_count')
        result = result.sort_values('complaint_count', ascending=False)

        result.to_csv(os.path.join(transformed_dir, 'nhtsa_complaints_cylinders.csv'), index=False)
        logger.info('NHTSA Complaints processed & saved')
        return True
    except Exception as e:
        logger.error(f'NHTSA Complaints processing failed: {e}')
        return False

def vehicle_type_shift(processed_dir, transformed_dir):
    try:
        logger.info('Processing EPA Vehicle Type Shift')
        df = pd.read_csv(os.path.join(processed_dir, 'epa_processed.csv'), low_memory=False)
        logger.info(f'EPA Processed Dataset loaded {len(df)} rows')

        df = df.dropna(subset=['year', 'atvType'])
        result = df.groupby(['year', 'atvType']).size().reset_index(name='vehicle_count')
        result = result.sort_values(['year', 'vehicle_count'], ascending=[True, False])

        result.to_csv(os.path.join(transformed_dir, 'epa_vehicle_type_shift_year.csv'), index=False)
        logger.info('EPA Vehicle Type shift per year processed & saved')
        return True
    except Exception as e:
        logger.error(f'EPA Vehicle Type shift per year processing failed: {e}')
        return False

def fuel_station_growth(processed_dir, transformed_dir):
    try:
        logger.info('Processing AFDC Fuel Station growth')
        df = pd.read_csv(os.path.join(processed_dir, 'afdc_processed.csv'), low_memory=False)
        logger.info(f'AFDC Processed Dataset loaded {len(df)} rows')

        df['open_date'] = pd.to_datetime(df['open_date'], errors='coerce')
        df['open_year'] = df['open_date'].dt.year
        df = df.dropna(subset=['open_year', 'fuel_type_code'])
        result = df.groupby(['open_year', 'fuel_type_code']).size().reset_index(name='fuel_station_growth')
        result = result.sort_values(['open_year', 'fuel_station_growth'], ascending=[True, False])

        result.to_csv(os.path.join(transformed_dir, 'afdc_fuel_type_growth.csv'), index=False)
        logger.info('AFDC Fuel Type Growth processed & saved')
        return True
    except Exception as e:
        logger.error(f'AFDC Fuel Station Growth processing failed: {e}')
        return False

def complaints_based_on_fuel(processed_dir, transformed_dir):
    try:
        logger.info('Processing NHTSA Complaints / Fuel Type')
        df = pd.read_csv(os.path.join(processed_dir, 'nhtsa_processed.csv'), low_memory=False)
        logger.info(f'NHTSA Processed Dataset loaded {len(df)} rows')

        df = df.dropna(subset=['FUEL_TYPE'])
        result = df.groupby(['FUEL_TYPE']).size().reset_index(name='fuel_type_complaints')
        result = result.sort_values('fuel_type_complaints', ascending=False)

        result.to_csv(os.path.join(transformed_dir, 'nhtsa_complaints_fuel.csv'), index=False)
        logger.info('NHTSA Fuel Complaints / Fuel Type processed & saved')
        return True
    except Exception as e:
        logger.error(f'NHTSA Complaints / Fuel Type processing failed: {e}')
        return False

def main():
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    results = {}
    results['COMPLAINTS'] = complaints_by_cylinders(PROCESSED_DIR, TRANSFORMED_DIR)
    results['VEHICLE_TYPE'] = vehicle_type_shift(PROCESSED_DIR, TRANSFORMED_DIR)
    results['FUEL_TYPE'] = fuel_station_growth(PROCESSED_DIR, TRANSFORMED_DIR)
    results['COMPLAINTS_FUEL'] = complaints_based_on_fuel(PROCESSED_DIR, TRANSFORMED_DIR)

    logger.info('Data Transformation Complete')
    for source, success in results.items():
        status = 'OK' if success else 'FAILED'
        logger.info(f'{source}:{status}')

if __name__ == '__main__':
    main()