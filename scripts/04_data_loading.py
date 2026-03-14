import logging
import sys
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
today = datetime.now().strftime('%Y-%m-%d')

# DIRECTORIES
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRANSFORMED_DIR = os.path.join(BASE_DIR, '..', 'data', '03_transformed', today)

# SIMULATION FLAG
SIMULATION = True

# CONNECTION STRINGS
AZURE_SQL_CONN = os.getenv('AZURE_SQL_CONN')
DELTA_LAKE_CONN = os.getenv('DELTA_LAKE_CONN')

# LOADING FUNCTION AZURE SQL
def load_table_azure(transformed_dir, filename, tablename):
    try:
        df = pd.read_csv(os.path.join(transformed_dir, filename), low_memory=False)
        logger.info(f'Loaded {filename}: {len(df)} rows & {len(df.columns)} columns')

        if SIMULATION:
            logger.info(f'SIMULATED - Would load {len(df)} rows to Azure SQL table: {tablename}')

        # In production:
        # IMPORT - from sqlalchemy import create_engine
        # ENGINE - engine = create_engine(AZURE_SQL_CONN)
        # FUNCTION - df.to_sql(tablename, engine, if_exists='replace', index=False)

        return True
    except Exception as e:
        logger.error(f'Failed to load {filename}: {e}')
        return False

# LOADING FUNCTION DATABRICKS
def load_table_databricks(transformed_dir, filename, tablename):

    try:
        df = pd.read_csv(os.path.join(transformed_dir, filename), low_memory=False)
        logger.info(f'Loaded {filename}: {len(df)} rows & {len(df.columns)} columns')

        if SIMULATION:
            logger.info(f'SIMULATED - Would load {len(df)} rows to Delta Lake table: {tablename}')

        # In production:
        # IMPORT - from pyspark.sql import SparkSession
        # DATAFRAME - spark_df = spark.createDataFrame(df)
        # FUNCTION - spark_df.write.format("delta").mode("overwrite").saveAsTable(f"automotive.{tablename}")

        return True
    except Exception as e:
        logger.error(f'Failed to load {filename}: {e}')
        return False


def main():
    results_azure = {}
    results_azure['FUEL_TYPE_GROWTH'] = load_table_azure(TRANSFORMED_DIR, 'afdc_fuel_type_growth.csv', 'fuel_type_growth')
    results_azure['VEHICLE_TYPE_SHIFT']= load_table_azure(TRANSFORMED_DIR, 'epa_vehicle_type_shift_year.csv', 'vehicle_type_shift_year')
    results_azure['COMPLAINTS_CYLINDER'] = load_table_azure(TRANSFORMED_DIR, 'nhtsa_complaints_cylinders.csv', 'complaints_cylinders')
    results_azure['COMPLAINTS_FUEL'] = load_table_azure(TRANSFORMED_DIR, 'nhtsa_complaints_fuel.csv', 'complaints_fuel')

    results_databricks = {}
    results_databricks['FUEL_TYPE_GROWTH'] = load_table_databricks(TRANSFORMED_DIR, 'afdc_fuel_type_growth.csv', 'fuel_type_growth')
    results_databricks['VEHICLE_TYPE_SHIFT']= load_table_databricks(TRANSFORMED_DIR, 'epa_vehicle_type_shift_year.csv', 'vehicle_type_shift_year')
    results_databricks['COMPLAINTS_CYLINDER'] = load_table_databricks(TRANSFORMED_DIR, 'nhtsa_complaints_cylinders.csv', 'complaints_cylinders')
    results_databricks['COMPLAINTS_FUEL'] = load_table_databricks(TRANSFORMED_DIR, 'nhtsa_complaints_fuel.csv', 'complaints_fuel')

    logger.info('Data loading Complete for Azure')
    for source, success in results_azure.items():
        status = 'OK' if success else 'FAILED'
        logger.info(f'{source}:{status}')

    logger.info('Data loading Complete for Databricks')
    for source, success in results_databricks.items():
        status = 'OK' if success else 'FAILED'
        logger.info(f'{source}:{status}')
    

if __name__ == '__main__':
    main()