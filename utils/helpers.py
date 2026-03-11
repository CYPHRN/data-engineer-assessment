import os
import zipfile
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def download_and_extract(url, directory, filename):
    try:
        os.makedirs(directory, exist_ok=True)
        zip_path = os.path.join(directory, filename)
        response = requests.get(url)
        response.raise_for_status()
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(directory)
        os.remove(zip_path)
        logger.info(f"Download and extracted {filename} to {directory}")
        return True
    except Exception as e:
        logger.error(f"Failed to download {filename}: {e}")
        return False