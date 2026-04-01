"""Configuration for the BS.TO series scraper."""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Credentials (store in .env file)
USERNAME = os.getenv("BS_USERNAME", "")
PASSWORD = os.getenv("BS_PASSWORD", "")

# Data storage
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# Logs directory
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOGS_DIR, "bs_to_backup.log")

# Scraping configuration
# Number of parallel httpx sessions (1 = sequential, 12+ = fast)
NUM_WORKERS = 12
