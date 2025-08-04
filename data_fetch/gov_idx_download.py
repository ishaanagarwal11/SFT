# direct 

import os
import time
import logging
import requests
from itertools import cycle

# Configuration
YEARS = list(range(2018, 2026))
EMAILS = [
    "idx.downloader1@example.com", "idx.downloader2@example.com"
]
EMAIL_CYCLE = cycle(EMAILS)

# Constants
BASE_DIR = "./"
RETRY_LIMIT = 3
RETRY_BACKOFF = 1  # seconds
CALLS_PER_EMAIL = 15
API_CALL_COUNT = 0

# Logging setup
timestamp = time.strftime("%Y%m%d_%H%M%S")
log_filename = f"sec_idx_downloader_{timestamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

HEADERS = {
    "User-Agent": next(EMAIL_CYCLE)
}


def rotate_user_agent():
    """Rotate User-Agent header every fixed number of API calls."""
    global API_CALL_COUNT, HEADERS
    API_CALL_COUNT += 1
    if API_CALL_COUNT % CALLS_PER_EMAIL == 0:
        HEADERS["User-Agent"] = next(EMAIL_CYCLE)
        logging.info(f"Rotated User-Agent to: {HEADERS['User-Agent']}")


def http_get(url):
    """Perform GET request with retry and exponential backoff."""
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            rotate_user_agent()
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                return response.text
            logging.warning(
                f"HTTP {response.status_code} received for {url} (attempt {attempt})"
            )
        except Exception as exc:
            logging.warning(f"GET request failed for {url} (attempt {attempt}): {exc}")
        time.sleep(RETRY_BACKOFF)
    return None


def main():
    for year in YEARS:
        for quarter in range(1, 5):
            idx_url = (
                f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"
            )
            logging.info(f"Fetching index file: {idx_url}")
            idx_text = http_get(idx_url)

            if idx_text:
                local_idx_path = os.path.join(
                    BASE_DIR, "idx", str(year), f"QTR{quarter}", "company.idx"
                )
                os.makedirs(os.path.dirname(local_idx_path), exist_ok=True)
                with open(local_idx_path, "w", encoding="utf-8") as f:
                    f.write(idx_text)
                logging.info(f"Saved index file: {local_idx_path}")
            else:
                logging.warning(f"Index file could not be downloaded: {idx_url}")


if __name__ == "__main__":
    main()
