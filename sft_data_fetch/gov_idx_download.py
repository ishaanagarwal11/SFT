import os
import time
import logging
import requests
from itertools import cycle
from config import EMAILS, EMAILS_TO_USE, CALLS_PER_EMAIL, SELECTED_YEARS, RETRY_LIMIT, SLEEP_TIME

timestamp = time.strftime("%Y%m%d_%H%M%S")
log_filename = "sec_idx_downloader.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

EMAILS_TO_CYCLE = EMAILS[:EMAILS_TO_USE]
EMAIL_CYCLE = cycle(EMAILS_TO_CYCLE)

API_CALL_COUNT = 0


def rotate_user_agent():
    global API_CALL_COUNT
    API_CALL_COUNT += 1
    if API_CALL_COUNT % CALLS_PER_EMAIL == 0:
        HEADERS["User-Agent"] = next(EMAIL_CYCLE)
        logging.info(f"Rotated User-Agent to: {HEADERS['User-Agent']}")

def http_get(url):
    """Perform GET request with retry and exponential backoff."""
    # Define the HEADERS here
    HEADERS = {
        "User-Agent": next(EMAIL_CYCLE)
    }
    
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
        time.sleep(SLEEP_TIME)
    return None


def download_idx_files():
    for year in SELECTED_YEARS:
        for quarter in range(1, 5):
            idx_url = (
                f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/company.idx"
            )
            logging.info(f"Fetching index file: {idx_url}")
            idx_text = http_get(idx_url)

            if idx_text:
                local_idx_path = os.path.join(
                    "./data/idx", str(year), f"QTR{quarter}", "company.idx"
                )
                os.makedirs(os.path.dirname(local_idx_path), exist_ok=True)
                with open(local_idx_path, "w", encoding="utf-8") as f:
                    f.write(idx_text)
                logging.info(f"Saved index file: {local_idx_path}")
            else:
                logging.warning(f"Index file could not be downloaded: {idx_url}")

            time.sleep(SLEEP_TIME)
    
    logging.info("All index files downloaded.")
