import os
import logging
import requests
import time
from tqdm import tqdm
from bs4 import BeautifulSoup
from itertools import cycle
from config import CIK_MAP, EMAILS, EMAILS_TO_USE, CALLS_PER_EMAIL, SELECTED_TICKERS, SELECTED_FORMS, SELECTED_YEARS, RETRY_LIMIT, SLEEP_TIME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler("download_filings.log"),
        logging.StreamHandler()
    ]
)

CIK_SET = {CIK_MAP[ticker] for ticker in SELECTED_TICKERS}
SEC_PREFIX = "https://www.sec.gov/Archives/"
IDX_DIR = "./data/idx"
OUTPUT_DIR = "./data/filings"
EMAIL_CYCLE = cycle(EMAILS[:EMAILS_TO_USE]) 

def parse_idx_line_fixed(line):
    """Parse fixed-width fields in .idx line"""
    try:
        company = line[0:62].strip()
        form = line[62:74].strip().upper()
        cik = line[74:86].strip().zfill(10)
        date = line[86:98].strip()
        filename = line[98:].strip()
        return {
            "company": company,
            "form": form,
            "cik": cik,
            "date": date,
            "filename": filename
        }
    except Exception as e:
        logging.warning(f"Failed to parse line: {line[:100]} → {e}")
        return None


def is_valid_entry(entry):
    """Check if the entry matches the required filters."""
    return (
        entry
        and entry["cik"] in CIK_SET
        and entry["form"] in SELECTED_FORMS
        and int(entry["date"][:4]) in map(int, SELECTED_YEARS)
    )

        

def process_idx_file(idx_path, matches):
    """Process IDX file to extract matching entries."""
    logging.info(f"Scanning index file: {idx_path}")
    with open(idx_path, encoding='latin-1') as f:
        for line in f:
            entry = parse_idx_line_fixed(line)
            if is_valid_entry(entry):
                matches.append(entry)


def find_filings_in_idx():
    """Find all matching filings in IDX files."""
    matches = []
    for root, _, files in os.walk(IDX_DIR):
        for fname in files:
            if fname.endswith(".idx"):
                idx_path = os.path.join(root, fname)
                process_idx_file(idx_path, matches)
    return matches


def extract_primary_filing_filename(txt_text, form):
    """Extract primary filing document from the .txt content"""
    docs = txt_text.split("<DOCUMENT>")
    for doc in docs:
        doc_upper = doc.upper()
        if f"<TYPE>{form.upper()}" in doc_upper:
            for line in doc.splitlines():
                if line.startswith("<FILENAME>"):
                    return line.replace("<FILENAME>", "").strip()
    return None


def download_filing(entry, user_agent):
    """Download filing with retries and backoff"""
    cik = entry["cik"]
    form = entry["form"]
    year = entry["date"][:4]
    filename = entry["filename"]
    ticker = next((t for t, c in CIK_MAP.items() if c == cik), cik)
    accession = os.path.splitext(os.path.basename(filename))[0]
    accession_folder = accession.replace("-", "")
    cik_folder = filename.split("/")[2]
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik_folder}/{accession_folder}"
    txt_url = f"{base_url}/{accession}.txt"
    headers = {"User-Agent": user_agent}

    for attempt in range(RETRY_LIMIT):
        try:
            response = requests.get(txt_url, headers=headers, timeout=15)
            response.raise_for_status()
            primary_filename = extract_primary_filing_filename(response.text, form)
            if not primary_filename:
                logging.warning(f"No matching <FILENAME> found in {txt_url}")
                return

            file_url = f"{base_url}/{primary_filename}"
            ext = os.path.splitext(primary_filename)[1].lstrip(".")
            save_dir = os.path.join(OUTPUT_DIR, ticker, form, year)
            os.makedirs(save_dir, exist_ok=True)
            save_path = os.path.join(save_dir, f"{form}_{year}_{accession}.{ext}")

            if os.path.exists(save_path):
                logging.info(f"Already exists: {save_path}")
                return

            file_resp = requests.get(file_url, headers=headers, timeout=20)
            if file_resp.status_code == 200 and len(file_resp.content) > 100:
                with open(save_path, "wb") as f:
                    f.write(file_resp.content)
                logging.info(f"Downloaded: {save_path}")
                return
            else:
                raise requests.exceptions.RequestException(
                    f"File fetch failed or empty content: {file_url}"
                )
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}/{RETRY_LIMIT} failed for {txt_url} → {e}")
            time.sleep(1 + attempt)

    logging.error(f"Failed after {RETRY_LIMIT} retries: {txt_url}")


def download_filings():
    filings = find_filings_in_idx() 
    logging.info(f"Found {len(filings)} matching filings.")
    email = next(EMAIL_CYCLE)

    for i, entry in enumerate(filings):
        if i % CALLS_PER_EMAIL == 0:
            email = next(EMAIL_CYCLE)
            logging.info(f"Switching User-Agent email to: {email}")
        
        download_filing(entry, user_agent=email)  

    logging.info("All filings downloaded.")
