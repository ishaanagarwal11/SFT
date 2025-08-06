import os
import json
import logging
import requests
import time
from itertools import cycle
from config import CIK_MAP, EMAILS, EMAILS_TO_USE, CALLS_PER_EMAIL, SELECTED_TICKERS, SELECTED_FORMS, SELECTED_YEARS, SLEEP_TIME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler("generate_links.log"),
        logging.StreamHandler()
    ]
)

# Constants from config
CIK_SET = {CIK_MAP[ticker] for ticker in SELECTED_TICKERS}

SEC_ARCHIVES = "https://www.sec.gov/Archives"
IDX_DIR = "./data/idx"
OUTPUT_DIR = "./data/links"
EMAIL_CYCLE = cycle(EMAILS[:EMAILS_TO_USE])
EMAIL_ROTATE_EVERY = CALLS_PER_EMAIL


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

def extract_primary_filing_filename(txt_url, form, user_agent):
    headers = {"User-Agent": user_agent}
    try:
        resp = requests.get(txt_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        docs = resp.text.split("<DOCUMENT>")
        for doc in docs:
            if f"<TYPE>{form.upper()}" in doc.upper():
                for line in doc.splitlines():
                    if line.startswith("<FILENAME>"):
                        return line.replace("<FILENAME>", "").strip()
    except Exception as e:
        logging.warning(f"Failed to fetch or parse {txt_url}: {e}")
    return None


def generate_links(progress_bar=None):
    filings = find_filings_in_idx()
    logging.info(f"Found {len(filings)} matching filings.")
    all_links = {}
    email = next(EMAIL_CYCLE)
    total_steps = len(filings)
    
    if progress_bar:
        progress_bar.progress(0)

    for i, entry in enumerate(filings):
        if i % EMAIL_ROTATE_EVERY == 0:
            email = next(EMAIL_CYCLE)
            logging.info(f"Rotating User-Agent to: {email}")

        cik = entry["cik"]
        form = entry["form"]
        date = entry["date"]
        year = date[:4]

        filename = entry["filename"]
        accession = os.path.splitext(os.path.basename(filename))[0]
        accession_folder = accession.replace("-", "")
        cik_folder = filename.split("/")[2]
        base_url = f"{SEC_ARCHIVES}/edgar/data/{cik_folder}/{accession_folder}"
        txt_url = f"{base_url}/{accession}.txt"

        primary_filename = extract_primary_filing_filename(txt_url, form, email)
        if not primary_filename:
            continue

        file_url = f"{base_url}/{primary_filename}"
        ticker = next((t for t, c in CIK_MAP.items() if c == cik), cik)

        download_key = f"{form}_{year}_{accession}"

        all_links.setdefault(ticker, {}).setdefault(form, {}).setdefault(year, {})
        all_links[ticker][form][year][download_key] = file_url

        time.sleep(SLEEP_TIME)

        if progress_bar:
            progress_bar.progress((i + 1) / total_steps)

    # Debugging: Show the final directory where files should be saved
    logging.info(f"Current working directory: {os.getcwd()}")
    logging.info(f"Saving links toooo: {OUTPUT_DIR}")
    logging.info(f"Total unique tickers found: {len(all_links)}")

    for ticker, forms in all_links.items():
        save_dir = os.path.join(OUTPUT_DIR, ticker)
        
        # Check if directory is being created
        logging.info(f"Creating directory for ticker {ticker}: {save_dir}")
        os.makedirs(save_dir, exist_ok=True)

        save_path = os.path.join(save_dir, "links.json")
        
        # Check the file path before saving
        logging.info(f"Saving links to: {save_path}")
        try:
            with open(save_path, "w") as f:
                json.dump({"ticker": ticker, "links": forms}, f, indent=2)
            logging.info(f"Wrote: {save_path}")
        except Exception as e:
            logging.error(f"Failed to save {save_path}: {e}")

    logging.info("Link generation completed.")


# Example usage
if __name__ == "__main__":
    generate_links()
