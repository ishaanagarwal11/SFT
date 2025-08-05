import os
import json
import logging
import requests
import time
from tqdm import tqdm
from itertools import cycle

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler("generate_links.log"),
        logging.StreamHandler()
    ]
)

# Constants
CIK_MAP = {
    "WMT": "0000104169", "AMZN": "0001018724", "UNH": "0000731766", "AAPL": "0000320193",
    "CVS": "0000064803", "BRK.B": "0001067983", "GOOGL": "0001652044", "XOM": "0000034088",
    "MCK": "0000927653", "COR": "0001355839", "JPM": "0000019617", "COST": "0000909832",
    "CI": "0001739940", "MSFT": "0000789019", "CAH": "0000721371"
}
CIK_SET = set(CIK_MAP.values())
FORM_TYPES = {"10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"}
TARGET_YEARS = {"2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"}

SEC_ARCHIVES = "https://www.sec.gov/Archives"
IDX_DIR = "./idx"
OUTPUT_DIR = "./links"
EMAILS = ["link.generator1@gmail.com", "link.generator2@gmail.com", "link.generator3@gmail.com",
           "link.generator4@gmail.com", "link.generator5@gmail.com", "link.generator6@gmail.com"]
EMAIL_CYCLE = cycle(EMAILS)
EMAIL_ROTATE_EVERY = 18


def parse_idx_line_fixed(line):
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
    return (
        entry
        and entry["cik"] in CIK_SET
        and entry["form"] in FORM_TYPES
        and entry["date"][:4].isdigit()
    )


def find_matching_filings():
    matches = []
    for root, _, files in os.walk(IDX_DIR):
        for fname in files:
            if fname.endswith(".idx"):
                idx_path = os.path.join(root, fname)
                with open(idx_path, encoding='latin-1') as f:
                    for line in f:
                        entry = parse_idx_line_fixed(line)
                        if is_valid_entry(entry):
                            matches.append(entry)
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


def generate_links():
    filings = find_matching_filings()
    logging.info(f"Found {len(filings)} matching filings.")
    all_links = {}
    email = next(EMAIL_CYCLE)

    for i, entry in enumerate(tqdm(filings, desc="Generating links")):
        if i % EMAIL_ROTATE_EVERY == 0:
            email = next(EMAIL_CYCLE)
            logging.info(f"Rotating User-Agent to: {email}")

        cik = entry["cik"]
        form = entry["form"]
        date = entry["date"]
        year = date[:4]

        if not year.isdigit() or year not in TARGET_YEARS:
            continue

        filename = entry["filename"]
        accession = os.path.splitext(os.path.basename(filename))[0]  # e.g., 0001018724-24-000008
        accession_folder = accession.replace("-", "")
        cik_folder = filename.split("/")[2]
        base_url = f"{SEC_ARCHIVES}/edgar/data/{cik_folder}/{accession_folder}"
        txt_url = f"{base_url}/{accession}.txt"

        primary_filename = extract_primary_filing_filename(txt_url, form, email)
        if not primary_filename:
            continue

        file_url = f"{base_url}/{primary_filename}"
        ticker = next((t for t, c in CIK_MAP.items() if c == cik), cik)

        download_key = f"{form}_{year}_{accession}"  # No file extension

        all_links.setdefault(ticker, {}).setdefault(form, {}).setdefault(year, {})
        all_links[ticker][form][year][download_key] = file_url

        time.sleep(0.9)

    for ticker, forms in all_links.items():
        save_dir = os.path.join(OUTPUT_DIR, ticker)
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, "links.json")
        with open(save_path, "w") as f:
            json.dump({"ticker": ticker, "links": forms}, f, indent=2)
        logging.info(f"Wrote: {save_path}")

    logging.info("Link generation completed.")


if __name__ == "__main__":
    generate_links()
