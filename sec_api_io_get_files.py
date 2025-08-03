# data 
import os
import json
import requests
import pandas as pd
from sec_api import QueryApi
from time import sleep

# Configuration
API_KEY = "f363d9cac0c6143159e29b5990896dfad3e7ffe82e7f940c7db6c741a37cfa29"  # Replace with your SEC-API key
EMAIL = "youremaizl@example.com"  # Replace with your email
TICKERS = [
    "WMT", "AMZN", "UNH", "AAPL", "CVS", "BRK.B", "GOOGL", "XOM", "MCK",
    "COR", "JPM", "COST", "CI", "MSFT", "CAH"
]
FORM_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"]
YEARS = list(range(2018, 2026))  # or [2020, 2021, 2022, ...]
BASE_DIR = "./filings"

queryApi = QueryApi(api_key=API_KEY)

HEADERS = {
    "User-Agent": EMAIL,
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}

not_found_log = []

def normalize_url(url):
    """Fix ix?doc= URLs and prepend SEC base if missing."""
    if "ix?doc=" in url:
        raw = url.split("ix?doc=")[-1]
        return f"https://www.sec.gov{raw}" if raw.startswith("/") else f"https://www.sec.gov/{raw}"
    return url if url.startswith("http") else f"https://www.sec.gov{url}"

# Loop over all tickers
for ticker in TICKERS:
    print(f"\n========= Processing {ticker} =========")

    for form in FORM_TYPES:
        form_dir = os.path.join(BASE_DIR, ticker, form.replace(" ", "_"))
        os.makedirs(form_dir, exist_ok=True)
        hosted_links = {}

        for year in YEARS:
            print(f"\nSearching {ticker} {form} for year {year}...")

            search_params = {
                "query": f'ticker:{ticker} AND formType:"{form}" AND filedAt:[{year}-01-01 TO {year}-12-31]',
                "from": "0",
                "size": "1",
                "sort": [{"filedAt": {"order": "desc"}}]
            }

            try:
                response = queryApi.get_filings(search_params)
            except Exception as e:
                print(f"[ERROR] SEC-API failed for {ticker} {form} {year}: {e}")
                not_found_log.append((ticker, form, year, "SEC-API error"))
                continue

            filings = response.get("filings", [])
            if not filings:
                print(f"[INFO] No filing found for {ticker} {form} in {year}")
                not_found_log.append((ticker, form, year, "No filing found"))
                continue

            filing = filings[0]
            accession = filing.get("accessionNo")
            filed_date = filing.get("filedAt", "")[:10]
            print(f"Found: Accession {accession} | Filed: {filed_date}")

            doc_url = None
            if filing.get("documentFormatFiles"):
                doc_url = normalize_url(filing["documentFormatFiles"][0]["documentUrl"])
            elif "linkToTxt" in filing:
                doc_url = normalize_url(filing["linkToTxt"])

            if not doc_url:
                print(f"[WARN] No document link found for {ticker} {form} {year}")
                not_found_log.append((ticker, form, year, "No linkToTxt or documentFormatFiles"))
                continue

            print(f"Downloading: {doc_url}")
            try:
                response_doc = requests.get(doc_url, headers=HEADERS)
                if response_doc.status_code != 200:
                    print(f"[ERROR] HTTP {response_doc.status_code} for {ticker} {form} {year}")
                    not_found_log.append((ticker, form, year, f"HTTP {response_doc.status_code}"))
                    continue

                if len(response_doc.text.strip()) < 500:
                    print(f"[WARN] Empty or incomplete content for {ticker} {form} {year}")
                    not_found_log.append((ticker, form, year, "Empty or small content"))
                    continue

                ext = os.path.splitext(doc_url)[-1].split("?")[0]
                filename = f"{form.replace(' ', '_')}_{year}{ext}"
                save_path = os.path.join(form_dir, filename)

                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(response_doc.text)

                print(f"Saved to: {save_path}")
                hosted_links[filename] = doc_url

            except Exception as e:
                print(f"[ERROR] Download failed for {ticker} {form} {year}: {e}")
                not_found_log.append((ticker, form, year, "Download error"))

            # Save debug JSON
            debug_path = os.path.join(form_dir, f"debug_{form}_{year}.json")
            with open(debug_path, "w") as f:
                json.dump(filing, f, indent=2)
            print(f"API JSON saved to: {debug_path}")

            sleep(1.0)

        # Write manifest
        manifest_path = os.path.join(form_dir, "hosted_links.json")
        with open(manifest_path, "w") as f:
            json.dump(hosted_links, f, indent=2)
        print(f"Manifest written to: {manifest_path}")

# Final summary
if not_found_log:
    print("\nSummary of Missing or Failed Filings:")
    df = pd.DataFrame(not_found_log, columns=["Ticker", "Form Type", "Year", "Reason"])
    print(df.to_string(index=False))
    df.to_csv("missing_filings.csv", index=False)
    print("Detailed report saved to: missing_filings.csv")
else:
    print("\nAll filings successfully downloaded.")
