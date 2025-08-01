import os
import json
import requests
import pandas as pd
from sec_api import QueryApi
from time import sleep

# Configuration
API_KEY = "key"  
EMAIL = "youremail@example.com"  
TICKER = "AAPL"
FORM_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"]
YEARS = [2024]
BASE_DIR = f"./filings/{TICKER}"

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

for form in FORM_TYPES:
    form_dir = os.path.join(BASE_DIR, form.replace(" ", "_"))
    os.makedirs(form_dir, exist_ok=True)
    hosted_links = {}

    for year in YEARS:
        print(f"\nSearching {TICKER} {form} for year {year}...")

        search_params = {
            "query": f'ticker:{TICKER} AND formType:"{form}" AND filedAt:[{year}-01-01 TO {year}-12-31]',
            "from": "0",
            "size": "1",
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        try:
            response = queryApi.get_filings(search_params)
        except Exception as e:
            print(f"[ERROR] SEC-API failed for {form} {year}: {e}")
            not_found_log.append((form, year, "SEC-API error"))
            continue

        filings = response.get("filings", [])
        if not filings:
            print(f"[INFO] No filing found for {form} in {year}")
            not_found_log.append((form, year, "No filing found"))
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
            print(f"[WARN] No document link found for {form} {year}")
            not_found_log.append((form, year, "No linkToTxt or documentFormatFiles"))
            continue

        print(f"Downloading: {doc_url}")
        try:
            response_doc = requests.get(doc_url, headers=HEADERS)
            if response_doc.status_code != 200:
                print(f"[ERROR] HTTP {response_doc.status_code} for {form} {year}")
                not_found_log.append((form, year, f"HTTP {response_doc.status_code}"))
                continue

            if len(response_doc.text.strip()) < 500:
                print(f"[WARN] Empty or incomplete HTML/text for {form} {year}")
                not_found_log.append((form, year, "Empty or small content"))
                continue

            ext = os.path.splitext(doc_url)[-1].split("?")[0]
            filename = f"{form.replace(' ', '_')}_{year}{ext}"
            save_path = os.path.join(form_dir, filename)

            with open(save_path, "w", encoding="utf-8") as f:
                f.write(response_doc.text)

            print(f"Saved to: {save_path}")
            hosted_links[filename] = doc_url

        except Exception as e:
            print(f"[ERROR] Download failed for {form} {year}: {e}")
            not_found_log.append((form, year, "Download error"))

        debug_path = os.path.join(form_dir, f"debug_{form}_{year}.json")
        with open(debug_path, "w") as f:
            json.dump(filing, f, indent=2)
        print(f"API JSON saved to: {debug_path}")

        sleep(1.0)  # crawl delay

    manifest_path = os.path.join(form_dir, "hosted_links.json")
    with open(manifest_path, "w") as f:
        json.dump(hosted_links, f, indent=2)
    print(f"Manifest written to: {manifest_path}")

if not_found_log:
    print("\nSummary of Missing or Failed Filings:")
    df = pd.DataFrame(not_found_log, columns=["Form Type", "Year", "Reason"])
    print(df.to_string(index=False))
    df.to_csv("missing_filings.csv", index=False)
    print("Detailed report saved to: missing_filings.csv")
else:
    print("\nAll filings successfully downloaded.")
