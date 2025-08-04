#!/usr/bin/env python3

from __future__ import annotations

import itertools
import json
import logging
import pathlib
import re
import sys
import traceback
import unicodedata
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag
from bs4 import XMLParsedAsHTMLWarning
from tabulate import tabulate
from tqdm import tqdm
from unidecode import unidecode
import os

# CONFIGURATION
TICKERS_TO_PROCESS: list[str] = ["AAPL"]           
FILINGS_DIR      = pathlib.Path("filings/filings")   
LINKS_DIR        = pathlib.Path("links")    
OUT_DIR          = pathlib.Path("chunks")   
LOG_PATH         = pathlib.Path("parse_def14a.log")

TOKEN_LIMIT      = 512                    # max tokens per chunk
TOKEN_OVERLAP    = int(TOKEN_LIMIT * 0.12)  # sliding-window overlap

if "TICKERS_TO_PROCESS" in os.environ:
    TICKERS_TO_PROCESS = os.environ["TICKERS_TO_PROCESS"].split(",")

if "FILINGS_DIR" in os.environ:
    FILINGS_DIR = pathlib.Path(os.environ["FILINGS_DIR"])

if "LINKS_DIR" in os.environ:
    LINKS_DIR = pathlib.Path(os.environ["LINKS_DIR"])

if "OUT_DIR" in os.environ:
    OUT_DIR = pathlib.Path(os.environ["OUT_DIR"])

# LOG SETUP
file_handler = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)  # keep console clean
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

log = logging.getLogger("DEF14AParser")
log.setLevel(logging.INFO)
log.addHandler(file_handler)
log.addHandler(console_handler)
log.propagate = False

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# CANONICAL SECTION MAP
RAW_CANON_SECTIONS: List[str] = [
    "Cover Page",
    "Item 1. Date, Time & Place Information",
    "Item 2. Revocability of Proxy",
    "Item 3. Dissenters’ Right of Appraisal",
    "Item 4. Persons Making the Solicitation",
    "Item 5. Interest of Certain Persons in Matters to Be Acted Upon",
    "Item 6. Voting Securities & Principal Holders",
    "Item 7. Directors & Executive Officers",
    "Item 8. Compensation of Directors & Executive Officers",
    "Compensation Discussion and Analysis (CD&A)",
    "Pay Ratio Disclosure",
    "Item 9. Independent Public Accountants",
    "Item 10. Compensation Plans & Arrangements",
    "Item 11. Authorization or Issuance of Securities Other Than for Exchange",
    "Item 12. Modification or Exchange of Securities",
    "Item 13. Financial & Other Information (Merger/Acquisition Context)",
    "Item 14. Mergers, Consolidations, Acquisitions & Similar Matters",
    "Item 15. Acquisition or Disposition of Property",
    "Item 16. Restatement of Accounts",
    "Item 17. Action with Respect to Reports",
    "Item 18. Matters Not Required to Be Submitted",
    "Item 19. Amendment of Charter, By-laws or Other Documents",
    "Item 20. Other Proposed Action",
    "Item 21. Voting Procedures",
    "Item 22. Information Required in Investment-Company Proxy Statements",
    "Signatures",
    "Exhibits Index",
]

ITEM_ID_RE = re.compile(r"item\s+(\d{1,2}[a-z]?)", re.I)
ITEM_MAP   = {
    f"ITEM {m.group(1).upper()}": canon
    for canon in RAW_CANON_SECTIONS
    if (m := ITEM_ID_RE.search(canon))
}

# TEXT UTILITIES
ZW_CHARS = ["\u200b", "\u200c", "\u200d", "\u2060"]

def clean_text(text: str) -> str:
    if not text:
        return ""
    for ch in ZW_CHARS:
        text = text.replace(ch, "")
    text = (text.replace("&nbsp;", " ")
                .replace("&amp;", "&")
                .replace("\xa0", " "))
    text = unicodedata.normalize("NFC", text)
    text = unidecode(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    return text.strip()

def token_count(txt: str) -> int:
    return len(txt.split())

# TABLE HELPERS
def table_to_dict(tbl: Tag, pre: str, post: str) -> Dict[str, Any]:
    rows = [
        [clean_text(td.get_text(" ", strip=True))
         for td in tr.find_all(["th", "td"])]
        for tr in tbl.find_all("tr")
    ]
    headers = rows[0] if tbl.find("th") else []
    return {
        "headers": headers,
        "data": rows,
        "pre_context": pre,
        "post_context": post,
    }

def dict_to_markdown(tbl: Dict[str, Any]) -> str:
    hdrs = tbl["headers"] or ["" for _ in tbl["data"][0]]
    md  = "|" + "|".join(hdrs) + "|\n"
    md += "|" + "|".join("---" for _ in hdrs) + "|\n"
    for row in tbl["data"][1:]:
        md += "|" + "|".join(row) + "|\n"
    return md.strip()

#  HEADING / SECTION DETECTION
DASH_FIX = str.maketrans({"–": "-", "—": "-"})

def canonical_from_heading(txt: str) -> Optional[str]:
    cleaned = clean_text(txt).translate(DASH_FIX)
    if m := ITEM_ID_RE.search(cleaned):
        return ITEM_MAP.get(f"ITEM {m.group(1).upper()}")
    lowered = cleaned.lower()
    for canon in RAW_CANON_SECTIONS:
        if canon.lower().translate(DASH_FIX) in lowered:
            return canon
    return None

def is_subheading(tag: Tag) -> bool:
    style = (tag.get("style") or "").lower()
    txt   = clean_text(tag.get_text(" ", strip=True))
    return (
        tag.name in {"b", "strong"} or
        "bold" in style or
        "font-weight:700" in style or
        (txt.isupper() and 1 < len(txt.split()) <= 12)
    )

#  TAG (KEYWORD) EXTRACTION
CAP_SEQ = re.compile(r"[A-Z][\w\u2019']*(?:\s+[A-Z][\w\u2019']*)*")

def extract_tags(heading: str) -> List[str]:
    tags = set()
    for m in CAP_SEQ.finditer(heading):
        phrase = m.group(0)
        tags.add(phrase)
        if (ws := phrase.split()) and len(ws) > 1:
            tags.add(ws[-1])
    return sorted(tags, key=str.lower)

# CORE PARSER
def parse_def14a_file(
    fp: pathlib.Path,
    link_map: Dict[str, str]
) -> Tuple[Dict[str, Any], set[str]]:
    html_raw = fp.read_text("utf-8", errors="ignore")
    soup = BeautifulSoup(html_raw, "lxml")

    # remove scripts/styles/etc.
    for tag in soup(["script", "style", "header", "footer"]):
        tag.decompose()

    accession   = fp.stem.split("_")[-1]
    filing_date = (re.search(r"(\d{8})", accession) or [""])[0]
    parts       = fp.relative_to(FILINGS_DIR).parts   # <TIC>/DEF 14A/<YR>/…
    ticker, _, year = parts[:3]

    # initialise section containers
    sections = {
        canon: {
            "html_blocks": [],
            "subsections": defaultdict(list),
            "tables": [],
            "chunks": [],
            "missing": True
        } for canon in RAW_CANON_SECTIONS
    }

    body = soup.body or soup
    nodes: List[Tag | NavigableString] = list(body.descendants)

    current_sec: Optional[str] = None
    current_sub: Optional[str] = None

    for i, node in enumerate(nodes):
        if not isinstance(node, Tag):
            continue
        txt = clean_text(node.get_text(" ", strip=True))
        if not txt:
            continue

        if canon := canonical_from_heading(txt):
            current_sec, current_sub = canon, None
            sections[canon]["missing"] = False
            log.debug("%s | ⇒ section %s", fp.name, canon)
            continue

        if current_sec is None:
            continue

        if is_subheading(node):
            current_sub = txt
            continue

        # table capture
        if node.name == "table":
            pre  = clean_text(nodes[i-1].get_text(" ", strip=True)) if i else ""
            post = clean_text(nodes[i+1].get_text(" ", strip=True)) if i+1 < len(nodes) else ""
            tbl  = table_to_dict(node, pre, post)
            sections[current_sec]["tables"].append(tbl)
            # inject md table into text flow
            md_table = dict_to_markdown(tbl)
            sections[current_sec]["html_blocks"].append(f"{pre}\n\n{md_table}\n\n{post}")
            continue

        # regular text node
        target = (
            sections[current_sec]["html_blocks"]
            if current_sub is None
            else sections[current_sec]["subsections"][current_sub]
        )
        target.append(txt)

    # chunking
    missing_urls: set[str] = set()

    def chunk_meta(sec: str, st: int, ed: int) -> Dict[str, Any]:
        url = link_map.get(accession, "")
        if not url:
            missing_urls.add(accession)
            log.debug("%s | no SEC url for %s", fp.name, accession)
        return {
            "section": sec,
            "start_token": st,
            "end_token": ed,
            "token_count": ed - st,
            "accession": accession,
            "filing_date": filing_date,
            "ticker": ticker,
            "source_url": url,
            "tags": extract_tags(sec),
        }

    for sec, obj in sections.items():
        if obj["missing"]:
            log.info("%s | %s → missing", fp.name, sec)
            continue

        full_text = "\n".join(
            obj["html_blocks"] +
            list(itertools.chain.from_iterable(obj["subsections"].values()))
        )
        tokens = token_count(full_text)

        if tokens <= TOKEN_LIMIT:
            obj["chunks"].append({
                "text": full_text,
                "meta": chunk_meta(sec, 0, tokens)
            })
        else:
            words, start = full_text.split(), 0
            while start < tokens:
                end   = min(start + TOKEN_LIMIT, tokens)
                chunk = " ".join(words[start:end])
                obj["chunks"].append({
                    "text": chunk,
                    "meta": chunk_meta(sec, start, end)
                })
                if end == tokens:
                    break
                start = end - TOKEN_OVERLAP

        log.info("%s | %s → %d chunks (%d tokens)",
                 fp.name, sec, len(obj["chunks"]), tokens)

    parsed_doc = {
        "meta": {
            "ticker": ticker,
            "form_type": "DEF 14A",
            "fiscal_year": int(year),
            "accession": accession,
            "source_url": link_map.get(accession, ""),
            "local_path": str(fp),
            "parsed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "sections": sections,
    }
    return parsed_doc, missing_urls

#  SUPPORT ROUTINES
def load_links(ticker: str) -> Dict[str, str]:
    lp = LINKS_DIR / ticker / "links.json"
    if not lp.exists():
        return {}
    with lp.open(encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for form, year_map in data.get("links", {}).items():
        if form != "DEF 14A":
            continue
        for year, filings in year_map.items():
            for key, url in filings.items():
                out[key.split("_")[-1]] = url
    return out

def enumerate_def14a() -> Dict[str, Dict[str, List[pathlib.Path]]]:
    out: Dict[str, Dict[str, List[pathlib.Path]]] = defaultdict(lambda: defaultdict(list))
    for ticker_dir in FILINGS_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        tic = ticker_dir.name
        if TICKERS_TO_PROCESS and tic not in TICKERS_TO_PROCESS:
            continue
        for year_dir in (ticker_dir / "DEF 14A").glob("*"):
            if year_dir.is_dir():
                out[tic][year_dir.name].extend(year_dir.glob("*.htm"))
    return out

def log_counts(counts: Dict[str, Dict[str, List[pathlib.Path]]]) -> None:
    years = sorted({y for d in counts.values() for y in d})
    rows  = [
        [tic] + [len(counts[tic].get(y, [])) for y in years]
        for tic in sorted(counts)
    ]
    table = tabulate(rows, headers=["Ticker"] + years, tablefmt="github")
    print("\nDetected DEF 14A files\n" + table)
    log.info("\nDetected DEF 14A files\n%s", table)

# MAIN
def main() -> None:
    counts = enumerate_def14a()
    log_counts(counts)

    jobs: list[tuple[str, str, pathlib.Path, dict[str, str]]] = []
    for tic, year_map in counts.items():
        links = load_links(tic)
        for yr, files in year_map.items():
            jobs.extend((tic, yr, fp, links) for fp in files)

    success = fail = 0
    missing_all: set[str] = set()

    with tqdm(total=len(jobs), desc="Processing DEF 14A", unit="file") as bar:
        for tic, yr, fp, link_map in jobs:
            out_dir = OUT_DIR / tic / "DEF 14A" / yr
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                parsed, miss = parse_def14a_file(fp, link_map)
                dest = out_dir / (fp.stem + "_chunks.json")
                dest.write_text(json.dumps(parsed, indent=2, ensure_ascii=False))
                success += 1
                missing_all.update(miss)
            except Exception:
                fail += 1
                log.error("Error parsing %s\n%s", fp, traceback.format_exc())
            bar.update(1)

    if missing_all:
        log.warning("Missing SEC source URLs for: %s",
                    ", ".join(sorted(missing_all)))

    if fail == 0:
        print(f"\nFinished. All {success} filings processed successfully.")
        log.info("Finished run. All %s filings processed successfully.", success)
    else:
        print(f"\nFinished.  Successful: {success}   Failed: {fail}")
        log.info("Finished run.  Successful: %s   Failed: %s", success, fail)

if __name__ == "__main__":
    main()
