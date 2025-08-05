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
TICKERS_TO_PROCESS = ["AAPL"]         
FILINGS_DIR = pathlib.Path("filings/filings")
LINKS_DIR = pathlib.Path("links")
OUT_DIR = pathlib.Path("chunks")
LOG_FILE = pathlib.Path("parse_10k.log")
TOKEN_LIMIT = 512                    # chunk size
TOKEN_OVERLAP = int(TOKEN_LIMIT * 0.12)

if "TICKERS_TO_PROCESS" in os.environ:
    TICKERS_TO_PROCESS = os.environ["TICKERS_TO_PROCESS"].split(",")

if "FILINGS_DIR" in os.environ:
    FILINGS_DIR = pathlib.Path(os.environ["FILINGS_DIR"])

if "LINKS_DIR" in os.environ:
    LINKS_DIR = pathlib.Path(os.environ["LINKS_DIR"])

if "OUT_DIR" in os.environ:
    OUT_DIR = pathlib.Path(os.environ["OUT_DIR"])

# LOG SETUP

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
console_handler.setLevel(logging.WARNING)

log = logging.getLogger("10KParser")
log.setLevel(logging.INFO)
log.addHandler(file_handler)
log.addHandler(console_handler)
log.propagate = False  # no double-logging

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# CANONICAL SECTION MAP

RAW_CANON_SECTIONS: List[str] = [
    "Cover Page",
    "Table of Contents",
    "Forward-Looking Statements",
    "Part I – Item 1. Business",
    "Part I – Item 1A. Risk Factors",
    "Part I – Item 1B. Unresolved Staff Comments",
    "Part I – Item 1C. Cybersecurity",
    "Part I – Item 2. Properties",
    "Part I – Item 3. Legal Proceedings",
    "Part I – Item 4. Mine Safety Disclosures",
    "Part II – Item 5. Market for Registrants Common Equity, Related Stockholder Matters & Issuer Purchases of Equity Securities",
    "Part II – Item 6. [Reserved]",
    "Part II – Item 7. Management's Discussion & Analysis (MD&A)",
    "Part II – Item 7A. Quantitative & Qualitative Disclosures About Market Risk",
    "Part II – Item 8. Financial Statements & Supplementary Data",
    "Part II – Item 9. Changes in & Disagreements With Accountants",
    "Part II – Item 9A. Controls & Procedures",
    "Part II – Item 9B. Other Information",
    "Part II – Item 9C. Foreign Jurisdiction Audit Inspection Disclosure",
    "Part III – Item 10. Directors, Executive Officers & Corporate Governance",
    "Part III – Item 11. Executive Compensation",
    "Part III – Item 12. Security Ownership of Certain Beneficial Owners & Management",
    "Part III – Item 13. Certain Relationships & Related Transactions",
    "Part III – Item 14. Principal Accounting Fees & Services",
    "Part IV – Item 15. Exhibits & Financial-Statement Schedules",
    "Part IV – Item 16. Form 10-K Summary",
    "Certifications",
    "Signatures",
    "Exhibits Index",
]

# ITEM-anchor map
ITEM_MAP: Dict[str, str] = {}
ITEM_ID_RE = re.compile(r"item\s+(\d{1,2}[a-zA-Z]?)", re.I)
for canon in RAW_CANON_SECTIONS:
    if (m := ITEM_ID_RE.search(canon)):
        ITEM_MAP[f"ITEM {m.group(1).upper()}"] = canon

# TEXT UTILITIES

NON_PRINTING = ["\u200b", "\u200c", "\u200d", "\u2060"]  # ZW chars

def clean_text(text: str) -> str:
    if not text:
        return ""
    for ch in NON_PRINTING:
        text = text.replace(ch, "")
    text = text.replace("&amp;", "&")
    text = unicodedata.normalize("NFC", text)
    text = unidecode(text)
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()

def token_count(txt: str) -> int:
    return len(txt.split())

# TABLE HELPERS

def table_to_dict(tbl: Tag, pre: str, post: str) -> Dict[str, Any]:
    rows = [
        [clean_text(td.get_text(" ", strip=True)) for td in tr.find_all(["th", "td"])]
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
    md = "|" + "|".join(hdrs) + "|\n"
    md += "|" + "|".join("---" for _ in hdrs) + "|\n"
    for row in tbl["data"][1:]:
        md += "|" + "|".join(row) + "|\n"
    return md.strip()

# HEADING / SECTION DETECTION

def match_section(text: str) -> Optional[str]:
    cleaned = clean_text(text)
    if (m := ITEM_ID_RE.search(cleaned)):
        return ITEM_MAP.get(f"ITEM {m.group(1).upper()}")
    lowered = cleaned.lower().replace("–", "-").replace("—", "-")
    for canon in RAW_CANON_SECTIONS:
        if canon.lower().replace("–", "-").replace("—", "-") in lowered:
            return canon
    return None

def detect_subheading(node: Tag) -> bool:
    style = (node.get("style") or "").lower()
    txt = clean_text(node.get_text(" ", strip=True))
    return (
        node.name in {"b", "strong"} or
        "bold" in style or
        "font-weight:700" in style or
        (txt.isupper() and 1 < len(txt.split()) <= 12)
    )

# TAG EXTRACTION (capital phrases)  

CAP_SEQ = re.compile(r"[A-Z][\w\u2019']*(?:\s+[A-Z][\w\u2019']*)*")

def extract_tags(heading: str) -> List[str]:
    tags: set[str] = set()
    for m in CAP_SEQ.finditer(heading):
        phrase = m.group(0)
        tags.add(phrase)
        if len(words := phrase.split()) > 1:
            tags.add(words[-1])
    return sorted(tags, key=str.lower)

# CORE PARSER  

def parse_10k_file(
    fp: pathlib.Path, link_map: Dict[str, str]
) -> Tuple[Dict[str, Any], set[str]]:
    html_raw = fp.read_text("utf-8", errors="ignore")
    soup = BeautifulSoup(html_raw, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()

    accession = fp.stem.split("_")[-1]
    filing_date = (re.search(r"(\d{8})", accession) or [""])[0]
    rel_parts = fp.relative_to(FILINGS_DIR).parts
    ticker, _, year = rel_parts[:3]

    sections = {
        lab: {
            "html_blocks": [],
            "subsections": defaultdict(list),
            "tables": [],
            "chunks": [],
            "missing": True,
        }
        for lab in RAW_CANON_SECTIONS
    }

    body = soup.body or soup
    nodes = list(body.descendants)
    current_section: Optional[str] = None
    current_sub: Optional[str] = None

    for i, node in enumerate(nodes):
        if not isinstance(node, Tag):
            continue
        txt = clean_text(node.get_text(" ", strip=True))
        if not txt:
            continue

        if (canon := match_section(txt)):
            current_section, current_sub = canon, None
            sections[canon]["missing"] = False
            log.debug("%s | § %s", fp.name, canon)
            continue

        if current_section is None:
            continue

        if detect_subheading(node):
            current_sub = txt
            continue

        if node.name == "table":
            pre  = clean_text(nodes[i-1].get_text(" ", strip=True)) if i else ""
            post = clean_text(nodes[i+1].get_text(" ", strip=True)) if i+1 < len(nodes) else ""
            tbl_dict = table_to_dict(node, pre, post)
            sections[current_section]["tables"].append(tbl_dict)

            md_table   = dict_to_markdown(tbl_dict)

            sections[current_section]["html_blocks"].append(
                f"{pre}\n\n{md_table}\n\n{post}"
            )
            continue

        target = (
            sections[current_section]["html_blocks"]
            if current_sub is None
            else sections[current_section]["subsections"][current_sub]
        )
        target.append(txt)

    #  chunkin 
    missing_sources: set[str] = set()

    def build_meta(sec_name: str, start: int, end: int) -> Dict[str, Any]:
        src = link_map.get(accession, "")
        if not src:
            missing_sources.add(accession)
            log.debug("%s | no source-url in links.json for %s", fp.name, accession)
        else:
            log.debug("%s | source-url OK: %s", fp.name, src)
        return {
            "section": sec_name,
            "start_token": start,
            "end_token": end,
            "token_count": end - start,
            "accession": accession,
            "filing_date": filing_date,
            "ticker": ticker,
            "source_url": src,
            "tags": extract_tags(sec_name),
        }

    for sec_name, sec in sections.items():
        if sec["missing"]:
            log.info("%s | %s → missing", fp.name, sec_name)
            continue

        plain_text = "\n".join(
            sec["html_blocks"] + list(
                itertools.chain.from_iterable(sec["subsections"].values())
            )
        )
        tokens = token_count(plain_text)

        if tokens <= TOKEN_LIMIT:
            sec["chunks"].append({"text": plain_text,
                                  "meta": build_meta(sec_name, 0, tokens)})
        else:
            words = plain_text.split()
            start = 0
            while start < tokens:
                end = min(start + TOKEN_LIMIT, tokens)
                chunk = " ".join(words[start:end])
                sec["chunks"].append({"text": chunk,
                                      "meta": build_meta(sec_name, start, end)})
                if end == tokens:
                    break
                start = end - TOKEN_OVERLAP

        log.info("%s | %s → %d chunks (%d tokens)",
                 fp.name, sec_name, len(sec["chunks"]), tokens)

    parsed_doc = {
        "meta": {
            "ticker": ticker,
            "form_type": "10-K",
            "fiscal_year": int(year),
            "accession": accession,
            "source_url": link_map.get(accession, ""),
            "local_path": str(fp),
            "parsed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "sections": sections,
    }
    return parsed_doc, missing_sources

# SUPPORT ROUTINES 

def load_links(ticker: str) -> Dict[str, str]:
    lp = LINKS_DIR / ticker / "links.json"
    if not lp.exists():
        return {}
    with lp.open("r", encoding="utf-8") as f:
        data = json.load(f)
    tenk_links = {}
    for form_type, year_map in data.get("links", {}).items():
        for year, filings in year_map.items():
            for full_key, url in filings.items():
                accession = full_key.split("_")[-1]
                tenk_links[accession] = url
    return tenk_links


def enumerate_filings() -> Dict[str, Dict[str, List[pathlib.Path]]]:
    out: Dict[str, Dict[str, List[pathlib.Path]]] = defaultdict(lambda: defaultdict(list))
    for ticker_dir in FILINGS_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        if TICKERS_TO_PROCESS and ticker not in TICKERS_TO_PROCESS:
            continue
        for year_dir in (ticker_dir / "10-K").iterdir():
            if year_dir.is_dir():
                out[ticker][year_dir.name].extend(year_dir.glob("*.htm"))
    return out

def print_counts(counts: Dict[str, Dict[str, List[pathlib.Path]]]):
    years = sorted({y for d in counts.values() for y in d})
    rows = [[tic] + [len(counts[tic].get(y, [])) for y in years]
            for tic in sorted(counts)]
    table = tabulate(rows, headers=["Ticker"] + years, tablefmt="github")
    print("\nDetected 10-K files\n" + table)
    log.info("\nDetected 10-K files\n%s", table)

# MAIN

def main():
    counts = enumerate_filings()
    print_counts(counts)

    worklist: List[Tuple[str, str, pathlib.Path, Dict[str, str]]] = []
    for ticker, yearmap in counts.items():
        link_map = load_links(ticker)
        for year, files in yearmap.items():
            worklist.extend((ticker, year, fp, link_map) for fp in files)

    success = fail = 0
    missing_sources_all: set[str] = set()

    with tqdm(total=len(worklist), desc="Processing 10-Ks", unit="file") as bar:
        for ticker, year, fp, link_map in worklist:
            out_dir = OUT_DIR / ticker / "10-K" / year
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                parsed, misses = parse_10k_file(fp, link_map)
                (out_dir / (fp.stem + "_chunks.json")).write_text(
                    json.dumps(parsed, indent=2, ensure_ascii=False)
                )
                success += 1
                missing_sources_all.update(misses)
            except Exception:
                fail += 1
                log.error("Error parsing %s\n%s", fp, traceback.format_exc())
            bar.update(1)

    if missing_sources_all:
        log.warning("Missing SEC source URLs for: %s",
                    ", ".join(sorted(missing_sources_all)))

    if fail == 0:
        print(f"\nFinished. All {success} filings processed successfully.")
        log.info("Finished run. All %s filings processed successfully.", success)
    else:
        print(f"\nFinished.  Successful: {success}   Failed: {fail}")
        log.info("Finished run.  Successful: %s   Failed: %s", success, fail)


if __name__ == "__main__":
    main()
