# sft_parsers/parser_8_k.py

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

from bs4 import BeautifulSoup, NavigableString, Tag
from bs4 import XMLParsedAsHTMLWarning
from tqdm import tqdm
from unidecode import unidecode
from tabulate import tabulate
import os
from config import SELECTED_TICKERS

# CONFIGURATION
TICKERS_TO_PROCESS = SELECTED_TICKERS
FILINGS_DIR = pathlib.Path("./data/filings")
LINKS_DIR = pathlib.Path("./data/links")
OUT_DIR = pathlib.Path("./data/chunks")  # output folder
LOG_FILE         = pathlib.Path("parse_8k.log")

TOKEN_LIMIT      = 512                    # chunk size (words)
TOKEN_OVERLAP    = int(TOKEN_LIMIT * 0.12)

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
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
console_handler.setLevel(logging.WARNING)

log = logging.getLogger("8KParser")
log.setLevel(logging.INFO)
log.addHandler(file_handler)
log.addHandler(console_handler)
log.propagate = False

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# CANONICAL SECTION MAP
RAW_CANON_SECTIONS: List[str] = [
    # Cover + meta
    "Cover Page",
    "Filing Date",
    "Event Date",
    # Item 1
    "Section 1 – 1.01 Entry into a Material Definitive Agreement",
    "Section 1 – 1.02 Termination of a Material Definitive Agreement",
    "Section 1 – 1.03 Bankruptcy or Receivership",
    "Section 1 – 1.04 Mine Safety — Shutdowns & Patterns of Violations",
    "Section 1 – 1.05 Material Cybersecurity Incidents",
    # Item 2
    "Section 2 – 2.01 Completion of Acquisition or Disposition of Assets",
    "Section 2 – 2.02 Results of Operations & Financial Condition",
    "Section 2 – 2.03 Creation of a Direct Financial Obligation or Off-Balance Sheet Obligation",
    "Section 2 – 2.04 Triggering Events That Accelerate or Increase a Direct Financial Obligation",
    "Section 2 – 2.05 Costs Associated with Exit or Disposal Activities",
    "Section 2 – 2.06 Material Impairments",
    # Item 3
    "Section 3 – 3.01 Notice of Delisting or Transfer of Listing",
    "Section 3 – 3.02 Unregistered Sales of Equity Securities",
    "Section 3 – 3.03 Material Modification to Rights of Security Holders",
    # Item 4
    "Section 4 – 4.01 Changes in Registrant’s Certifying Accountant",
    "Section 4 – 4.02 Non-Reliance on Previously Issued Financial Statements",
    # Item 5
    "Section 5 – 5.01 Changes in Control of Registrant",
    "Section 5 – 5.02 Director/Officer Changes & Compensation Arrangements",
    "Section 5 – 5.03 Charter/By-Law Amendments; Fiscal Year Change",
    "Section 5 – 5.04 Trading-Plan Suspension",
    "Section 5 – 5.05 Code of Ethics Amendment or Waiver",
    "Section 5 – 5.06 Change in Shell Company Status",
    "Section 5 – 5.07 Submission of Matters to a Vote of Security Holders",
    "Section 5 – 5.08 Shareholder Director Nominations",
    # Item 6
    "Section 6 – 6.01 ABS Informational & Computational Material",
    "Section 6 – 6.02 Change of Servicer or Trustee",
    "Section 6 – 6.03 Change in Credit Enhancement or Other External Support",
    "Section 6 – 6.04 Failure to Make a Required Distribution",
    "Section 6 – 6.05 Securities Act Updating Disclosure",
    # Item 7
    "Section 7 – 7.01 Regulation FD Disclosure",
    # Item 8
    "Section 8 – 8.01 Other Events",
    # Item 9
    "Section 9 – 9.01 Financial Statements & Exhibits",
    # Exhibits index
    "Exhibits Index",
]

ITEM_ID_RE  = re.compile(r"item\s+(\d{1,2}\.?[A-Za-z0-9]*)", re.I)
ITEM_MAP: Dict[str, str] = {}
for canon in RAW_CANON_SECTIONS:
    m = ITEM_ID_RE.search(canon)
    if m:
        ITEM_MAP[f"ITEM {m.group(1).upper()}"] = canon

# TEXT UTILITIES
NON_PRINTING = ["\u200b", "\u200c", "\u200d", "\u2060"]

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
    row_headers = [r[0] for r in rows[1:]] if headers and len(headers) < len(rows[1]) else []
    return {
        "headers": headers,
        "data": rows[1:] if headers else rows,
        "row_headers": row_headers,
        "pre_context": pre,
        "post_context": post,
    }

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
def parse_8k_file(
    fp: pathlib.Path,
    link_map: Dict[str, str]
) -> Tuple[Dict[str, Any], set[str]]:
    html_raw = fp.read_text("utf-8", errors="ignore")
    soup = BeautifulSoup(html_raw, "lxml")
    for tag in soup(["script", "style", "footer", "header"]):
        tag.decompose()

    accession     = fp.stem.split("_")[-1]
    filing_date   = (re.search(r"(\d{8})", accession) or [""])[0]
    ticker, _, year = fp.relative_to(FILINGS_DIR).parts[:3]

    sections = {
        canon: {
            "html_blocks": [],
            "subsections": defaultdict(list),
            "tables": [],
            "chunks": [],
            "missing": True,
        }
        for canon in RAW_CANON_SECTIONS
    }

    body   = soup.body or soup
    nodes  = list(body.descendants)
    cur_sec: Optional[str] = None
    cur_sub: Optional[str] = None

    for i, node in enumerate(nodes):
        if not isinstance(node, Tag):
            continue
        txt = clean_text(node.get_text(" ", strip=True))
        if not txt:
            continue

        # main section match
        if (canon := match_section(txt)):
            cur_sec, cur_sub = canon, None
            sections[canon]["missing"] = False
            continue

        if cur_sec is None:
            continue

        # subsection
        if detect_subheading(node):
            cur_sub = txt
            continue

        # table
        if node.name == "table":
            pre  = clean_text(nodes[i-1].get_text(" ", strip=True)) if i else ""
            post = clean_text(nodes[i+1].get_text(" ", strip=True)) if i+1 < len(nodes) else ""
            tbl_dict = table_to_dict(node, pre, post)
            sections[cur_sec]["tables"].append(tbl_dict)

            # embed table context for chunking
            sections[cur_sec]["html_blocks"].append(pre)
            sections[cur_sec]["html_blocks"].append("[[TABLE]]")   # placeholder
            sections[cur_sec]["html_blocks"].append(post)
            continue

        # regular text
        target = (
            sections[cur_sec]["html_blocks"]
            if cur_sub is None else
            sections[cur_sec]["subsections"][cur_sub]
        )
        target.append(txt)

    # chunking per section
    missing_sources: set[str] = set()

    def build_meta(sec_name: str, start: int, end: int) -> Dict[str, Any]:
        src = link_map.get(accession, "")
        if not src:
            missing_sources.add(accession)
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
            sec["html_blocks"] +
            list(itertools.chain.from_iterable(sec["subsections"].values()))
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
            "form_type": "8-K",
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
    with lp.open(encoding="utf-8") as f:
        data = json.load(f)
    eightk_links: Dict[str, str] = {}
    for form_type, year_map in data.get("links", {}).items():
        if form_type != "8-K":
            continue
        for filings in year_map.values():
            for full_key, url in filings.items():
                eightk_links[full_key.split("_")[-1]] = url
    return eightk_links

def enumerate_filings() -> Dict[str, Dict[str, List[pathlib.Path]]]:
    """Return {ticker: {year: [Path,…]}} for .htm 8-K filings."""
    out: Dict[str, Dict[str, List[pathlib.Path]]] = defaultdict(lambda: defaultdict(list))
    for ticker_dir in FILINGS_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        if TICKERS_TO_PROCESS and ticker not in TICKERS_TO_PROCESS:
            continue
        eightk_dir = ticker_dir / "8-K"
        if not eightk_dir.exists():
            continue
        for year_dir in eightk_dir.iterdir():
            if year_dir.is_dir():
                out[ticker][year_dir.name].extend(year_dir.glob("*.htm"))
    return out

def print_counts(counts: Dict[str, Dict[str, List[pathlib.Path]]]):
    years = sorted({y for d in counts.values() for y in d})
    rows  = [[tic] + [len(counts[tic].get(y, [])) for y in years] for tic in sorted(counts)]
    table = tabulate(rows, headers=["Ticker"] + years, tablefmt="github")
    print("\nDetected 8-K files\n" + table)
    log.info("\nDetected 8-K files\n%s", table)

# MAIN
def parse8k():
    counts = enumerate_filings()
    if not counts:
        print("No 8-K filings found with current settings.")
        return

    print_counts(counts)

    worklist: List[Tuple[str, str, pathlib.Path, Dict[str, str]]] = []
    for ticker, yearmap in counts.items():
        link_map = load_links(ticker)
        for year, files in yearmap.items():
            worklist.extend((ticker, year, fp, link_map) for fp in files)

    success = fail = 0
    missing_sources_all: set[str] = set()

    with tqdm(total=len(worklist), desc="Processing 8-Ks", unit="file") as bar:
        for ticker, year, fp, link_map in worklist:
            out_dir = OUT_DIR / ticker / "8-K" / year
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                parsed, misses = parse_8k_file(fp, link_map)
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

