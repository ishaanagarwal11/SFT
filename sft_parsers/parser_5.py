#!/usr/bin/env python

from __future__ import annotations

import json
import logging
import pathlib
import re
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import xml.etree.ElementTree as ET
from tabulate import tabulate
from tqdm import tqdm
import os

# CONFIGURATION
TICKERS_TO_PROCESS: List[str] = ["AAPL"]          
FILING_TYPE_DIRNAME = "5"                       
FILINGS_DIR = pathlib.Path("filings/filings") 
LINKS_DIR   = pathlib.Path("links")          
OUT_DIR        = pathlib.Path("chunks")          
LOG_FILE       = pathlib.Path("parse_form5.log")

# Tokenisation / chunking
TOKEN_LIMIT   = 512
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
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
console_handler.setLevel(logging.WARNING)

log = logging.getLogger("Form5Parser")
log.setLevel(logging.INFO)
log.addHandler(file_handler)
log.addHandler(console_handler)
log.propagate = False

# CANONICAL SECTIONS
FORM5_CANON_SECTIONS: List[str] = [
    "Cover Page & Statement Period",
    "Issuer Information",
    "Reporting Owner Information",
    "Table I – Annual Statement of Non-Derivative Securities",
    "Table II – Annual Statement of Derivative Securities",
    "Footnotes/Remarks",
    "Date Signed",
    "Signature",
]

# Map XML element paths → canonical section label
# (kept for extensibility; Form 5 schema aligns closely with Form 3/4)
XML_SECTION_MAP: Dict[str, str] = {}

# TEXT UTILITIES #
NON_PRINTING = {"\u200b", "\u200c", "\u200d", "\u2060"}

def clean_text(txt: str) -> str:
    for ch in NON_PRINTING:
        txt = txt.replace(ch, "")
    return re.sub(r"[ \t\r\f\v]+", " ", txt.replace("\n", " ").strip())

def token_count(text: str) -> int:
    return len(text.split())

CAP_SEQ = re.compile(r"[A-Z][A-Z0-9&\-]*(?: [A-Z0-9&\-]+)*")

def extract_tags(heading: str) -> List[str]:
    tags: set[str] = set()
    for m in CAP_SEQ.finditer(heading.upper()):
        phrase = m.group(0)
        tags.add(phrase)
        if " " in phrase:
            tags.add(phrase.split()[-1])
    return sorted(tags)

# XML → SECTION EXTRACTION
def markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    if not headers and not rows:
        return ""
    hdr = headers or [""] * len(rows[0])
    md = "|" + "|".join(hdr) + "|\n"
    md += "|" + "|".join(["---"] * len(hdr)) + "|\n"
    for row in rows:
        md += "|" + "|".join(row) + "|\n"
    return md.strip()

def parse_form5_xml(fp: pathlib.Path, link_map: Dict[str, str]) -> Tuple[Dict[str, Any], set[str]]:
    """Parse a single Form 5 XML → structured dict & missing-URL set"""
    accession = fp.stem.split("_")[-1]
    filing_date_match = re.search(r"(\d{8})", accession)
    filing_date = filing_date_match.group(1) if filing_date_match else ""
    rel_parts = fp.relative_to(FILINGS_DIR).parts            # <TICKER>/5/<YEAR>/file.xml
    ticker, _, year = rel_parts[:3]

    tree = ET.parse(fp)
    root = tree.getroot()

    # Base structure for all canonical sections
    sections: Dict[str, Dict[str, Any]] = {
        s: {"html_blocks": [], "subsections": {}, "tables": [], "chunks": [], "missing": True}
        for s in FORM5_CANON_SECTIONS
    }

    def add_block(section: str, text: str):
        if text:
            sections[section]["html_blocks"].append(clean_text(text))
            sections[section]["missing"] = False

    # Cover Page & Statement Period
    doc_type = root.findtext("documentType", default="")
    period   = root.findtext("periodOfReport", default="")
    schema   = root.findtext("schemaVersion", default="")
    add_block("Cover Page & Statement Period",
              f"Document Type: {doc_type}\nPeriod of Report: {period}\nSchema Version: {schema}")

    # Issuer Information
    issuer = root.find("issuer")
    if issuer is not None:
        data = [
            ("CIK", issuer.findtext("issuerCik", "")),
            ("Name", issuer.findtext("issuerName", "")),
            ("Trading Symbol", issuer.findtext("issuerTradingSymbol", "")),
        ]
        md = markdown_table(["Field", "Value"], [[k, v] for k, v in data])
        add_block("Issuer Information", md)

    # Reporting Owner Information
    rep = root.find("reportingOwner")
    if rep is not None:
        owner_id  = rep.find("reportingOwnerId")
        owner_addr = rep.find("reportingOwnerAddress")
        owner_rel  = rep.find("reportingOwnerRelationship")
        rows = [
            ["Owner CIK", owner_id.findtext("rptOwnerCik", "")],
            ["Owner Name", owner_id.findtext("rptOwnerName", "")],
            ["Street 1", owner_addr.findtext("rptOwnerStreet1", "")],
            ["City", owner_addr.findtext("rptOwnerCity", "")],
            ["State", owner_addr.findtext("rptOwnerState", "")],
            ["Zip", owner_addr.findtext("rptOwnerZipCode", "")],
            ["Is Officer", owner_rel.findtext("isOfficer", "")],
            ["Officer Title", owner_rel.findtext("officerTitle", "")],
        ]
        md = markdown_table(["Field", "Value"], rows)
        add_block("Reporting Owner Information", md)

    # Table I – Non-Derivative
    non_derivative = root.find("nonDerivativeTable")
    if non_derivative is not None:
        headers = ["Security Title", "Shares Owned", "Ownership"]
        rows: List[List[str]] = []
        for holding in non_derivative.findall("nonDerivativeHolding"):
            rows.append([
                holding.findtext("securityTitle/value") or "",
                holding.findtext("postTransactionAmounts/sharesOwnedFollowingTransaction/value") or "",
                holding.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        md = markdown_table(headers, rows)
        add_block("Table I – Annual Statement of Non-Derivative Securities", md)
        sections["Table I – Annual Statement of Non-Derivative Securities"]["tables"].append(
            {"headers": headers, "data": rows}
        )

    # Table II – Derivative
    derivative = root.find("derivativeTable")
    if derivative is not None:
        headers = ["Security Title", "Underlying Shares", "Expiration", "Ownership"]
        rows: List[List[str]] = []
        for holding in derivative.findall("derivativeHolding"):
            rows.append([
                holding.findtext("securityTitle/value") or "",
                holding.findtext("underlyingSecurity/underlyingSecurityShares/value") or "",
                holding.findtext("expirationDate/value") or "",
                holding.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        md = markdown_table(headers, rows)
        add_block("Table II – Annual Statement of Derivative Securities", md)
        sections["Table II – Annual Statement of Derivative Securities"]["tables"].append(
            {"headers": headers, "data": rows}
        )

    # Footnotes / Remarks
    footnotes = root.find("footnotes")
    if footnotes is not None:
        for fn in footnotes.findall("footnote"):
            fid = fn.attrib.get("id", "")
            add_block("Footnotes/Remarks", f"[{fid}] {clean_text(fn.text or '')}")
    remarks = root.findtext("remarks", default="")
    if remarks:
        add_block("Footnotes/Remarks", f"Remarks: {remarks}")

    # Signature & Date Signed
    sig = root.find("ownerSignature")
    if sig is not None:
        add_block("Signature", sig.findtext("signatureName", ""))
        add_block("Date Signed", sig.findtext("signatureDate", ""))

    # Chunking #
    missing_sources: set[str] = set()

    def build_meta(sec_name: str, start: int, end: int) -> Dict[str, Any]:
        src_url = link_map.get(accession, "")
        if not src_url:
            missing_sources.add(accession)
            log.debug("%s | no source-url in links.json", accession)
        return {
            "section": sec_name,
            "start_token": start,
            "end_token": end,
            "token_count": end - start,
            "accession": accession,
            "filing_date": filing_date,
            "ticker": ticker,
            "source_url": src_url,
            "tags": extract_tags(sec_name),
        }

    for sec_name, sec in sections.items():
        if sec["missing"]:
            log.info("%s | %s → missing", accession, sec_name)
            continue
        plain_text = "\n".join(sec["html_blocks"])
        tokens = token_count(plain_text)
        if tokens <= TOKEN_LIMIT:
            sec["chunks"].append({"text": plain_text, "meta": build_meta(sec_name, 0, tokens)})
        else:
            words = plain_text.split()
            start = 0
            while start < tokens:
                end = min(start + TOKEN_LIMIT, tokens)
                chunk_txt = " ".join(words[start:end])
                sec["chunks"].append({"text": chunk_txt, "meta": build_meta(sec_name, start, end)})
                if end == tokens:
                    break
                start = end - TOKEN_OVERLAP
        log.info("%s | %s → %d chunks (%d tokens)", accession, sec_name, len(sec["chunks"]), tokens)

    parsed_doc = {
        "meta": {
            "ticker": ticker,
            "form_type": "5",
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
    data = json.loads(lp.read_text("utf-8"))
    filing_links = {}
    for form_type, year_map in data.get("links", {}).items():
        if form_type != "5":
            continue
        for year, filings in year_map.items():
            for full_key, url in filings.items():
                accession = full_key.split("_")[-1]
                filing_links[accession] = url
    return filing_links

def enumerate_filings() -> Dict[str, Dict[str, List[pathlib.Path]]]:
    out: Dict[str, Dict[str, List[pathlib.Path]]] = defaultdict(lambda: defaultdict(list))
    for ticker_dir in FILINGS_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        if TICKERS_TO_PROCESS and ticker not in TICKERS_TO_PROCESS:
            continue
        form_dir = ticker_dir / FILING_TYPE_DIRNAME
        if not form_dir.exists():
            continue
        for year_dir in form_dir.iterdir():
            if year_dir.is_dir():
                out[ticker][year_dir.name].extend(year_dir.glob("*.xml"))
    return out

def print_counts(counts: Dict[str, Dict[str, List[pathlib.Path]]]):
    years = sorted({y for d in counts.values() for y in d})
    rows = [[tic] + [len(counts[tic].get(y, [])) for y in years] for tic in sorted(counts)]
    table = tabulate(rows, headers=["Ticker"] + years, tablefmt="github")
    print("\nDetected Form 5 files\n" + table)
    log.info("\nDetected Form 5 files\n%s", table)

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
    missing_sources_total: set[str] = set()

    with tqdm(total=len(worklist), desc="Processing Form 5s", unit="file", ncols=90) as bar:
        for ticker, year, fp, link_map in worklist:
            out_dir = OUT_DIR / ticker / FILING_TYPE_DIRNAME / year
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                parsed, misses = parse_form5_xml(fp, link_map)
                out_path = out_dir / (fp.stem + "_chunks.json")
                out_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False))
                success += 1
                missing_sources_total.update(misses)
            except Exception:
                fail += 1
                log.error("Error parsing %s\n%s", fp, traceback.format_exc())
            bar.update(1)

    if missing_sources_total:
        log.warning("Missing SEC source URLs for: %s", ", ".join(sorted(missing_sources_total)))

    if fail == 0:
        print(f"\nFinished. All {success} filings processed successfully.")
        log.info("Finished run. All %s filings processed successfully.", success)
    else:
        print(f"\nFinished.  Successful: {success}   Failed: {fail}")
        log.info("Finished run.  Successful: %s   Failed: %s", success, fail)

if __name__ == "__main__":
    main()
