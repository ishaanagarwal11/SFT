#!/usr/bin/env python3

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
FORM_DIRNAME            = "4"                   
FILINGS_DIR          = pathlib.Path("filings/filings")
LINKS_DIR            = pathlib.Path("links")
OUT_DIR                 = pathlib.Path("chunks")
LOG_FILE                = pathlib.Path("parse_form4.log")

TOKEN_LIMIT             = 512 # per chunk
TOKEN_OVERLAP           = int(TOKEN_LIMIT * 0.12)


if "TICKERS_TO_PROCESS" in os.environ:
    TICKERS_TO_PROCESS = os.environ["TICKERS_TO_PROCESS"].split(",")

if "FILINGS_DIR" in os.environ:
    FILINGS_DIR = pathlib.Path(os.environ["FILINGS_DIR"])

if "LINKS_DIR" in os.environ:
    LINKS_DIR = pathlib.Path(os.environ["LINKS_DIR"])

if "OUT_DIR" in os.environ:
    OUT_DIR = pathlib.Path(os.environ["OUT_DIR"])

# LOG SETUP 
file_hdlr = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_hdlr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
file_hdlr.setLevel(logging.INFO)

console_hdlr = logging.StreamHandler(sys.stdout)
console_hdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
console_hdlr.setLevel(logging.WARNING)

log = logging.getLogger("Form4Parser")
log.setLevel(logging.INFO)
log.addHandler(file_hdlr)
log.addHandler(console_hdlr)
log.propagate = False

# CANONICAL SECTIONS
FORM4_CANON_SECTIONS: List[str] = [
    "Cover Page & Statement Date",
    "Issuer Information",
    "Reporting Owner Information",
    "Table I – Non-Derivative Securities Acquired/Disposed/Owned",
    "Table II – Derivative Securities Acquired/Disposed/Owned",
    "Footnotes/Remarks",
    "Signature",
    "Date Signed",
]

# XML to canonical section map (primary detection method)
XML_SECTION_MAP: Dict[str, str] = {
    "issuer":                               "Issuer Information",
    "reportingOwner":                       "Reporting Owner Information",
    "nonDerivativeTable":                   "Table I – Non-Derivative Securities Acquired/Disposed/Owned",
    "derivativeTable":                      "Table II – Derivative Securities Acquired/Disposed/Owned",
    "footnotes":                            "Footnotes/Remarks",
    "ownerSignature/signatureName":         "Signature",
    "ownerSignature/signatureDate":         "Date Signed",
}

# TEXT UTILITIES
NON_PRINTING = {"\u200b", "\u200c", "\u200d", "\u2060"}
CAP_SEQ = re.compile(r"[A-Z][A-Z0-9&\-]*(?: [A-Z0-9&\-]+)*")

def clean_text(txt: str) -> str:
    for ch in NON_PRINTING:
        txt = txt.replace(ch, "")
    txt = re.sub(r"\s+", " ", txt.replace("\n", " ")).strip()
    return txt

def token_count(text: str) -> int:
    return len(text.split())

def extract_tags(heading: str) -> List[str]:
    tags: set[str] = set()
    for m in CAP_SEQ.finditer(heading.upper()):
        phrase = m.group(0)
        tags.add(phrase)
        if " " in phrase:
            tags.add(phrase.split()[-1])
    return sorted(tags)

def markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    md = "|" + "|".join(headers) + "|\n"
    md += "|" + "|".join(["---"] * len(headers)) + "|\n"
    for r in rows:
        md += "|" + "|".join(r) + "|\n"
    return md.rstrip()

# XML to SECTION EXTRACTION
def parse_form4_xml(fp: pathlib.Path, link_map: Dict[str, str]) -> Tuple[Dict[str, Any], set[str]]:
    """
    Parse a single Form 4 XML to structured dict & missing-URL set
    """
    accession = fp.stem.split("_")[-1]
    filing_date_match = re.search(r"(\d{8})", accession)
    filing_date = filing_date_match.group(1) if filing_date_match else ""
    ticker, _, year = fp.relative_to(FILINGS_DIR).parts[:3]  # <TICKER>/4/<YEAR>/file.xml

    root = ET.parse(fp).getroot()

    # Build empty scaffold
    sections: Dict[str, Dict[str, Any]] = {
        s: {
            "html_blocks": [],
            "subsections": {},
            "tables": [],
            "chunks": [],
            "missing": True,
        } for s in FORM4_CANON_SECTIONS
    }

    def add_block(sec: str, text: str):
        if text:
            sections[sec]["html_blocks"].append(clean_text(text))
            sections[sec]["missing"] = False

    # Cover Page & Statement Date
    add_block("Cover Page & Statement Date",
              f"Document Type: {root.findtext('documentType', '')}\n"
              f"Period of Report: {root.findtext('periodOfReport', '')}\n"
              f"Transaction Date (earliest): {root.findtext('dateOfEarliestTransaction', '')}")

    # Issuer Information
    issuer = root.find("issuer")
    if issuer is not None:
        rows = [
            ["CIK", issuer.findtext("issuerCik", "")],
            ["Name", issuer.findtext("issuerName", "")],
            ["Trading Symbol", issuer.findtext("issuerTradingSymbol", "")],
        ]
        add_block("Issuer Information", markdown_table(["Field", "Value"], rows))

    # Reporting Owner Information
    rep = root.find("reportingOwner")
    if rep is not None:
        owner_id = rep.find("reportingOwnerId")
        owner_addr = rep.find("reportingOwnerAddress")
        owner_rel  = rep.find("reportingOwnerRelationship")
        rows = [
            ["Owner CIK",     owner_id.findtext("rptOwnerCik", "")],
            ["Owner Name",    owner_id.findtext("rptOwnerName", "")],
            ["Street 1",      owner_addr.findtext("rptOwnerStreet1", "")],
            ["City",          owner_addr.findtext("rptOwnerCity", "")],
            ["State",         owner_addr.findtext("rptOwnerState", "")],
            ["Zip",           owner_addr.findtext("rptOwnerZipCode", "")],
            ["Is Officer",    owner_rel.findtext("isOfficer", "")],
            ["Officer Title", owner_rel.findtext("officerTitle", "")],
        ]
        add_block("Reporting Owner Information", markdown_table(["Field", "Value"], rows))

    # Table I – Non-Derivative
    non_derivative = root.find("nonDerivativeTable")
    if non_derivative is not None:
        headers = ["Security Title", "Trans. Shares", "Trans. Price", "Shares After", "Ownership"]
        rows: List[List[str]] = []
        for tx in non_derivative.findall(".//nonDerivativeTransaction"):
            rows.append([
                tx.findtext("securityTitle/value") or "",
                tx.findtext("transactionAmounts/transactionShares/value") or "",
                tx.findtext("transactionAmounts/transactionPricePerShare/value") or "",
                tx.findtext("postTransactionAmounts/sharesOwnedFollowingTransaction/value") or "",
                tx.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        for hold in non_derivative.findall("nonDerivativeHolding"):
            rows.append([
                hold.findtext("securityTitle/value") or "",
                "", "",  # no transaction
                hold.findtext("postTransactionAmounts/sharesOwnedFollowingTransaction/value") or "",
                hold.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        if rows:
            md = markdown_table(headers, rows)
            add_block("Table I – Non-Derivative Securities Acquired/Disposed/Owned", md)
            sections["Table I – Non-Derivative Securities Acquired/Disposed/Owned"]["tables"].append(
                {"headers": headers, "data": rows}
            )

    # Table II – Derivative
    derivative = root.find("derivativeTable")
    if derivative is not None:
        headers = ["Security Title", "Underlying Shares", "Ex. Price", "Expiration", "Trans. Shares", "Ownership"]
        rows: List[List[str]] = []
        for tx in derivative.findall(".//derivativeTransaction"):
            rows.append([
                tx.findtext("securityTitle/value") or "",
                tx.findtext("underlyingSecurity/underlyingSecurityShares/value") or "",
                tx.findtext("transactionAmounts/transactionPricePerShare/value") or "",
                tx.findtext("expirationDate/value") or "",
                tx.findtext("transactionAmounts/transactionShares/value") or "",
                tx.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        for hold in derivative.findall("derivativeHolding"):
            rows.append([
                hold.findtext("securityTitle/value") or "",
                hold.findtext("underlyingSecurity/underlyingSecurityShares/value") or "",
                "", hold.findtext("expirationDate/value") or "",
                "", hold.findtext("ownershipNature/directOrIndirectOwnership/value") or "",
            ])
        if rows:
            md = markdown_table(headers, rows)
            add_block("Table II – Derivative Securities Acquired/Disposed/Owned", md)
            sections["Table II – Derivative Securities Acquired/Disposed/Owned"]["tables"].append(
                {"headers": headers, "data": rows}
            )

    # Footnotes / Remarks
    foots = root.find("footnotes")
    if foots is not None:
        for fn in foots.findall("footnote"):
            fid = fn.attrib.get("id", "")
            add_block("Footnotes/Remarks", f"[{fid}] {clean_text(fn.text or '')}")
    remarks = root.findtext("remarks", "")
    if remarks:
        add_block("Footnotes/Remarks", f"Remarks: {remarks}")

    # Signature & Date
    sig = root.find("ownerSignature")
    if sig is not None:
        add_block("Signature", sig.findtext("signatureName", ""))
        add_block("Date Signed", sig.findtext("signatureDate", ""))

    # Chunking 
    missing_sources: set[str] = set()

    def build_meta(sec_name: str, start: int, end: int) -> Dict[str, Any]:
        src_url = link_map.get(accession, "")
        if not src_url:
            missing_sources.add(accession)
            log.debug("%s | missing source-url in links.json", accession)
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
            log.info("%s | %s to missing", accession, sec_name)
            continue
        plain = "\n".join(sec["html_blocks"])
        tokens = token_count(plain)
        if tokens <= TOKEN_LIMIT:
            sec["chunks"].append({"text": plain, "meta": build_meta(sec_name, 0, tokens)})
        else:
            words = plain.split()
            start = 0
            while start < tokens:
                end = min(start + TOKEN_LIMIT, tokens)
                chunk_txt = " ".join(words[start:end])
                sec["chunks"].append({"text": chunk_txt, "meta": build_meta(sec_name, start, end)})
                if end == tokens:
                    break
                start = end - TOKEN_OVERLAP
        log.info("%s | %s to %d chunks (%d tokens)", accession, sec_name, len(sec["chunks"]), tokens)

    parsed_doc = {
        "meta": {
            "ticker": ticker,
            "form_type": "4",
            "fiscal_year": int(year),
            "accession": accession,
            "source_url": link_map.get(accession, ""),
            "local_path": str(fp),
            "parsed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "sections": sections,
    }
    return parsed_doc, missing_sources

# I/O HELPERS 
def load_links(ticker: str) -> Dict[str, str]:
    lp = LINKS_DIR / ticker / "links.json"
    if not lp.exists():
        return {}
    data = json.loads(lp.read_text("utf-8"))
    out = {}
    for form, yearmap in data.get("links", {}).items():
        if form != "4":
            continue
        for yr, filings in yearmap.items():
            for full_key, url in filings.items():
                out[full_key.split("_")[-1]] = url  # key by accession
    return out

def enumerate_filings() -> Dict[str, Dict[str, List[pathlib.Path]]]:
    res: Dict[str, Dict[str, List[pathlib.Path]]] = defaultdict(lambda: defaultdict(list))
    for ticker_dir in FILINGS_DIR.iterdir():
        if not ticker_dir.is_dir():
            continue
        ticker = ticker_dir.name
        if TICKERS_TO_PROCESS and ticker not in TICKERS_TO_PROCESS:
            continue
        form_dir = ticker_dir / FORM_DIRNAME
        if not form_dir.exists():
            continue
        for year_dir in form_dir.iterdir():
            if year_dir.is_dir():
                res[ticker][year_dir.name].extend(year_dir.glob("*.xml"))
    return res

def print_counts(counts: Dict[str, Dict[str, List[pathlib.Path]]]):
    years = sorted({yr for d in counts.values() for yr in d})
    rows = [[tic] + [len(counts[tic].get(yr, [])) for yr in years] for tic in sorted(counts)]
    table = tabulate(rows, headers=["Ticker"] + years, tablefmt="github")
    print("\nDetected Form 4 files\n" + table)
    log.info("\nDetected Form 4 files\n%s", table)

# MAIN 
def main():
    counts = enumerate_filings()
    print_counts(counts)

    work: List[Tuple[str, str, pathlib.Path, Dict[str, str]]] = []
    for ticker, yearmap in counts.items():
        link_map = load_links(ticker)
        for year, files in yearmap.items():
            work.extend((ticker, year, fp, link_map) for fp in files)

    success = fail = 0
    missing_src_total: set[str] = set()

    with tqdm(total=len(work), desc="Processing Form 4s", unit="file", ncols=90) as bar:
        for ticker, year, fp, link_map in work:
            out_dir = OUT_DIR / ticker / FORM_DIRNAME / year
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                parsed, misses = parse_form4_xml(fp, link_map)
                (out_dir / f"{fp.stem}_chunks.json").write_text(
                    json.dumps(parsed, ensure_ascii=False, indent=2)
                )
                success += 1
                missing_src_total.update(misses)
            except Exception:
                fail += 1
                log.error("Error parsing %s\n%s", fp, traceback.format_exc())
            bar.update(1)

    if missing_src_total:
        log.warning("Missing SEC source URLs for: %s", ", ".join(sorted(missing_src_total)))

    if fail == 0:
        msg = f"\nFinished. All {success} Form 4 filings processed successfully."
    else:
        msg = f"\nFinished.  Successful: {success}   Failed: {fail}"
    print(msg)
    log.info(msg.strip())

if __name__ == "__main__":
    main()
