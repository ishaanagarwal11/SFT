from __future__ import annotations
import json, logging, os, pathlib, re, sys, traceback
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import faiss, numpy as np, requests
from tabulate import tabulate
from tqdm import tqdm

# USER CONFIG
TICKERS_TO_PROCESS = [
    "WMT","AMZN","UNH","AAPL","CVS","BRK.B","GOOGL","XOM",
    "MCK","COR","JPM","COST","CI","MSFT","CAH"
]
FORM_TYPES_TO_PROCESS = ["10-K","10-Q","8-K","3","4","5","DEF 14A"]

CHUNKS_DIR  = pathlib.Path(os.getenv("CHUNKS_DIR", "chunks"))
OLLAMA_URL  = os.getenv("OLLAMA_URL", "http://localhost:11434/api/embeddings") 
EMBED_MODEL = "nomic-embed-text"

INDEX_PATH        = pathlib.Path("faiss_index.index")
METADATA_PATH     = pathlib.Path("faiss_metadata.json")
LOG_PATH          = pathlib.Path("build_faiss_embeddings.log")
SKIPPED_JSON_PATH = pathlib.Path("skipped_chunks.json")
FAILED_JSON_PATH  = pathlib.Path("failed_chunks.json")

SAVE_SKIPPED_JSON = True
SAVE_FAILED_JSON  = True
MIN_CHARS         = 30    

# LOGGING
log = logging.getLogger("EmbedAllForms")
log.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s"))
fh.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
ch.setLevel(logging.WARNING)
log.addHandler(fh); log.addHandler(ch); log.propagate = False

# HELPERS
def env_override() -> None:
    """Allow env-vars to override ticker / form lists."""
    global TICKERS_TO_PROCESS, FORM_TYPES_TO_PROCESS
    if os.getenv("TICKERS_TO_PROCESS"):
        TICKERS_TO_PROCESS = os.getenv("TICKERS_TO_PROCESS").split(",")
    if os.getenv("FORM_TYPES_TO_PROCESS"):
        FORM_TYPES_TO_PROCESS = os.getenv("FORM_TYPES_TO_PROCESS").split(",")

def discover_files() -> Dict[str, Dict[str, Dict[str, List[pathlib.Path]]]]:
    """Return nested dict {form: {ticker: {year: [Path,…]}}}."""
    out = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for t_dir in CHUNKS_DIR.iterdir():
        if not t_dir.is_dir(): continue
        tic = t_dir.name
        if TICKERS_TO_PROCESS and tic not in TICKERS_TO_PROCESS: continue
        for f_dir in t_dir.iterdir():
            if not f_dir.is_dir(): continue
            form = f_dir.name
            if FORM_TYPES_TO_PROCESS and form not in FORM_TYPES_TO_PROCESS: continue
            for y_dir in f_dir.iterdir():
                if y_dir.is_dir():
                    out[form][tic][y_dir.name].extend(y_dir.glob("*_chunks.json"))
    return out

def print_counts(ct: Dict[str, Dict[str, Dict[str, List[pathlib.Path]]]]) -> None:
    """Pretty print file-count table per form."""
    for form in sorted(ct):
        years = sorted({y for t_map in ct[form].values() for y in t_map})
        rows = [[tic] + [len(ct[form][tic].get(y, [])) for y in years]
                for tic in sorted(ct[form])]
        print(f"\nDetected {form} *_chunks.json files")
        print(tabulate(rows, headers=[form] + years, tablefmt="github"))
        log.info("\nDetected %s files\n%s", form, tabulate(rows, headers=years))

def should_skip(text: str) -> bool:
    """Skip empty / short / non-alphanumeric."""
    return (not text.strip()) or len(text.strip()) < MIN_CHARS or not re.search(r"[A-Za-z0-9]", text)

def embed(text: str) -> List[float]:
    """Call Ollama embeddings endpoint."""
    resp = requests.post(
        OLLAMA_URL,
        json={"model": EMBED_MODEL, "prompt": text, "stream": False},
        timeout=90,
    )
    resp.raise_for_status()
    emb = resp.json().get("embedding")
    if not emb or len(emb) != 768:
        raise ValueError("Empty or invalid embedding returned")
    return emb

# MAIN
def main() -> None:
    env_override()
    counts = discover_files()
    if not counts:
        print("No matching *_chunks.json files under", CHUNKS_DIR)
        return
    print_counts(counts)

    # Flatten worklist [(form, ticker, Path)]
    work = [(form, tic, fp)
            for form, t_map in counts.items()
            for tic, y_map in t_map.items()
            for files in y_map.values()
            for fp in files]
    if not work:
        print("No files to process after filtering."); return

    vecs, meta = [], []; dim = None
    skipped_chunks = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # nested
    failed_chunks  = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    file_status, errors = {}, []

    with tqdm(total=len(work), desc="Embedding files", ncols=90, unit="file") as bar:
        for form, tic, fp in work:
            try:
                doc = json.loads(fp.read_text("utf-8"))
                for sec in doc.get("sections", {}).values():
                    for ch in sec.get("chunks", []):
                        txt = ch.get("text", "")
                        if should_skip(txt):
                            skipped_chunks[tic][form][fp.name].append(txt)
                            continue
                        try:
                            vec = np.array(embed(txt), dtype="float32")
                            if dim is None: dim = vec.size
                            elif vec.size != dim: raise ValueError("Dim mismatch")
                            vecs.append(vec)
                            meta.append({"text": txt, "meta": ch["meta"]})
                        except Exception:
                            failed_chunks[tic][form][fp.name].append(txt)
                file_status[fp.as_posix()] = "OK"
            except Exception:
                file_status[fp.as_posix()] = "FAIL"
                err = traceback.format_exc()
                errors.append(f"{fp} → {err.splitlines()[0]}")
                log.error("Error in %s\n%s", fp, err)
            finally:
                bar.update(1)

    # Build FAISS + metadata
    if vecs:
        index = faiss.IndexFlatL2(dim)
        index.add(np.vstack(vecs))
        faiss.write_index(index, str(INDEX_PATH))
        METADATA_PATH.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    log.info("Total vectors embedded: %s", len(vecs))

    # Persist skipped / failed chunks
    if SAVE_SKIPPED_JSON and skipped_chunks:
        SKIPPED_JSON_PATH.write_text(json.dumps(skipped_chunks, indent=2, ensure_ascii=False))
        log.info("Skipped chunks → %s", SKIPPED_JSON_PATH)
    if SAVE_FAILED_JSON and failed_chunks:
        FAILED_JSON_PATH.write_text(json.dumps(failed_chunks, indent=2, ensure_ascii=False))
        log.info("Failed chunks  → %s", FAILED_JSON_PATH)

    # Summary
    ok_files   = sum(1 for v in file_status.values() if v == "OK")
    fail_files = len(file_status) - ok_files
    idx_msg    = f"{len(vecs):,} vectors saved → {INDEX_PATH}" if vecs else "No vectors created."

    ticker_tbl = []
    for form in sorted(counts):
        for tic in sorted(counts[form]):
            expected = sum(len(files) for files in counts[form][tic].values())
            processed = sum(1 for (f, t, p) in work if f == form and t == tic and file_status[p.as_posix()] == "OK")
            status = "ALL OK" if processed == expected else f"{processed}/{expected} OK"
            ticker_tbl.append([form, tic, status])

    print("\n\nSummary")
    print(f"Files processed : {len(work)}")
    print(f"   • Success    : {ok_files}")
    print(f"   • Failed     : {fail_files}")
    print(idx_msg)
    print("\nPer-Form / Ticker status")
    print(tabulate(ticker_tbl, headers=["Form", "Ticker", "Status"], tablefmt="github"))

    if skipped_chunks:
        total_skipped = sum(len(v) for form in skipped_chunks.values() for file in form.values() for v in file.values())
        print(f"\nSkipped chunks: {total_skipped}")
    if failed_chunks:
        total_failed  = sum(len(v) for form in failed_chunks.values() for file in form.values() for v in file.values())
        print(f"Ollama failures: {total_failed}")
    if errors:
        print("\nErrors (condensed):")
        for e in errors: print(" •", e)

if __name__ == "__main__":
    main()
