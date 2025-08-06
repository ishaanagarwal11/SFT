import os
from pathlib import Path
from config import (
    CIK_MAP, SELECTED_TICKERS,
    SELECTED_FORMS, SELECTED_YEARS
)
from .parser_10_K import parse10k
from .parser_10_Q import parse10q
from .parser_8_K import parse8k
from .parser_DEF_14A import parsedef14a
from .parser_3 import parse3
from .parser_4 import parse4
from .parser_5 import parse5

# Shared directories
FILINGS_DIR = "./data/filings"
LINKS_DIR = "./data/links"
OUT_DIR = "./data/chunks"

# ENV SETUP
env = os.environ.copy()
env["FILINGS_DIR"] = FILINGS_DIR
env["LINKS_DIR"] = LINKS_DIR
env["OUT_DIR"] = OUT_DIR

project_root = Path(__file__).parent.resolve()
env["PYTHONPATH"] = str(project_root)

def parser():
    """
    Download IDX files for the selected tickers and process them.
    """
    total_files_to_process = 0
    processed_files = 0

    selected_tickers = [ticker for ticker in SELECTED_TICKERS if ticker in CIK_MAP]

    for ticker in selected_tickers:
        for form in SELECTED_FORMS:
            for year in SELECTED_YEARS:
                form_dir = Path(FILINGS_DIR) / ticker / form / str(year)
                if form_dir.exists() and form_dir.is_dir():
                    files = list(form_dir.glob("*.htm")) 
                    total_files_to_process += len(files)

    print(f"Total files to process: {total_files_to_process}")

    # Process the files
    for ticker in selected_tickers:
        cik = CIK_MAP[ticker]

        for form in SELECTED_FORMS:
            for year in SELECTED_YEARS:
                form_dir = Path(FILINGS_DIR) / ticker / form / str(year)
                if form_dir.exists() and form_dir.is_dir():
                    files = list(form_dir.glob("*.htm"))
                    for filing_file in files:
                        try:
                            print(f"Processing file: {filing_file}")
                            with open(filing_file, 'r', encoding='utf-8') as file:
                                content = file.read()
                            
                            processed_files += 1
                        
                        except Exception as e:
                            print(f"Error processing file {filing_file}: {e}")
                            continue

    print(f"Processed {processed_files}/{total_files_to_process} filings.")

def run_all_parsers():
    """
    Run all SEC parsers and handle IDX file downloading and parsing.
    """
    print("\nLaunching all SEC parsers from ./parsers/\n")

    PARSER_FUNCTIONS = [
        parse3,
        parse4,
        parse5,
        parse8k,
        parse10k,
        parse10q,
        parsedef14a,
    ]

    parser()

    for parse_func in PARSER_FUNCTIONS:
        print(f"\n\nRunning: {parse_func.__name__} …")
        
        try:
            parse_func() 
            print(f"{parse_func.__name__} completed successfully")
        except Exception as e:
            print(f"Error: {parse_func.__name__} failed with error: {e}")

    print("All parser functions finished.")

if __name__ == "__main__":
    run_all_parsers()
