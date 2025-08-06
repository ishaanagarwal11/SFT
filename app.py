import streamlit as st
from sft_data_fetch.gov_idx_download import download_idx_files
from sft_data_fetch.gov_idx_to_filings import download_filings
from sft_data_fetch.gov_filings_src_links import generate_links
from itertools import cycle
import os

CONFIG_PATH = "config.py"

# Function to save user inputs to config.py
def save_config(config_data):
    with open(CONFIG_PATH, "w") as f:
        for key, value in config_data.items():
            f.write(f'{key} = {repr(value)}\n')

def load_config():
    config_data = {}
    if os.path.exists(CONFIG_PATH) and os.path.getsize(CONFIG_PATH) > 0: 
        with open(CONFIG_PATH, "r") as f:
            for line in f:
                if line.strip() and "=" in line: 
                    key, value = line.strip().split(" = ")
                    config_data[key] = eval(value) 
    return config_data

config_data = load_config()

CIK_MAP ={
    "WMT": "0000104169", "AMZN": "0001018724", "UNH": "0000731766", "AAPL": "0000320193",
    "CVS": "0000064803", "BRK.B": "0001067983", "GOOGL": "0001652044", "XOM": "0000034088",
    "MCK": "0000927653", "COR": "0001355839", "JPM": "0000019617", "COST": "0000909832",
    "CI": "0001739940", "MSFT": "0000789019", "CAH": "0000721371"
}

TICKERS_LIST = list(CIK_MAP.keys())
FORM_TYPES_LIST = ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"]

if "file_status" not in st.session_state:
    st.session_state.file_status = None

EMAILS = [
    "downloader1@example.com", "downloader2@example.com", "downloader3@example.com",
    "downloader4@example.com", "downloader5@example.com", "downloader6@example.com",
    "downloader7@example.com", "downloader8@example.com", "downloader9@example.com", 
    "downloader10@example.com"
]

st.title("SEC")
st.header("Fetch, Download, and Process")

email_cycle_count = st.selectbox(
    "Select number of emails to cycle through",
    [2, 5, 10], 
    index=0 
)

calls_per_email = st.selectbox(
    "Select number of API calls per email",
    [10, 15],  
    index=0 
)

selected_tickers = st.multiselect(
    "Select tickers to process",
    TICKERS_LIST,
    default=["WMT"] if "WMT" in TICKERS_LIST else []
)

selected_forms = st.multiselect(
    "Select form types to process",
    FORM_TYPES_LIST,
    default=["10-K"] 
)

selected_years = st.multiselect(
    "Select years to download .idx files",
    list(range(2018, 2026)),
    default=[2018]  
)

RETRY_LIMIT = st.number_input('Retry Limit', min_value=1, max_value=10, value=3)

RETRY_BACKOFF = st.selectbox(
    'Retry Backoff (seconds)',
    [0.5, 0.8, 1.0], 
    index=0 
)

def fetch_idx():
    progress_bar = st.progress(0)
    download_idx_files(progress_bar)  
    st.session_state.file_status = "Download complete!" 
if st.button("Download IDX"):
    fetch_idx()


def fetch_filings():
    progress_bar = st.progress(0)  
    download_filings(progress_bar)  
    st.session_state.file_status = "Filing download complete!"

    generate_links(progress_bar)
    st.session_state.file_status = "Links generated successfully!"
    

if st.button("Download Filings"):
    fetch_filings()


save_config({
    "CIK_MAP": CIK_MAP,
    "EMAILS": EMAILS, 
    "EMAILS_TO_USE": email_cycle_count,
    "CALLS_PER_EMAIL": calls_per_email,
    "SELECTED_TICKERS": selected_tickers,
    "SELECTED_FORMS": selected_forms,
    "SELECTED_YEARS": selected_years,
    "RETRY_LIMIT": RETRY_LIMIT,
    "SLEEP_TIME": RETRY_BACKOFF
})
