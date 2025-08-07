import streamlit as st
import os
import subprocess
from sft_data_fetch.gov_idx_download import download_idx_files
from sft_data_fetch.gov_idx_to_filings import download_filings
from sft_data_fetch.gov_filings_src_links import generate_links
from sft_parsers.sft_run_parser import run_all_parsers
from sft_embed_and_ask.sft_embed import embeddings
from sft_embed_and_ask.ollama_setup import run_ollama_commands
from sft_embed_and_ask.sft_qna import ask_question
from sft_embed_and_ask.clear_data import delete_data_folder

# Config Path
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

# CIK_MAP and Form Types
CIK_MAP ={
    "WMT": "0000104169", "AMZN": "0001018724", "UNH": "0000731766", "AAPL": "0000320193",
    "CVS": "0000064803", "BRK.B": "0001067983", "GOOGL": "0001652044", "XOM": "0000034088",
    "MCK": "0000927653", "COR": "0001355839", "JPM": "0000019617", "COST": "0000909832",
    "CI": "0001739940", "MSFT": "0000789019", "CAH": "0000721371"
}

TICKERS_LIST = list(CIK_MAP.keys())
FORM_TYPES_LIST = ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"]

# Check if /data folder exists
DATA_PATH = 'data'
FAISS_INDEX_PATH = os.path.join(DATA_PATH, 'faiss/faiss_index.index')

# Session state
if "file_status" not in st.session_state:
    st.session_state.file_status = None

EMAILS = [
    "downloader1@example.com", "downloader2@example.com", "downloader3@example.com",
    "downloader4@example.com", "downloader5@example.com", "downloader6@example.com",
    "downloader7@example.com", "downloader8@example.com", "downloader9@example.com", 
    "downloader10@example.com"
]

# App layout and title
st.title("SEC Analytics")

# Function to handle Data Processing Mode
def data_processing_mode():
    st.header("Fetch, Download, and Process")

    email_cycle_count = st.selectbox("Select number of emails to cycle through", [2, 5, 10], index=2, key="email_cycle_count")
    calls_per_email = st.selectbox("Select number of API calls per email", [10, 15, 18], index=1, key="calls_per_email")
    selected_tickers = st.multiselect("Select tickers to process", TICKERS_LIST, default=["WMT"], key="selected_tickers")
    selected_forms = st.multiselect("Select form types to process", FORM_TYPES_LIST, default=["10-K"], key="selected_forms")
    selected_years = st.multiselect("Select years to download .idx files", list(range(2018, 2026)), default=[2018], key="selected_years")
    RETRY_LIMIT = st.number_input('Retry Limit', min_value=1, max_value=10, value=3, key="retry_limit")
    RETRY_BACKOFF = st.selectbox('Retry Backoff (seconds)', [0.5, 0.8, 1.0], index=0, key="retry_backoff")

    # Process buttons
    if st.button("Download IDX", key="download_idx"):
        st.session_state.file_status = "Downloading IDX..."
        download_idx_files()
        st.info("Download complete!")

    if st.button("Download Filings", key="download_filings"):
        st.session_state.file_status = "Downloading filings..."
        download_filings()
        generate_links()
        st.info("Filing download complete! Links generated successfully!")
        
    if st.button("Run Parsers", key="run_parsers"):
        st.session_state.file_status = "Running parsers..."
        run_all_parsers()
        st.info("All parsers executed successfully!")

    if st.button("Embed Chunks", key="embed_chunks"):
        st.session_state.file_status = "Embedding chunks..."
        if run_ollama_commands():
            embeddings()
            st.info("Embedding complete!")

    # Save configuration settings
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

    if st.button("Clear Data and Reprocess", key="clear_data"):
        delete_data_folder()
        st.session_state.file_status = "Data folder cleared and ready for reprocessing."
        st.info("Data folder cleared and ready for reprocessing.")
        st.experimental_rerun()  # This will refresh the app after clearing data

# Function to handle Chat Mode
def chat_mode():
    st.header("Chat Mode")

    st.write("FAISS index file exists!")
    query = st.text_input("Enter your query:", "", key="query_input")

    if query:
        answer = ask_question(query)
        st.write(f"Answer: {answer}")

    if st.button("Clear Data and Reprocess", key="clear_data_chat"):
        delete_data_folder()
        st.session_state.file_status = "Data folder cleared and ready for reprocessing."
        st.info("Data folder cleared and ready for reprocessing.")
        st.experimental_rerun()  # This will refresh the app after clearing data

tabs = st.tabs(["Data Processing", "QnA"])

with tabs[0]:
    data_processing_mode()

with tabs[1]:
    if os.path.exists(FAISS_INDEX_PATH):
        # Check if gemma3 model is available
        st.info("Checking for 'gemma3' model...")
        result_list = subprocess.run(
            ["ollama", "list"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        available_models = result_list.stdout
        gemma_ok = "gemma3" in available_models

        if gemma_ok:
            st.info("'gemma3' is available. Start chat")
            chat_mode()
        else:
            st.info("'gemma3' not found. Installing...")
            subprocess.run(
                ["ollama", "pull", "gemma3"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            st.info("'gemma3' installed. You can now start chatting.")
            chat_mode()
    else:
        st.warning("FAISS index not found. Please complete the data processing first.")


# Custom CSS to position the clear data button at the bottom right
st.markdown(
    """
    <style>
    .css-1emrehx {
        position: fixed;
        bottom: 10px;
        right: 10px;
        z-index: 100;
    }
    </style>
    """,
    unsafe_allow_html=True
)
