import streamlit as st
from gov_idx_download import download_idx  # Import the download function

# Fixed list of emails
EMAILS = [
    "idx.downloader1@example.com", "idx.downloader2@example.com", "idx.downloader3@example.com",
    "idx.downloader4@example.com", "idx.downloader5@example.com", "idx.downloader6@example.com",
    "idx.downloader7@example.com", "idx.downloader8@example.com", "idx.downloader9@example.com", 
    "idx.downloader10@example.com"
]

# Set of available tickers and form types
TICKERS_LIST = ["WMT", "AMZN", "UNH", "AAPL", "CVS", "BRK.B", "GOOGL", "XOM", "MCK", "COR", "JPM", "COST", "CI", "MSFT", "CAH"]
FORM_TYPES_LIST = ["10-K", "10-Q", "8-K", "DEF 14A", "3", "4", "5"]

# Set up the app's initial state
if "file_status" not in st.session_state:
    st.session_state.file_status = None

# Streamlit Inputs for user to select options
st.title("SEC Index Fetcher")
st.header("Fetch and download SEC .idx files")

# Select emails to cycle through
email_cycle_count = st.selectbox(
    "Select number of emails to cycle through",
    [2, 5, 10],  # User can select how many emails to cycle through
    index=2  # Default to 10 emails
)

# Select the number of API calls per email
calls_per_email = st.selectbox(
    "Select number of API calls per email",
    [10, 15],  # User can select the calls per email
    index=1  # Default to 15 calls
)

# Select tickers to process
selected_tickers = st.multiselect(
    "Select tickers to process",
    TICKERS_LIST,
    default=TICKERS_LIST  # Default to all tickers
)

# Select form types to process
selected_forms = st.multiselect(
    "Select form types to process",
    FORM_TYPES_LIST,
    default=FORM_TYPES_LIST  # Default to all forms
)

# Retry settings
RETRY_LIMIT = st.number_input('Retry Limit', min_value=1, max_value=10, value=3)
RETRY_BACKOFF = st.number_input('Retry Backoff (seconds)', min_value=1, max_value=5, value=1)

# Define YEARS here
YEARS = list(range(2018, 2026))  # The years to fetch the .idx files for

# Function to call the download_idx and update progress
def fetch_idx_files():
    st.session_state.file_status = "Downloading .idx files..."
    progress_bar = st.progress(0)  # Initialize the progress bar

    # Select the emails based on user input
    selected_emails = EMAILS[:email_cycle_count]  # Use the first `email_cycle_count` emails from the list

    for progress in download_idx(YEARS, RETRY_LIMIT, RETRY_BACKOFF, selected_emails, calls_per_email):
        progress_bar.progress(progress)  # Update progress bar in Streamlit

    st.session_state.file_status = "Index files downloaded successfully!"
    st.success("All .idx files have been successfully downloaded!")

# Trigger the download
if st.button("Download .IDX Files"):
    fetch_idx_files()

# Display status or logs
if st.session_state.file_status:
    st.write(st.session_state.file_status)
    st.text_area("Log Output", value=f"Fetching files... See progress bar.", height=200)
