import subprocess
import sys

def run_ollama_commands() -> bool:
    try:
        # Check if ollama is installed by running `ollama --version`
        subprocess.run(["ollama", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Ollama is already installed.")
    except subprocess.CalledProcessError:
        # If ollama is not installed, install it via pip
        print("Ollama not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "ollama"])

    try:
        # Pull the nomic-embed-text model
        print("Pulling nomic-embed-text model...")
        result_pull = subprocess.run(
            ["ollama", "pull", "nomic-embed-text"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result_pull.stdout)
        if result_pull.stderr:
            print(result_pull.stderr)

        # List all available models
        print("\nListing available models...")
        result_list = subprocess.run(
            ["ollama", "list"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(result_list.stdout)
        if result_list.stderr:
            print(result_list.stderr)

        # Check if the model is listed
        is_available = "nomic-embed-text" in result_list.stdout
        if is_available:
            print("\nThe model 'nomic-embed-text' is available.")
        else:
            print("\nThe model 'nomic-embed-text' is not available.")
        return is_available

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
        print(f"Error output: {e.stderr}")
        return False
