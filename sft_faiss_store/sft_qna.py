import json
import pathlib
import faiss
import numpy as np
import requests
import logging
from typing import List, Dict


# CONFIGURATION
EMBED_MODEL = "nomic-embed-text"
GEN_MODEL = "gemma3"
OLLAMA_URL = "http://localhost:11434"

INDEX_PATH = pathlib.Path("faiss_index.index")
METADATA_PATH = pathlib.Path("faiss_metadata.json")

TOP_K = 50
MAX_CONTEXT_CHARS = 50000


# LOGGING SETUP
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


# HELPER FUNCTIONS
def embed_query(text: str) -> np.ndarray:
    """Embed the query using the `nomic-embed-text` model."""
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": text, "stream": False},
            timeout=30,
        )
        resp.raise_for_status()
        return np.array(resp.json()["embedding"], dtype="float32")
    except requests.exceptions.RequestException as e:
        log.error(f"Error embedding query: {e}")
        raise


def generate_answer(question: str, context: str) -> str:
    """Generate a detailed, insightful, and actionable answer using context and question."""
    
    prompt = f"""
    You are a highly skilled AI assistant capable of answering a wide range of questions with detailed, thoughtful, and actionable insights. Your goal is to provide answers that are:

    1. Thorough: Ensure you cover all relevant aspects of the question and provide a comprehensive answer.
    2. Structured: Organize your response in a clear, logical manner, making it easy for the user to understand.
    3. Insightful: Offer deep insights into the topic at hand, drawing from all relevant data and knowledge.
    4. Actionable: Where appropriate, provide recommendations or next steps that could be followed based on the answer.
    5. Context-Aware: Use the provided context to deliver a response that directly answers the question, even if the query is broad or high-level.

    HERE IS THE QUESTION AND CONTEXT:
    dont mention the word "context" in the answer, just use it to generate a good answer.
    any thing that you talk about in the answer, make sure to mention the company name that it is related to.

    f"\n\nContext:\n{context}\n\n"
    f"Question: {question}"

    """
    
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": GEN_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["response"].strip()
    except requests.exceptions.RequestException as e:
        log.error(f"Error generating answer: {e}")
        raise

def load_index_and_metadata() -> tuple[faiss.IndexFlatL2, List[Dict]]:
    """Load FAISS index and metadata from disk."""
    try:
        index = faiss.read_index(str(INDEX_PATH))
        with open(METADATA_PATH, encoding="utf-8") as f:
            metadata = json.load(f)
        return index, metadata
    except Exception as e:
        log.error(f"Error loading index or metadata: {e}")
        raise


def retrieve_context(query_vec: np.ndarray, index: faiss.IndexFlatL2, metadata: List[Dict]) -> str:
    """Retrieve top-k relevant chunks from the FAISS index and return them as context."""
    try:
        distances, indices = index.search(query_vec, TOP_K)
        retrieved = []
        sources = []
        for idx in indices[0]:
            if idx >= len(metadata):
                continue
            entry = metadata[idx]
            text = entry["text"].strip()
            url = entry["meta"].get("source_url", "Unknown source")
            retrieved.append(text)
            sources.append(url)
        
        context = "\n\n".join(retrieved)[:MAX_CONTEXT_CHARS]
        return context, sources
    except Exception as e:
        log.error(f"Error retrieving context: {e}")
        raise


def ask_question_rag(query: str) -> None:
    """Ask a question and return an answer using RAG."""
    log.info(f"Searching for: {query}")
    query_vec = embed_query(query).reshape(1, -1)

    try:
        index, metadata = load_index_and_metadata()
        context, sources = retrieve_context(query_vec, index, metadata)
        
        log.info("Generating answer with gemma3...")
        answer = generate_answer(query, context)
        
        log.info("\nAnswer:\n")
        print(answer)

        log.info("\nSources:")
        for i, url in enumerate(sources, 1):
            print(f"{i}. {url}")
    except Exception as e:
        log.error(f"Error during question answering: {e}")


# MAIN LOOP
if __name__ == "__main__":
    try:
        while True:
            q = input("\nAsk a question (or type 'exit'): ").strip()
            if q.lower() in {"exit", "quit"}:
                log.info("Exiting...")
                break
            if q:
                ask_question_rag(q)
    except KeyboardInterrupt:
        log.info("\nExiting...")
