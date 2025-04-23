import nltk

from langchain.text_splitter import RecursiveCharacterTextSplitter
def chunk_text(text: str, chunk_size: int = 1000, chunk_overlap: int = 200):

    """Chunk the text into smaller chunks using RecursiveCharacterTextSplitter."""
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = text_splitter.split_text(text)
    return chunks