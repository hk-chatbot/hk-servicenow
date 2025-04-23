import time
from openai import RateLimitError

def get_embedding_with_retry(client, chunk, model, max_retries=5, delay=2):
    retries = 0
    while retries < max_retries:
        try:
            response = client.embeddings.create(input=chunk, model=model)
            return response.data[0].embedding
        except RateLimitError as e:
            print(f"[429] Rate limit hit. Retrying in {delay} sec... (Retry {retries + 1})")
            time.sleep(delay)
            retries += 1
            delay *= 2  # exponential backoff
        except Exception as e:
            print(f"[Error] Embedding failed: {e}")
            break
    return None  # Return None if all retries fail
