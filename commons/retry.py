import time
import random
import math
from ..kafka_setup import logger

# Setup logger
logger = logger.setup_logger(__name__)

# --- Configuration Constants ---
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 1  # Starting delay for the first retry
JITTER_FACTOR = 0.5     # Jitter can be up to 50% of the calculated delay

# --- Simulated API Call ---
# This function simulates an API call that fails a certain number of times
# before succeeding.
ATTEMPTS_FAILED = 0
SUCCESS_AFTER_ATTEMPT = 3

def simulated_api_call(attempt: int, max_attempts: int):
    """Simulates an API call with transient failure."""
    global ATTEMPTS_FAILED
    
    if ATTEMPTS_FAILED < SUCCESS_AFTER_ATTEMPT:
        ATTEMPTS_FAILED += 1
        # Use a random probability for failure in later attempts for better simulation
        if ATTEMPTS_FAILED <= SUCCESS_AFTER_ATTEMPT:
            logger.error(f"API Failure! Attempt {attempt}/{max_attempts} failed.")
            raise ConnectionError("Simulated server overload or transient network issue.")
    
    logger.info(f"API Success! Attempt {attempt}/{max_attempts} succeeded.")
    return {"status": "ok", "data": "Request processed."}

# --- Retry Logic with Exponential Backoff and Jitter ---

def retry_with_backoff_and_jitter(func, *args, **kwargs):
    """
    Attempts to call a function using an exponential backoff strategy 
    with added jitter for delay.
    """
    for attempt in range(MAX_RETRIES):
        try:
            # 1. Attempt the function call
            return func(attempt + 1, MAX_RETRIES, *args, **kwargs)

        except Exception as e:
            # If this was the final attempt, re-raise the error
            if attempt == MAX_RETRIES - 1:
                logger.error(f"All {MAX_RETRIES} attempts failed. Giving up.")
                raise e

            # 2. Calculate Exponential Backoff Delay (D)
            # D = BASE_DELAY * 2^(attempt)
            exponential_delay = BASE_DELAY_SECONDS * (2 ** attempt)
            
            # 3. Calculate Jitter (R)
            # R = random(0, exponential_delay * JITTER_FACTOR)
            # This adds a random delay up to 50% of the exponential delay
            jitter = random.uniform(0, exponential_delay * JITTER_FACTOR)

            # 4. Determine total sleep time (T)
            sleep_time = exponential_delay + jitter
            
            logger.info(f"Retrying in {sleep_time:.2f} seconds (Base: {exponential_delay:.2f}s, Jitter: {jitter:.2f}s)")
            
            # 5. Wait
            time.sleep(sleep_time)

# --- Main Execution ---

if __name__ == "__main__":
    logger.info(f"Starting API Retry Test (Max Retries: {MAX_RETRIES})")
    
    try:
        result = retry_with_backoff_and_jitter(simulated_api_call)
        logger.info(f"Final Result: {result}")
    except ConnectionError as e:
        logger.error(f"Operation failed after retries: {e}")
