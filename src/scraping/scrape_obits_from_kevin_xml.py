import pandas as pd
import json
import os
import multiprocessing
from functools import partial
import random
from tqdm.contrib.concurrent import process_map

from src.load_obit_from_url import load_obit_text_and_metadata


DEBUG = False


def load_urls_from_json(url_path):
    with open(url_path, 'r') as f:
        all_urls = json.load(f)

    return all_urls


def process_url(out_dir, o, DEBUG=False):
    """
    Process a single URL and save its obit text and metadata.
    
    Args:
        out_dir (str): Output directory for saving JSON files
        o (dict): Dictionary containing URL and timestamp
    
    Returns:
        tuple: (success, url, error_info)
    """
    url = o['url']
    # Load obit text and metadata
    obit_text_and_metadata = load_obit_text_and_metadata(url, DEBUG=DEBUG)
        
    try:
        if obit_text_and_metadata:
                # Add timestamp to metadata
            obit_text_and_metadata['timestamp_from_scrape'] = o['timestamp']
                
                # Save successful obit
            output_path = os.path.join(out_dir, f"{obit_text_and_metadata['id']}.json")
            with open(output_path, 'w') as f:
                json.dump(obit_text_and_metadata, f)
                
            return (True, url, None)
        else:
            # Failed to load obit
            return (False, url, "Failed to load obit")
    
    except Exception as e:
        # Catch any unexpected errors
        return (False, url, str(e))

def parallel_obit_scraper(all_urls, out_dir, num_workers=None, debug=False):
    """
    Parallelize obit scraping with progress tracking.
    
    Args:
        all_urls (list): List of URL dictionaries
        out_dir (str): Output directory for saving files
        num_workers (int, optional): Number of parallel workers. 
                                     Defaults to CPU count if not specified.
    """

    print("Beginning parallel obit scraping...")

    # Ensure output directory exists
    os.makedirs(out_dir, exist_ok=True)
    
    # Open failed URLs log file
    failed_urls_path = os.path.join(out_dir, 'failed_urls.txt')
    
    # Use default number of CPU cores if not specified
    if num_workers is None:
        num_workers = multiprocessing.cpu_count()

    # Try scraping one test URL
    print("Testing URL: ", all_urls[0]['url'])
    test_url = all_urls[0]['url']
    test_result = process_url(out_dir, all_urls[0], DEBUG=debug)
    if not test_result[0]:
        raise Exception(f"Failed to scrape test URL: {test_url}")
    
    print("Test URL scraped successfully!")

    print("Scraping full list of URLs...")

    # Use process_map for progress tracking
    results = process_map(
        partial(process_url, out_dir), 
        all_urls, 
        max_workers=num_workers, 
        chunksize=1
    )
    
    # Log failed URLs
    with open(failed_urls_path, 'a') as f:
        for success, url, error in results:
            if not success:
                f.write(f"{url}: {error}\n")

    print("Done!")

# Example usage
if __name__ == '__main__':
    # Assuming load_obit_text_and_metadata is defined elsewhere
    OUT_DIR = "/data/laviniad/obits/kevin-obits/"
    URL_PATH = "/data/laviniad/obits/all_urls.json"
    FAILED_URLS_PATH = os.path.join(OUT_DIR, 'failed_urls.txt')
    NUM_WORKERS = 16

    print("Loading URLs...")
    all_urls = load_urls_from_json(URL_PATH)
    print("Loaded JSON URLs!")
    failed_urls = []
    if os.path.exists(FAILED_URLS_PATH):
        with open(FAILED_URLS_PATH, 'r') as f:
            failed_urls = [line.split(':')[0] for line in f.readlines()]
    print(f"Loaded {len(failed_urls)} failed URLs.")

    #all_urls = [o for o in all_urls if str(o['url']) not in failed_urls]

    print(f"Scraping {len(all_urls)} URLs.")

    if DEBUG:
        print("Running in debug mode. Limiting to 10 URLs.")
        all_urls = random.sample(all_urls, 10)
        NUM_WORKERS = 2

    parallel_obit_scraper(all_urls, OUT_DIR, num_workers=NUM_WORKERS, debug=DEBUG)
