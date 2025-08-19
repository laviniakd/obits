import os
import json
import random
import argparse
import threading
from tqdm import tqdm
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from seleniumbase import Driver
from concurrent.futures import ThreadPoolExecutor, as_completed
from obittools import ROOT_DIR, initialize_collection
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


from obittools.extract_data import parse_page_metadata_from_schemas_in_html


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--samplesize", type=int, help="number of IDs to sample")
parser.add_argument("-t", "--threads", type=int, help="number of threads to use")
parser.add_argument("-b", "--beginindex", type=int, help="linux timestamp to start sampling from")
parser.add_argument("-e", "--endindex", type=int, help="linux timestamp to end sampling at")
args = parser.parse_args()

sample_size, threads, begin_index, end_index = 50000, 2, 1, 60000000

if args.samplesize is not None:
    sample_size = args.samplesize
if args.threads is not None:
    threads = args.threads
if args.beginindex is not None:
    begin_index = args.beginindex
if args.endindex is not None:
    end_index = args.endindex

current_time = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
collection = f"random_legacy_{current_time}"



# def get_driver(reset_driver=False):
#     """
#     manage selenium webdriver instances for each running thread
#     :param reset_driver:
#     :return:
#     """
#     # print(f"reset_driver is {reset_driver}")
#     thread_local = threading.local()
#     driver = getattr(thread_local, 'driver', None)
#     if reset_driver:
#         if driver is not None:
#             driver.quit()
#             sleep(5)
#         setattr(thread_local, 'driver', None)
#     if driver is None:
#         driver = Driver(uc=True, page_load_strategy='eager', guest_mode=True, do_not_track=True, headless2=True)  # , headless=True, binary_location=os.getenv("CHROME_BINARY"))
#     setattr(thread_local, 'driver', driver)
#     return driver

# example: https://www.legacy.com/us/obituaries/charlotte/name/david-melton-obituary?id=57552782
def build_url(id, base = "https://www.legacy.com", infix = "/us/obituaries/name/a-obituary?id="):
    """
    :param id: id of obituary
    :return: formatted url string
    """
    base, infix = base.strip(), infix.strip()
    return f"{base}{infix}{id}"


def extract_metadata(page_source):
    soup = BeautifulSoup(page_source, "html.parser")
    obituary_page_elements = soup.findAll("script", {"data-hypernova-key": "ObituaryPage"})
    if len(obituary_page_elements) == 1:
        metadata_json_string = obituary_page_elements[0].text
        metadata = json.loads(metadata_json_string[4:-3])

    return metadata


def already_scraped(obit_id):
    """
    Check if obit_id has already been scraped
    :param obit_id: id of obituary
    :return: bool
    """
    return os.path.exists(os.path.join(ROOT_DIR, "collections", collection, "metadata", f"{obit_id}.json"))



def check_url(url_tuple):
    """
    Check if url exists
    :param url_tuple: url and obit_id
    :return: dict: {"id": video id, "url": page url, "title": page title, "statusCode": status code in response,
                    "statusMsg": status message in response}
    """
    url = url_tuple[0]
    obit_id = url_tuple[1]

    TIMEOUT = 20

    tries, reset_driver = 0, False
    current_title, current_errormsg, current_url = "", "0", ""

    while tries < 5:
        try:
            sleep(random.random()*10)
            driver = Driver(uc=True, page_load_strategy='eager', guest_mode=True, do_not_track=True)  # , headless2=True)  # get_driver(reset_driver)
            print()
            print(f"Getting URL: {url}")
            wait = WebDriverWait(driver, TIMEOUT)
            driver.get(url)
            wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div[1]')))
            page_source, current_title, current_url = driver.page_source, driver.title, driver.current_url
            driver.quit()

            if "502" in current_title and "bad gateway" in current_title.lower():
                raise Exception("502: bad gateway")
            if f"/a-obituary?id={obit_id}" in current_url:
                raise Exception("no redirect")

            elif not "obituaries/search?firstName=a&lastName=obituary" in current_url:
                if ("?pid=" in current_url or "?id=" in current_url):
                    print()
                    print(f"Success: {current_url} {current_title}")
                    with open(os.path.join(ROOT_DIR, "collections", collection, "metadata", f"{obit_id}_obit.html"),
                              "w") as f:
                        f.write(page_source)
                    if str(obit_id) not in current_url:
                        current_errormsg = current_url.split("?")[-1]
                    else:
                        current_errormsg = "0"

                    return {
                        "id": str(obit_id),
                        "url": current_url,
                        "title": current_title,
                        "statusCode": "0",
                        "statusMsg": current_errormsg,
                    }
            current_errormsg = "redirect, no obituary id/pid"
            break
        except Exception as e:
            current_errormsg = str(e)
            if "Message: invalid session id" not in current_errormsg:  # "invalid session id" error is fixed with a driver reset
                tqdm.write(f"{obit_id} {current_errormsg}")
            # thread_local = threading.local()
            # driver = getattr(thread_local, 'driver', None)
            # if driver is not None:
            #     driver.quit()
            # setattr(thread_local, 'driver', None)
            sleep(5)
        finally:
            tries += 1
            reset_driver = True
    print(f"{obit_id} returning none")
    return {
        "id": str(obit_id),
        "url": current_url,
        "title": current_title,
        "statusCode": "ERROR",
        "statusMsg": current_errormsg,
    }


def main():
    print(initialize_collection(collection))
    all_ids_logged = []

    round = 0
    while True:
        all_ids = random.sample(range(begin_index, end_index), sample_size)

        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{current_time}_{sample_size}_{round}_queries.json"), "w") as f:
            json.dump(all_ids, f)
        with tqdm(total=len(all_ids)) as pbar:
            with ThreadPoolExecutor(max_workers=threads) as executor:
                results = []
                futures = [executor.submit(check_url, (build_url(generated_id), generated_id)) for
                           generated_id in all_ids]

                for future in as_completed(futures):
                    if future.result()["statusCode"] == "0":
                        # tqdm.write(json.dumps(future.result()))
                        all_ids_logged.append(future.result()["id"])
                        # tqdm.write("{:b}".format(int(future.result()["id"])).zfill(64))
                    results.append(future.result())
                    pbar.update(1)
        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{current_time}_{sample_size}_{round}_hits.json"), "w") as f:
            json.dump(results, f)

        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", f"{current_time}_{sample_size}_{round}_ids.json"), "w") as f:
            json.dump(all_ids_logged, f)
        round += 1


if __name__ == "__main__":
    main()