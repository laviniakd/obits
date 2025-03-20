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
from obittools import parse_page_metadata_from_schemas_in_html


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--samplesize", type=int, help="number of IDs to sample per second")
parser.add_argument("-t", "--threads", type=int, help="number of threads to use")
parser.add_argument("-b", "--beginindex", type=int, help="linux timestamp to start sampling from")
parser.add_argument("-e", "--endindex", type=int, help="linux timestamp to end sampling at")
args = parser.parse_args()

sample_size, threads, begin_index, end_index = 50000, 16, 1, 58000000

if args.samplesize is not None:
    sample_size = args.samplesize
if args.threads is not None:
    threads = args.threads
if args.beginindex is not None:
    begin_index = args.beginindex
if args.endindex is not None:
    end_index = args.endindex

collection = f"test_results"
thread_local = threading.local()

# def get_driver(reset_driver=False):
#     """
#     manage selenium webdriver instances for each running thread
#     :param reset_driver:
#     :return:
#     """
#     print(f"reset_driver is {reset_driver}")
#     driver = getattr(thread_local, 'driver', None)
#     if reset_driver:
#         if driver is not None:
#             driver.quit()
#             sleep(5)
#         setattr(thread_local, 'driver', None)
#     if driver is None:
#         driver = Driver(uc=True, 
#                         headless=True, 
#                         binary_location=os.getenv("CHROME_BINARY"),
#                         no_sandbox=True,
#                         disable_gpu=True,
#                         disable_csp=True, 
#         )
#         print(driver)
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


#def extract_metadata(page_source):
#    soup = BeautifulSoup(page_source, "html.parser")
#    obituary_page_elements = soup.findAll("script", {"data-hypernova-key": "ObituaryPage"})
#    if len(obituary_page_elements) == 1:
#        metadata_json_string = obituary_page_elements[0].text
#        metadata = json.loads(metadata_json_string[4:-3])



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
    current_title, current_errormsg = "", ""

    print("Processing page")

    while tries < 4:
        try:
            driver = Driver(uc=True, headless=True, driver_version="/home/kyzheng/obitvenv313/lib/python3.13/site-packages/seleniumbase/drivers/chromedriver", binary_location=os.getenv("CHROME_BINARY"), no_sandbox=True, disable_gpu=True, disable_csp=True, remote_debug=False, use_wire=True,) # get_driver(reset_driver)
            print(driver)
            print("Got driver")
            print(url)
            driver.get(url)
            #driver.wait_for_attribute(selector='script', attribute='data-hypernova-key', value='ObituaryPage')
            # driver.wait_for_element(timeout=100)
            print("Got page")
            # WebDriverWait(driver, TIMEOUT).until(expected_conditions.presence_of_element_located((By.CSS_SELECTOR, "div#topContent2")))
            # driver.implicitly_wait(5)  # wait up to 5 secs just in case things don't load immediately?
            page_source, current_title, current_url = driver.page_source, driver.title, driver.current_url
            print("Got page source")
            print(page_source)

            """
            STRUCTURE:
            
            if not denied:
                if url is different:
                    save html, extract data
                elif url is same
                    discard (ID did not resolve)
            else: (denied)
                reset driver, repeat 5 times  
            """


            # TODO: what is the title of the page when we're denied?
            if not current_title == "Access Denied":  # this is when we're not blocked
                # current_status = driver.page_stat
                json_metadata_object, results_dict = parse_page_metadata_from_schemas_in_html(page_source)
                print(results_dict)
                driver.quit()
                exit()
            else:  # this is when we are blocked -- gotcha
                driver.quit()
                sleep(5)
                # setattr(thread_local, 'driver', None)
        except Exception as e:
            current_errormsg = str(e)
            # kevin there is sometimes a field in the legacy webpage with the data-component value "LifespanText". includes like 1940 - 2025
            # yes let's try to modify and look for this
            # modifying data collection code to look for this. word
            if "Message: invalid session id" not in current_errormsg:  # "invalid session id" error is fixed with a driver reset
                tqdm.write(f"{obit_id} {current_errormsg}")
            # driver = getattr(thread_local, 'driver', None)
            if driver is not None:
                driver.quit()
            # setattr(thread_local, 'driver', None)
            sleep(5)
        finally:
            tries += 1
            reset_driver = True
    tqdm.write(f"{obit_id} returning none")
    return {
        "id": str(obit_id),
        "url": url,
        "title": current_title,
        "statusCode": "ERROR",
        "statusMsg": current_errormsg,
    }


def main():
    URL = "https://www.legacy.com/us/obituaries/name/a-obituary?id=57711813"
    results = check_url((URL, "57711813"))
    print(results)


if __name__ == "__main__":
    main()