import pandas as pd
import argparse
from datetime import datetime as dt
from tqdm import tqdm
import time
import numpy as np
import re

import requests
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random, retry_if_exception_type

from obits.src.scraping import misc_utils


DEBUG = False

USER_AGENT_STRING = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
# example legacy api url: https://www.legacy.com/api/_frontend/localmarket/jacksonville-fl?endDate=2024-09-10&limit=300&noticeType=all&offset=600&sortBy=date&startDate=2023-09-10


def load_obit_text_and_metadata(obit_url):
    # get page
    page = misc_utils.make_request(obit_url)
    #print(page.content)
    soup = BeautifulSoup(page.content, 'html.parser')

    obit_data = {}

    # get features
    name = soup.find('h2', {'data-component': 'NameHeadingText'})
    if name:
        name = name.text
    else:
        name = 'unknown'

    # note that text is wrapped in <p> tags
    text = soup.find('div', {'data-component': 'ObituaryText'})
    if text: # first kind of page
        text = text.text
    else:
        text = soup.find('div', {'data-component': 'ObituaryParagraph'})
        if text:
            text = text.text
        else:
            return None
    
    attribute_box = soup.find('div', {'data-component': 'AttributeValuesBox'})
    if not attribute_box:
        e = soup.find('a', {'data-component': 'FuneralHomeDirectoryLink'})
        if e:
            funeral_home_link = e.text
            funeral_location = soup.find('p', {'data-component': 'MemorialEventsFuneralHomeAddress'}).text
        else:
            funeral_home_link = 'unknown'
            funeral_location = 'unknown'
    else:
        a_els = attribute_box.find_all('a')
        if a_els:
            funeral_home = attribute_box.find_all('a')[0].text
            funeral_home_link = attribute_box.find_all('a')[0]['href']
            p_els = attribute_box.find_all('p')
            funeral_location = '\n'.join([e.text for e in attribute_box.find_all('p')])
        else:
            funeral_home_link = 'unknown'
            funeral_location = 'unknown'

    obit_data['name'] = name
    obit_data['url'] = obit_url
    obit_data['text'] = text
    obit_data['funeral_home_link'] = funeral_home_link
    obit_data['funeral_location'] = funeral_location

    return obit_data


def get_obit_urls_from_city_section(soup, page):
    obit_urls = []

    for a in soup.find_all('a', {'data-component': 'PersonCardBoxLink'}):
        obit_urls.append(a['href'])
        
    # find button by picking button where text is just == page
    button_list = soup.find_all('button')
    for b in button_list:
        if b.text == str(page + 1):
            button = b

    return obit_urls, button


def extract_num_obits(text):
    return int(text.split(' ')[0].replace(',', ''))
    

async def get_obit_urls_from_city_url(browser, url, city, state):
    state_dict = misc_utils.get_reverse_state_dict()
    REGEX = r"https://www\.legacy\.com/us/obituaries/(?:[^/]+/)?name/[^/]+-obituary\?id=\d+"
    #page = misc_utils.make_request(url)

    page = await browser.newPage()
    await page.goto(url)
    obit_urls = []

    soup = BeautifulSoup(page.content, 'html.parser')

    # get element with data-component='ObituariesTotalResults' and get text
    total_results = soup.find('p', {'data-component': 'ObituariesTotalResults'})
    if not total_results:
        return []
    num_obits = extract_num_obits(total_results.text)
    num_pages = num_obits // 50

    # wait for button
    await page.waitForSelector('div[class*="ObituaryListPaginator"]', {'visible': True})

    # Function to click a button with a specific number
    def click_page_button(number):
        try:
            button_selector = f'div[class*="ObituaryListPaginator"] button:not([disabled]):not([aria-current="true"]):has-text("{number}")'
            page.waitForSelector(button_selector, {'visible': True})
            page.click(button_selector)
            
            # suggests it's done
            page.waitForSelector('div[class*="ObituaryListPaginator"]', {'visible': True})

            print(f"Clicked button {number}")

            obit_url_results = get_obit_urls_from_city_section(page)
            return obit_url_results
        except:
            print(f"Failed to click button {number}")
            return None

    for page_num in range(1, num_pages + 1):
        obit_url_results = click_page_button(str(page_num))
        obit_urls.extend(obit_url_results)
        asyncio.sleep(0.6)

    # API-using code
    #obit_urls = scrape_obituary_urls_using_api(url, REGEX, num_obits, city, state_dict[state])

    return obit_urls, browser


# seeing things like "https://www.legacy.com/api/_frontend/localmarket/el-paso-tx?endDate=2024-09-16&limit=50&noticeType=obituary&offset=100&sortBy=date&startDate=2023-09-16"
# but get urls like "https://www.legacy.com/us/obituaries/local/texas/el-paso-area/"
def scrape_obituary_urls_using_api(url, REGEX, num_obits, city, state):
    url = 'https://www.legacy.com/api/_frontend/localmarket/' + city.replace(' ', '-').lower() + '-' + state.lower()

    START_DATE = '2020-01-01'
    END_DATE = '2024-09-01'
    NUM = 50

    for i in range(0, num_obits, NUM):
        req_text = call_legacy_api(url + f'?endDate={END_DATE}&limit={NUM}&noticeType=all&offset={i}&sortBy=date&startDate={START_DATE}')

        # use regex to find urls
        obit_urls = re.findall(REGEX, req_text)
        obit_urls.extend(obit_urls)
    return obit_urls


def return_none_on_failure(retry_state):
    return None


# taken from josh's strategy for ballotpedia
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2),
    retry_error_callback=return_none_on_failure,
    retry=retry_if_exception_type(requests.RequestException)
)
def call_legacy_api(url):
    global USER_AGENT_STRING

    print("in legacy api call sitch")

    init_response = misc_utils.make_request(url, user_agent=USER_AGENT_STRING)
    init_response.raise_for_status()
    print("init response: " + str(init_response.status_code))

    if init_response.status_code == 429:
        rand = np.random.randint(0, 10)
        # replace 10_15_[digit] with 10_15_rand using regex
        USER_AGENT_STRING = re.sub(r"10_15_\d", f"10_15_{rand}", USER_AGENT_STRING)

    return init_response.text


async def main(args): 
    # load args
    day = dt.now().strftime('%Y-%m-%d')
    start_date = dt.strptime(args.start_date, '%Y-%m-%d')
    end_date = dt.strptime(args.end_date, '%Y-%m-%d')
    output_dir = args.output_dir
    url_list = args.city_url_list

    # load browser
    browser = await launch(headless=True, options={'args': ['--no-sandbox', '--headless', '--disable-gpu']})

    # get obituaries
    BASE_URL = 'https://www.legacy.com/us/obituaries/local/'
    obit_data = []
    city_urls = pd.read_csv(url_list)

    for idx, row in city_urls.iterrows():
        city = row['city']
        state = row['state']
        url = row['url']

        print("Processing URL: ", url)
        obit_urls, browser = await get_obit_urls_from_city_url(browser, url, city, state)
        for o in tqdm(obit_urls):
            obit_text_and_metadata = load_obit_text_and_metadata(o)
            if obit_text_and_metadata:
                obit_text_and_metadata['city'] = city
                obit_text_and_metadata['state'] = state
                obit_text_and_metadata['date_scraped'] = day
                obit_text_and_metadata['id'] = int(obit_text_and_metadata['url'].split('id=')[1])
                obit_data.append(obit_text_and_metadata)
                if DEBUG:
                    print("Successful first try")
                    exit()

    # save data
    if len(obit_data) == 0:
        print("No obits found.")
        return
    df = pd.DataFrame(obit_data)
    df.to_json(output_dir + 'obits.json', index=False, orient='records')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--city_url_list', type=str, default='/data/laviniad/obits/obit_urls.csv')
    parser.add_argument('--output_dir', type=str, default='/data/laviniad/obits/scraped_obits/')
    parser.add_argument('--start_date', type=str, default='1900-01-01', help='YYYY-MM-DD format')
    parser.add_argument('--end_date', type=str,default='2025-01-01', help='YYYY-MM-DD format')
    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(main(args))