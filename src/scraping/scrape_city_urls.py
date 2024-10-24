import pandas as pd
import argparse
from datetime import datetime as dt
from tqdm import tqdm
import time
import numpy as np

import requests
from bs4 import BeautifulSoup

from obits.src.scraping import misc_utils


def find_city_url(city, state, base_url):
    # replace any number of spaces in city with '-'
    city = city.replace(' ', '-')

    # load city + state page
    url = (base_url + f'{state}/{city}-area/').lower() # e.g. https://www.legacy.com/us/obituaries/local/illinois/chicago-area
    print("url: ", url)
    page = misc_utils.make_request(url)
    
    if page.status_code != 200:
        print(f"Did not find obit page for {city}, {state}")
        return False
    else:
        return url
    

def main(args):
    # load args
    output_path = args.output_path

    # get obituaries
    BASE_URL = 'https://www.legacy.com/us/obituaries/local/'
    cities = misc_utils.get_big_msas_and_states()
    city_url_list = []

    for city in cities:
        print("Processing city: ", city['city'])
        city_name = city['city']
        state = city['state']
        city_url = find_city_url(city_name, state, BASE_URL)
        if city_url:
            city_url_list.append({'url': city_url, 'city': city_name, 'state': state})

    city_urls = pd.DataFrame(city_url_list)
    city_urls.to_csv(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_path', type=str, default='/data/laviniad/obits/obit_urls.csv')
    args = parser.parse_args()

    main(args)
