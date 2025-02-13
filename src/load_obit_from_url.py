from src.scraping import misc_utils
from bs4 import BeautifulSoup

import sys
import os

# experimenting with less aggressive method
from time import sleep
from seleniumbase import Driver

binary_location=os.getenv("CHROME_BINARY")
print(f"Using Chromium binary location: {binary_location}")

use_driver_to_find_elements = True


def load_obit_text_and_metadata(obit_url, DEBUG=False):
    #print("Loading driver...")
    #driver = Driver(uc=True, incognito=True)  # , headless=True)
    driver = Driver(uc=True, binary_location=binary_location, headless=True, incognito=True)  # , headless=True)
    #print("Loaded driver!")
    # get page
    #page = misc_utils.make_request(obit_url)
    driver.get(obit_url)
    #print("Got URL!")
    #driver.wait_for_ready_state_complete()
    #page_source = driver.page_source
    #print("Got page!")
    try:
        driver.wait_for_element("#obituary", timeout=3)
        #print("Found element!")
    except Exception as e:
        print(f"Error: {e}")
        if DEBUG:
            # save source to file
            with open('debug_page_source.html', 'w') as f:
                f.write(page_source)
                print("Saved debug page source to file: debug_page_source.html")

        driver.quit()
        return None

    page_source, _, _ = driver.page_source, driver.title, driver.current_url
    obit_data = {}

    if use_driver_to_find_elements:
        try:
            # find element where 'data-component' = 'NameHeadingText'
            name_heading = driver.find_element("[data-component='NameHeadingText']")
            name = name_heading.text

            text_div = driver.find_elements('div[data-component="ObituaryText"]')
            if text_div:  # First kind of page
                text = text_div[0].text
            else:
                # If 'ObituaryText' is not found, look for 'ObituaryParagraph'
                text_div = driver.find_elements('div[data-component="ObituaryParagraph"]')
                if text_div:
                    text = text_div[0].text
                else:
                    text = None

            # find element where 'data-component' = 'AttributeValuesBox'
            attribute_box = driver.find_elements('div[data-component="AttributeValuesBox"]')
            try:
                funeral_home_link_element = driver.find_element('a[data-component*="AttributeValueText"]')
                funeral_home = funeral_home_link_element.text
                funeral_home_link = funeral_home_link_element.get_attribute("href")
            except Exception as e:
                funeral_home = "unknown"
                funeral_home_link = "unknown"

            # Extract the address lines
            try:
                address_elements = driver.find_elements('p[data-component*="AttributeValueText"]')
                funeral_location = "\n".join([element.text for element in address_elements])
            except Exception as e:
                funeral_location = "unknown"
        except Exception as e:
            print(f"Error finding elements: {e}")
            print("URL: ", obit_url)
            driver.quit()
            return None

    else:
        soup = BeautifulSoup(page_source, 'html.parser')
    
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
        funeral_home = 'unknown'
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

    obit_data['id'] = int(obit_url.split('=')[-1])
    obit_data['name'] = name
    obit_data['url'] = obit_url
    obit_data['text'] = text
    obit_data['funeral_home_name'] = funeral_home
    obit_data['funeral_home_link'] = funeral_home_link
    obit_data['funeral_location'] = funeral_location

    return obit_data