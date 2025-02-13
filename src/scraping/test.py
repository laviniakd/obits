import time
from seleniumbase import Driver
import os

driver = Driver(uc=True, incognito=True, binary_location=os.getenv("CHROME_BINARY"))#, chromium_path="~/chrome/chrome")  # , headless=True)
driver.get("https://www.legacy.com/us/obituaries/name/merle-cullers-obituary?id=57278740")

time.sleep(5)  # Give extra time for dynamic content
print("Page title:", driver.title)
print(driver.page_source)  # Check if #obituary is even present
