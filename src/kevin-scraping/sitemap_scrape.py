from time import sleep
from seleniumbase import Driver


def main():
    urls = [f"http://www.legacy.com/us/memorials-sitemap-{generated_id}.xml" for generated_id in list(range(20, 0, -1))]
    for url in urls:
        print(url)
        driver = Driver(uc=True, incognito=True)  # , headless=True)
        driver.get(url)
        driver.wait_for_element_visible("#folder0")
        page_source, current_title, current_url = driver.page_source, driver.title, driver.current_url
        print(f"{current_title} {current_url}")
        with open(url.split("/")[-1], "w") as f:
            f.write(page_source)
        driver.quit()
        sleep(5)



if __name__ == "__main__":
    main()
