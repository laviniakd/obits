import json
import collections
from bs4 import BeautifulSoup

all_urls = []

regex


for i in range(1, 21):
    print(i)
    print(len(all_urls))
    with open(f"memorials-sitemap-{i}.xml", "rb") as f:
        soup = BeautifulSoup(f, features="html")
    elements = soup.find_all("url")
    for element in elements:
        all_urls.append({
            "url": element.find_all("loc")[0].get_text().replace("-memorial?id=", "-obituary?id="),
            "timestamp": element.find_all("lastmod")[0].get_text()
        })

with open("all_urls.json", "w") as f:
    json.dump(all_urls, f, indent=4)\

only_urls = [all_url["url"] for all_url in all_urls]
print([item for item, count in collections.Counter(only_urls).items() if count > 1])
print(len(only_urls))
only_urls = list(set(only_urls))
print(len(only_urls))

with open("only_urls.json", "w") as f:
    json.dump(only_urls, f, indent=4)

"""
delete
\u201c
\u201d

\u00a0\u00a0 - replace with dash



"""
