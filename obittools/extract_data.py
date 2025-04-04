import json
from bs4 import BeautifulSoup # check?


# key: personSchema
# values: country, state, city, zip code, birthdate, deathdate, familyname, givenname
# key: organizationSchema
# values: related to funeral home
# key: newsArticleSchema
# values: [comments] (text, date, author)
# value: articleBody is obituary
# key: [guestbook][entries]
# values: this is the good stuff (guestbook entries)
# key: eventSchemas
# values: related to funeral event


def get_schema_section(html_text):
    soup = BeautifulSoup(html_text, 'html.parser')
    json_schemas = soup.find('script', {'data-hypernova-key': 'ObituaryPage'})
    if json_schemas:
        json_schemas = soup.find('script', {'data-hypernova-key': 'ObituaryPage'})

        json_schemas = json_schemas.text[4:-3]
        json_schemas = json.loads(json_schemas)
        return json_schemas
    elif '<!--REDUX DATA-->' in html_text:
        # split by REDUX DATA and VIDDLER; <script> element between them is metadata JSON
        try:
            json_schemas = html_text.split('<!--REDUX DATA-->')[1].split('<!--VIDDLER-->')[0]
            json_schemas = json.loads(json_schemas.strip())
            return json_schemas
        except (IndexError, json.JSONDecodeError) as e:
            # handle cases where the split fails or JSON is malformed
            print(f"Error parsing JSON from HTML: {e}")
            return None
    else:
        return None


def parse_page_metadata_from_schemas_in_html(page_html):
    json_metadata_object = get_schema_section(page_html)
    if not json_metadata_object:
        print(page_html)
        return None, None

    results_dict = {}

    #guestbook_section = json_metadata_object["guestbook"]
    # if "schemas" in json_metadata_object.keys() and "personSchema" in json_metadata_object["schemas"].keys():
    #     person_details = json_metadata_object["schemas"]["personSchema"]
    #     for key in ["address", "birthDate", "deathDate", "familyName", "givenName"]:
    #         if key in person_details.keys():
    #             results_dict[key] = person_details[key]
    # address = person_details[]
    # birth_date = person_details[]
    # death_date = person_details[]
    # family_name = person_details[]
    # givenName = person_details[]
    # official_name = person_details["name"]

    # results_dict.update({
    #     "address": address,
    #     "birth_date": birth_date,
    #     "death_date": death_date,
    #     "family_name": family_name,
    #     "given_name": givenName,
    #     "official_name": official_name
    # })

    # organization_schema = json_metadata_object["schemas"]["organizationSchema"]
    # funeral_home_address = organization_schema["address"]
    # funeral_home_name = organization_schema["name"]
    # funeral_home_phone = organization_schema["telephone"]
    # funeral_home_url = organization_schema["url"]

    # results_dict.update({
    #     "funeral_home_address": funeral_home_address,
    #     "funeral_home_name": funeral_home_name,
    #     "funeral_home_phone": funeral_home_phone,
    #     "funeral_home_url": funeral_home_url
    # })

    # news_schema = json_metadata_object["schemas"]["newsArticleSchema"]

    # results_dict.update({
    #     "news_schema": news_schema
    # })

    # suggested_pages = json_metadata_object["shareModal"]["successFollowInterface"]["suggestedPages"]
    # if suggested_pages:
    #     newspaper_name = "unknown"
    #     community_name = "unknown"
    #     tag = "unknown"

    #     for p in suggested_pages:
    #         if p["type"] == "NewspaperListing":
    #             newspaper_name = p["name"]
    #             tag = p["slug"]

    #         elif p["type"] == "CommunityPage":
    #             community_name = p["name"]
    #             tag = p["slug"]

    #     results_dict.update({
    #         "newspaper_name": newspaper_name,
    #         "community_name": community_name,
    #         "tag": tag
    #     })


    # obituary_text = json_metadata_object["obituary"]["text"]

    # results_dict.update({
    #     "obituary_text": obituary_text
    # })

    # return both entire json and results_dict with only useful vars
    return json_metadata_object, results_dict


def load_obit_text_and_metadata_from_html(page_html):
    soup = BeautifulSoup(page_html, 'html.parser')

    obit_data = {}

    # get features
    name = soup.find('h2', {'data-component': 'NameHeadingText'})
    if name:
        name = name.text
    else:
        name = 'unknown'

    # note that text is wrapped in <p> tags
    text = soup.find('div', {'data-component': 'ObituaryText'})
    if text:  # first kind of page
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

    lifespan = soup.find('p', {'data-component': 'LifespanText'})
    if lifespan:
        lifespan = lifespan.text
        try:
            lifespan = lifespan.split(' - ')  # little dash
            assert len(lifespan) == 2  # should be [birth_year, end_year]
        except AssertionError:
            lifespan = []

    obit_data['name'] = name
    obit_data['text'] = text
    obit_data['funeral_home_link'] = funeral_home_link
    obit_data['funeral_location'] = funeral_location
    if lifespan and len(lifespan) == 2:
        obit_data['birth_year'] = lifespan[0]
        obit_data['death_year'] = lifespan[1]

    return obit_data