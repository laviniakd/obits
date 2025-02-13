def load_obit_text_and_metadata(page_html):
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