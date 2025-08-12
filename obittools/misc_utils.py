import pandas as pd
import time
import requests
import numpy as np


USER_AGENT_STRING = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"


def make_request(url, user_agent=None):
    if not user_agent:
        user_agent = USER_AGENT_STRING
    page = requests.get(url, headers={'User-Agent': user_agent, 'Accept': 'text/html'})

    if page.status_code != 200:
        raise Exception(f"Failed to get page: {url}")
    return page


def get_big_msas_and_states():
    path = "/data/laviniad/obits/aux/cbsa-met-est2023-pop.csv"
    df = pd.read_csv(path)
    # filter to top 100 MSAs by population
    # drop na
    df = df.dropna()
    df = df[df['Geographic Area'].str.contains('Metro Area')]
    df = df[df['Geographic Area'].str.contains('-') == False]
    df.sort_values('2023', ascending=False, inplace=True)
    df = df.head(100)
    # print list of df['Geographic Area']
    print(df['Geographic Area'].values)

    state_abbrev_full_dict = get_state_dict()
    city_state_tuples = []

    for idx, row in df.iterrows():
        city_state = row['Geographic Area'].replace('Metro Area', '').strip()
        # remove 'Urban'
        if city_state[:5] == 'Urban':
            city_state = city_state[6:]
        # also remove '.' from start
        if city_state[0] == '.':
            city_state = city_state[1:]
        city = city_state.split(',')[0]
        state = city_state.split(',')[1]
        state = state_abbrev_full_dict[state.strip()]
        city_state_tuples.append({'city': city, 'state': state})

    return city_state_tuples


def get_state_dict():
    state_abbrev_dict = {
        'AL': 'Alabama',
        'AK': 'Alaska',
        'AZ': 'Arizona',
        'AR': 'Arkansas',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'IA': 'Iowa',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'ME': 'Maine',
        'MD': 'Maryland',
        'MA': 'Massachusetts',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MS': 'Mississippi',
        'MO': 'Missouri',
        'MT': 'Montana',
        'NE': 'Nebraska',
        'NV': 'Nevada',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NY': 'New York',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VT': 'Vermont',
        'VA': 'Virginia',
        'WA': 'Washington',
        'WV': 'West Virginia',
        'WI': 'Wisconsin',
        'WY': 'Wyoming',
        'DC': 'District of Columbia',
        'PR': 'Puerto Rico'
    }

    return state_abbrev_dict


def get_reverse_state_dict():
    state_abbrev_dict = get_state_dict()
    state_full_dict = {v: k for k, v in state_abbrev_dict.items()}

    return state_full_dict
