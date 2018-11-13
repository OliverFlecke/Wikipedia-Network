import requests
import re, os
import datetime
from typing import List, Dict

all_links_file = 'dawiki-latest-all-titles-in-ns0'

with open('data/blacklist_dk.txt', 'r', encoding='utf-8') as f:
    blacklist = f.readlines()

    regex_blacklist = [
        re.compile(r'^{0}:'.format(black.strip()))
        for black in blacklist
    ]

def get_links(page: str) -> List[str]:
    params = {
        'action': 'parse',
        'page': page,
        'format': 'json',
        'prop': 'links',
    }
    url = 'https://da.wikipedia.org/w/api.php'

    request = requests.get(url=url, params=params)

    if request.status_code == 200:
        return clean_links(request.json())

    return []

def clean_links(content: Dict) -> List[str]:
    links = list(map(lambda x: x['*'], content['parse']['links']))

    if len(links) == 1:
        return []

    return [link for link in links
        if not isInBlacklist(link)
    ]

def isInBlacklist(link: str) -> bool:
    for regex in regex_blacklist:
        if regex.search(link):
            return True

    return False

def clean_filename(name: str) -> str:
    # <>:"/\|?*.
    name = name.replace(r'<', 'GREATERTHAN')
    name = name.replace(r'>', 'LESSTHAN')
    name = name.replace(r':', 'COLON')
    name = name.replace(r'"', 'QUOTE')
    name = name.replace(r'/', 'SLASH')
    name = name.replace(r'\\', 'BSLASH')
    name = name.replace(r'|', 'BAR')
    name = name.replace(r'?', 'QUESTIONMARK')
    name = name.replace(r'*', 'STAR')
    name = name.replace(r'.', 'DOT')

    return name

def save_links(page: str, links: List[str]):
    filename = clean_filename(page)
    with open(os.path.join('data/links', filename), 'w', encoding='utf-8') as f:
        f.writelines('\n'.join(links))

with open(os.path.join('data', all_links_file), 'r', encoding='utf-8') as f:
    f.readline()
    start = datetime.datetime.now()
    for count, page in enumerate(f):
        page = page.strip()
        links = get_links(page)
        if not links == []:
            save_links(page, links)

        if count % 100 == 0:
            now = datetime.datetime.now()
            print('Count {0} Time {1}'.format(count, now - start))

# links = get_links(',')
# print(links)

