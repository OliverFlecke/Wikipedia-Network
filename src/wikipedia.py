import requests
import re, os
import datetime
import base64
from typing import List, Dict

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
all_links_file = 'dawiki-latest-all-titles-in-ns0'
links_path = os.path.join(root, 'data/links2')

with open('data/blacklist_dk.txt', 'r', encoding='utf-8') as f:
    blacklist = f.readlines()

    regex_blacklist = [
        re.compile(r'^{0}:'.format(black.strip()))
        for black in blacklist
    ]

def get_links(page: str) -> List[str]:
    '''Get all the links between wikipedia pages from a gives page

    @param page: Name of the page to get the links for
    @return: The list of all the links of the given Wikipedia page
    '''
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
    '''Get the links from the content of a Wikipedia request and remove bad links

    @param content: The JSON content from a request to the wikipedia API
    @return: The cleaned list of links from the page
    '''
    try:
        links = list(map(lambda x: x['*'], content['parse']['links']))
    except:
        return []

    if len(links) == 1:
        return []

    return [link for link in links
        if not isInBlacklist(link)
    ]

def isInBlacklist(link: str) -> bool:
    '''Checks if a links is blacklisted

    @param link: The link to check
    @return: True if the link is blacklisted
    '''
    for regex in regex_blacklist:
        if regex.search(link):
            return True

    return False

def get_filename(page: str) -> str:
    '''Get the filename for a page in base32 format

    @param page: Name of the page
    @return: A file page that can be used on multiple OS
    '''
    return str(base64.b32encode(bytes(page, 'utf-8')))

def save_links(page: str, links: List[str]):
    '''Save links from a page to a file

    @param page: Name of the page
    @param links: The links to save
    '''
    filename = get_filename(page)
    with open(os.path.join(links_path, filename), 'w', encoding='utf-8') as f:
        f.writelines('\n'.join(links))

if not os.path.exists(links_path):
    os.mkdir(links_path)

start_index = 0

with open(os.path.join(root, 'data', all_links_file), 'r', encoding='utf-8') as f:
    f.readline()
    start = datetime.datetime.now()
    for _ in range(start_index):
        f.readline()
    for count, page in enumerate(f, start_index):
        page = page.strip()
        links = get_links(page)
        if not links == []:
            save_links(page, links)

        if count % 100 == 0:
            now = datetime.datetime.now()
            print('Count {0} Time {1}'.format(count, now - start))
