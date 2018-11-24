import os
import json
from util import get_filename

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
links_path = 'data/links2/'

with open(os.path.join(root, 'data', 'pages.txt'), 'r', encoding='utf-8') as f:
    pages = f.read().split('\n')

with open(os.path.join(root, 'data', 'redirects.json'), 'r', encoding='utf-8') as f:
    redirects = json.load(f)

def delete_redirect_files(redirects: dict):
    ''' Remove all files only containing redirects
    '''
    for redirect in redirects.keys():
        filename = os.path.join(root, links_path, get_filename(redirect))

        if os.path.exists(filename):
            os.remove(filename)

def swap_redirects(pages: list):
    ''' Swap redirects for each page
    '''
    for page in pages:
        filename = os.path.join(root, links_path, get_filename(page))
        
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                links = f.read().split('\n')

            for i, link in enumerate(links):
                if link in redirects:
                    links[i] = redirects[link]

            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(links))


delete_redirect_files(redirects)

swap_redirects(pages[:5])
