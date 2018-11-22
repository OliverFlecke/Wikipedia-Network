import os
import base64
import json

root = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
links_dir = 'data/links2/'

with open(os.path.join(root, 'data', 'filenames.txt'), 'r') as f:
    filenames = set(f.read().split('\n'))

def get_filename(page: str) -> str:
    '''Get the filename for a page in base32 format

    @param page: Name of the page
    @return: A file page that can be used on multiple OS
    '''
    return str(base64.b32encode(bytes(page, 'utf-8')))[:255]

with open(os.path.join(root, 'data', 'dawiki-latest-all-titles-in-ns0'), 'r', encoding='utf-8') as f:
    names = f.read().split('\n')

redirects = {}

for name in names:
    filename = get_filename(name)
    if filename in filenames:
        with open(os.path.join(root, links_dir, filename), 'r', encoding='utf-8') as f:
            links = f.read().split('\n')
        
        if len(links) == 1:
            redirects[name] = links[0]
        else:
            with open(os.path.join(root, 'data', 'pages.txt'), 'w', encoding='utf-8') as f:
                f.write(name + '\n')

with open(os.path.join(root, 'data', 'redirects.json'), 'w') as f:
    json.dump(redirects, f)

