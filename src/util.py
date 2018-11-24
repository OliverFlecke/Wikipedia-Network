import os
import base64

def get_filename(name: str) -> str:
    '''Get the filename for a page in base32 format

    @param page: Name of the page
    @return: A file page that can be used on multiple OS
    '''
    return str(base64.b32encode(bytes(name, 'utf-8')))[:255]

