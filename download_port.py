#################################################

#Created by Fernando Ruiz Diaz | August 4th 2021

#################################################

import logging
import os
import errno

# Cope with different location of urljoin in python 2 vs 3
try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin
from datetime import date
import requests

from beap_auth import Credentials, BEAPAdapter
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] [%(name)s:%(lineno)s]: %(message)s',
)
log = logging.getLogger(__name__)

credentials = Credentials.from_file('ferdle_home.txt')

session = requests.Session()
adapter = BEAPAdapter(credentials)
session.mount('https://', adapter)
HOST = 'https://api.bloomberg.com'

def download_distribution(session_, url, out_path, chunk_size=2048,
                          stream=True, headers=None):
    """
    Function to download the data to an output directory

    This function opts for the gzip output encoding by default and allows the
    user to specify the output location of this download. This function works
    for a single endpoint.

    You may set the 'Accept-Encoding' header to 'Identity' if you do not
    want to receive the gzipped file.

    Set 'chunk_size' to a larger byte size to speed up download process on
    larger downloads.
    """
    headers = headers or {'Accept-Encoding': 'gzip'}
    with session_.get(url, stream=stream, headers=headers) as response_:
        with open(out_path, 'wb') as out_file:
            for chunk in response_.raw.stream(chunk_size, decode_content=True):
                out_file.write(chunk)
            return response_

today=date.today().strftime('%Y%m%d')

equitynamr_distributions_path = '/eap/catalogs/792641/datasets/r20210616010413c3ea2b/snapshots/'+str(today)+'/distributions/r20210616010413c3ea2b.bbg'
equitynamr_distributions_url = urljoin(HOST, equitynamr_distributions_path)
response = session.get(equitynamr_distributions_url)

output_file = "minhaReq2."+str(today)+".csv"
# an output directory
output_dir = r'C:\Users\diete\Documents\Portfolio'
# Create the output directory, if it doesn't exist
try:
    os.makedirs(output_dir)
except OSError as err:
    if err.errno != errno.EEXIST:
        log.exception('Could not create output directory %s', output_dir)
        raise

output_path = os.path.join(output_dir, output_file)

sample_url = urljoin(equitynamr_distributions_url, 'r20210616010413c3ea2b.bbg')
response = download_distribution(session, sample_url, output_path)

log.info('Content-Encoding: %s', response.encoding)
log.info('Content-Length: %s', response.headers['Content-Length'])
log.info('Downloaded to: %s', output_path)
