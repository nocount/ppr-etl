# Loading data from Residential Property Price registry into S3
""" Todo: 
- separate file download and upload into separate functions
- add try catches so failure wouldn't break the flow
- scrape for link instead of hardcoding?
"""

import boto3

from urllib.request import Request, urlopen


ppr_link = (
	'https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip'
)


def scrape_dl_link():
	pass


def upload_to_s3():
	pass


if __name__ == '__main__':

	request = Request(ppr_link, headers={'User-Agent': 'Mozilla/5.0'})
	response = urlopen(request)

	s3 = boto3.resource('s3')
	s3.Object('ppr-etl', 'extract/ppr.zip').upload_fileobj(response)

	print('Praise the Sun')

