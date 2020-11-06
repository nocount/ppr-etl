# Loading data from Residential Property Price registry into S3
""" Todo: 
- separate file download and upload into separate functions
- add try catches so failure wouldn't break the flow
- put keys in a config file
"""
import boto3

from urllib.request import Request, urlopen

import ssl
ssl._create_default_https_context = ssl._create_stdlib_context
# import requests

aws_access_key_id = 'AKIAJTXHH6FE6TTMR2WA'
aws_secret_access_key = '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'

ppr_link = 'https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip'


if __name__ == '__main__':

	request = Request(ppr_link, headers={'User-Agent': 'Mozilla/5.0'})
	response = urlopen(request)
	# file = response.read()

	s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	s3.Object('ppr-etl', 'extract/ppr_test.zip').upload_fileobj(response)

	# s3 = boto3.resource('s3')
	# s3.Object('s3://ppr-etl/extract/', 'ppr_test.txt').put(Body=open('ppr_test.txt', 'rb'))
	print('Praise the Sun')

