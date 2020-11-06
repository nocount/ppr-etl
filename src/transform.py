# Download zip from S3 extract and then perform any necessary cleaning/transformations

import boto3
import zipfile
import pandas as pd
import numpy as np
import re

pd.set_option('display.max_columns', 500)

IE_COUNTIES = [
    'antrim', 'armagh', 'cavan', 'derry', 'donegal', 'down', 'fermanagh', 'monaghan', 'tyrone', 'galway', 'leitrim',
    'mayo', 'roscommon', 'sligo', 'carlow', 'dublin', 'kildare', 'kilkenny', 'laois', 'longford', 'louth',
    'meath', 'offaly', 'westmeath', 'wexford', 'wicklow', 'clare', 'cork', 'kerry', 'limerick', 'tipperary', 'waterford'
]

def parse_month_start(date):
    month_start = '01' + date[2:]
    return month_start


def clean_price(price):
    value = int(re.sub('[,.]', '', price[1:]))/100
    return value


def to_ind(field):
    ind = 0
    if field == 'Yes':
        ind = 1
    return ind


def description_to_ind(desc):
    ind = 0
    if desc == 'New Dwelling house /Apartment':
        ind = 1
    return ind


if __name__ == '__main__':
    # s3 = boto3.client('s3')
    # s3.download_file('ppr-etl', 'extract/ppr.zip', '../data/ppr.zip')
    #
    # with zipfile.ZipFile('../data/ppr.zip', 'r') as zip:
    #     zip.extractall('../data/')

    df = pd.read_csv('../data/PPR-ALL.csv', encoding='ISO-8859-1')
    # df = pd.read_csv('../data/PPR-ALL.csv', encoding='cp1252')

    # Postal code and property size columns contain null values and are not needed so we drop them
    df.drop(['Postal Code', 'Property Size Description'], axis=1, inplace=True)

    # Add quarantine columns so we can mark duplicates
    df['quarantine_ind'] = 0
    df['quarantine_code'] = ''

    # Check for duplicates and mark for quarantine
    df.loc[df.duplicated(keep=False) == True, 'quarantine_ind'] = 1
    df.loc[df.duplicated(keep=False) == True, 'quarantine_code'] = 'ERR - DUPLICATE DATA'

    # Start transformations
    # Creating id column
    df['id'] = df.index

    # Renaming sales_date column
    df.rename(columns={'Date of Sale (dd/mm/yyyy)': 'sales_date'}, inplace=True)

    df['month_start'] = df.sales_date.apply(lambda x: parse_month_start(x))

    df['address'] = df.Address.apply(lambda s: s.lower())
    df['county'] = df.County.apply(lambda s: s.lower())

    # TODO: fix this on mac so we dont reference by index
    df['sales_value'] = df.iloc[:, 3].apply(lambda x: clean_price(x)).astype('int64')
    df.drop(df.columns[3], axis=1, inplace=True)

    df['not_full_market_price_ind'] = df['Not Full Market Price'].apply(lambda x: to_ind(x))
    df['vat_exclusive_ind'] = df['VAT Exclusive'].apply(lambda x: to_ind(x))

    df['new_home_ind'] = df['Description of Property'].apply(lambda x: description_to_ind(x))

    # Quarantine for incorrect counties and ERR - NEW HOME NOT FULL MARKET VALUE
    df.loc[(df['new_home_ind'] == 1) & (df['not_full_market_price_ind'] == 1), 'quarantine_ind'] = 1
    df.loc[(df['new_home_ind'] == 1) & (df['not_full_market_price_ind'] == 1), 'quarantine_code'] = 'ERR - NEW HOME NOT FULL MARKET VALUE'

    df.loc[~df.county.isin(IE_COUNTIES), 'quarantine_ind'] = 1
    df.loc[~df.county.isin(IE_COUNTIES), 'quarantine_code'] = 'ERR - COUNTY NOT IN IRELAND'

    # dupe_df = df[df.duplicated(keep=False) == True]
    # print(dupe_df.shape)
    # print(dupe_df.head())
    # print(dupe_df.head())

    df.drop(['Address', 'County', 'Not Full Market Price', 'VAT Exclusive', 'Description of Property'], axis=1, inplace=True)

    # Reordering columns correctly
    df = df[['id', 'sales_date', 'month_start', 'address', 'county', 'sales_value', 'not_full_market_price_ind', 'vat_exclusive_ind', 'new_home_ind', 'quarantine_ind', 'quarantine_code']]
    print(df.info())
    print(df.shape)
    print(df.head)

    print('Transform completed')