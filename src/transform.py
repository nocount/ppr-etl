import zipfile
import pandas as pd
import re

from utils import IE_COUNTIES, download_file_from_s3, upload_file_to_s3

pd.set_option('display.max_columns', 500)


def parse_month_start(date):
    """
    Generates the month start date from the date of sale.

    :param date: date of sale to be processed
    :returns: formatted month start
    """
    month_start = '01' + date[2:]
    return month_start


def clean_price(price):
    """
    Cleans price field by removing punctuation, currency sign and converting to int.

    :param price: price field to be cleaned
    :returns: cleaned integer value of the price
    """
    value = int(int(re.sub('[,.]', '', price[1:]))/100)
    return value


def to_ind(field):
    """
    Converts a yes or no field to an indicator.

    :param field: field to be converted
    :returns: indicator value of the field
    """
    ind = 0
    if field == 'Yes':
        ind = 1
    return ind


def description_to_ind(desc):
    """
    Converts property description field to an indicator.

    :param desc: field to be converted
    :returns: indicator value of the description
    """
    ind = 0
    if desc == 'New Dwelling house /Apartment':
        ind = 1
    return ind


def transform_data(df):
    """
    Performs all of the required transformations of the data.

    :param df: raw unprocessed dataframe
    :returns: cleaned and formatted dataframe
    """
    # Postal code and property size columns contain null values and are not needed so we drop them
    df.drop(['Postal Code', 'Property Size Description'], axis=1, inplace=True)

    # Add quarantine columns so we can mark duplicates
    df['quarantine_ind'] = 0
    df['quarantine_code'] = ''

    # Check for duplicates and mark for quarantine
    df.loc[df.duplicated(keep=False) == True, 'quarantine_ind'] = 1
    df.loc[df.duplicated(keep=False) == True, 'quarantine_code'] = 'ERR - DUPLICATE DATA'

    # Creating id column
    df['id'] = df.index

    # Renaming sales_date column
    df.rename(columns={'Date of Sale (dd/mm/yyyy)': 'sales_date'}, inplace=True)

    df['month_start'] = df.sales_date.apply(lambda x: parse_month_start(date=x))

    df['address'] = df.Address.apply(lambda s: s.lower())
    df['county'] = df.County.apply(lambda s: s.lower())

    # TODO: fix this on mac so we dont reference by index
    df['sales_value'] = df.iloc[:, 3].apply(lambda x: clean_price(price=x)).astype('int64')
    df.drop(df.columns[3], axis=1, inplace=True)

    df['not_full_market_price_ind'] = df['Not Full Market Price'].apply(lambda x: to_ind(field=x))
    df['vat_exclusive_ind'] = df['VAT Exclusive'].apply(lambda x: to_ind(x))

    df['new_home_ind'] = df['Description of Property'].apply(lambda x: description_to_ind(desc=x))

    # Quarantine for incorrect counties and new homes that were not sold at market value
    df.loc[(df['new_home_ind'] == 1) & (df['not_full_market_price_ind'] == 1), 'quarantine_ind'] = 1
    df.loc[(df['new_home_ind'] == 1) & (
            df['not_full_market_price_ind'] == 1), 'quarantine_code'] = 'ERR - NEW HOME NOT FULL MARKET VALUE'

    df.loc[~df.county.isin(IE_COUNTIES), 'quarantine_ind'] = 1
    df.loc[~df.county.isin(IE_COUNTIES), 'quarantine_code'] = 'ERR - COUNTY NOT IN IRELAND'

    df.drop(['Address', 'County', 'Not Full Market Price', 'VAT Exclusive', 'Description of Property'], axis=1,
            inplace=True)

    # Reordering columns correctly
    df = df[['id', 'sales_date', 'month_start', 'address', 'county', 'sales_value', 'not_full_market_price_ind',
             'vat_exclusive_ind', 'new_home_ind', 'quarantine_ind', 'quarantine_code']]

    return df


def main():
    """
    Pulls the data from s3, extracts the zip to local storage, cleans and formats it and then uploads the
    transformed data back to s3.
    """
    download_file_from_s3(key='extract/ppr.zip', file_path='../data/ppr.zip')

    with zipfile.ZipFile('../data/ppr.zip', 'r') as zip_ref:
        zip_ref.extractall('../data/')

    df = pd.read_csv('../data/PPR-ALL.csv', encoding='ISO-8859-1')
    df = transform_data(df)
    df.to_csv('../data/ppr.csv', index=False)

    upload_file_to_s3(key='transform/ppr.csv', file_path='../data/ppr.csv')


if __name__ == '__main__':
    main()
