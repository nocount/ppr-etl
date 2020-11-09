import boto3
import pytest
import pandas as pd

from moto import mock_s3
from extract import pull_latest_ppr
from transform import parse_month_start, clean_price, transform_data
from load import merge_data_snapshots
from utils import upload_fileobj_to_s3, upload_file_to_s3


def test_ppr_data_retrieval():
    response = pull_latest_ppr()
    assert response.status == 200
    assert response.getheader(name='Content-Type') == 'application/x-zip'


@mock_s3
def test_extract():
    response = pull_latest_ppr()

    # Creating bucket for moto
    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='ppr-etl')

    upload_fileobj_to_s3(key='mock/file.zip', obj=response)

    assert s3.Object('ppr-etl', 'mock/file.zip').get()['ResponseMetadata']['HTTPStatusCode'] == 200


@pytest.mark.parametrize(
    "date, start",
    [('31/12/2020', '01/12/2020'), ('15/01/2020', '01/01/2020'), ('01/06/2020', '01/06/2020')]
)
def test_month_start_generated_correctly(date, start):
    month_start = parse_month_start(date=date)
    assert month_start == start


@pytest.mark.parametrize(
    "price, cleaned_price",
    [('€125,000.00', 125000), ('€191,363.63', 191363), ('€157,709.25', 157709)]
)
def test_price_cleaned_correctly(price, cleaned_price):
    value = clean_price(price)
    assert value == cleaned_price


def test_transform_formats_columns():
    df = pd.read_csv('../data/PPR-ALL.csv', encoding='ISO-8859-1')
    df = transform_data(df)
    assert df.shape[1] == 11
    assert df.columns.tolist() == [
        'id', 'sales_date', 'month_start', 'address', 'county', 'sales_value',
        'not_full_market_price_ind', 'vat_exclusive_ind', 'new_home_ind', 'quarantine_ind', 'quarantine_code'
    ]


@mock_s3
def test_load():
    current_df = merge_data_snapshots()
    assert current_df.shape[1] == 11

    current_df.to_csv('../data/ppr_current.csv', index=False)

    s3 = boto3.resource('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='ppr-etl')

    upload_file_to_s3(key='mock/mock.csv', file_path='../data/ppr_current.csv')

    assert s3.Object('ppr-etl', 'mock/mock.csv').get()['ResponseMetadata']['HTTPStatusCode'] == 200
