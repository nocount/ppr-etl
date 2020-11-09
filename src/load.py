import pandas as pd
import datetime

from utils import download_file_from_s3, upload_file_to_s3

pd.set_option('display.max_columns', 500)


def merge_data_snapshots():
    """
    Merges the current data snapshot with the newly transformed data.

    :returns: the combined dataframe
    """
    old_df = pd.read_csv('../data/ppr_old.csv')
    new_df = pd.read_csv('../data/ppr_new.csv')

    current_df = new_df.combine_first(old_df)
    return current_df


def main():
    """
    Downloads the newly transformed data and the current snapshot.
    Then merges them together and reuploads the result.
    """
    download_file_from_s3(key='transform/ppr.csv', file_path='../data/ppr_new.csv')
    download_file_from_s3(key='load/ppr_current.csv', file_path='../data/ppr_old.csv')

    current_df = merge_data_snapshots()
    current_df.to_csv('../data/ppr_current.csv', index=False)

    # This is unnecessary if you turn on object versioning in your S3 bucket but just to be safe.
    upload_file_to_s3(key='load/ppr_' + str(datetime.date.today()) + '.csv', file_path='../data/ppr_old.csv')
    upload_file_to_s3(key='load/ppr_current.csv', file_path='../data/ppr_current.csv')

    print(current_df.shape)
    print(current_df.head())


if __name__ == '__main__':
    main()
