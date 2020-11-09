import boto3

from botocore.exceptions import ClientError

PPR_LINK = (
    'https://www.propertypriceregister.ie/website/npsra/ppr/npsra-ppr.nsf/Downloads/PPR-ALL.zip/$FILE/PPR-ALL.zip'
)

IE_COUNTIES = [
    'antrim', 'armagh', 'cavan', 'derry', 'donegal', 'down', 'fermanagh', 'monaghan', 'tyrone', 'galway', 'leitrim',
    'mayo', 'roscommon', 'sligo', 'carlow', 'dublin', 'kildare', 'kilkenny', 'laois', 'longford', 'louth',
    'meath', 'offaly', 'westmeath', 'wexford', 'wicklow', 'clare', 'cork', 'kerry', 'limerick', 'tipperary', 'waterford'
]


def upload_file_to_s3(key, file_path):
    """
    Uploads a file from local storage to the ppr-etl s3 bucket.

    :param key: s3 key for the destination of the file
    :param file_path: path of the file in local storage
    """
    s3 = boto3.resource('s3')
    try:
        s3.Object('ppr-etl', key).upload_file(file_path)
    except ClientError as err:
        print('Log - Error occurred uploading file to s3: ' + err.response['Error']['Code'])


def upload_fileobj_to_s3(key, obj):
    """
    Uploads a file to s3, reads from buffer instead of local storage.

    :param key: s3 key for the destination of the file
    :param obj: the file obj to be uploaded
    """
    s3 = boto3.resource('s3')
    try:
        s3.Object('ppr-etl', key).upload_fileobj(obj)
    except ClientError as err:
        print('Log - Error occurred uploading file obj to s3: ' + err.response['Error']['Code'])


def download_file_from_s3(key, file_path):
    """
    Downloads a file from s3 to local storage.

    :param key: s3 key for the source of the file
    :param file_path: path of the file destination in local storage
    """
    s3 = boto3.resource('s3')
    try:
        s3.Bucket('ppr-etl').download_file(key, file_path)
    except ClientError as err:
        print('Log - Error occurred downloading file from s3: ' + err.response['Error']['Code'])
