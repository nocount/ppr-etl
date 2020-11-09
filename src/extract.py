from urllib.request import Request, urlopen
from urllib.error import URLError
from utils import PPR_LINK, upload_fileobj_to_s3


def pull_latest_ppr():
    """
    Pulls the latest data from the Property Price Register website.
    Currently pulls a full load of all past data.

    :returns: response containing data from PPR
    """
    request = Request(url=PPR_LINK, headers={'User-Agent': 'Mozilla/5.0'})
    response = urlopen(request)

    return response


def main():
    """
    Pulls data from the PPR website and then sends it to s3 as a zip.
    """
    try:
        response = pull_latest_ppr()
        upload_fileobj_to_s3(key='extract/ppr.zip', obj=response)
    except URLError:
        print('Log - Errror in pulling  and uploading ppr data')


if __name__ == '__main__':
    main()
