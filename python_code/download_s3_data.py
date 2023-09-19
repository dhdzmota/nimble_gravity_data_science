import boto3
import os
import yaml

from urllib.parse import urlparse


S3_URI = 's3://ng-data-science-interviews/clickstream2'
CREDENTIALS = 'aws_credentials'


def read_config(path):
    """Function to read a config that's made up as yaml format.

    Parameters
    ----------
    path: str
        String that contains the path to the desired yaml path.

    Returns
    -------
    config: dict
        Dictionary that contains the information of the yaml path.
    """
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def parse_s3_url(url):
    """ This function parses an s3 url. Returning a tuple: the bucket and the
    key of an s3 url.

    Parameters
    ----------
    url: str
        String that defines the name of the bucket and the corresponding key.

    Returns
    -------
    bucket: str
        Part of the url that corresponds to the bucket.
    key: str
        Part of the url that correspondes to the key.
    """
    parts = urlparse(url)
    bucket = parts.netloc
    key = parts.path.lstrip('/')
    return bucket, key


def download_files(credentials, bucket, path):
    """ This function download all the files that are found in a s3 bucket into
    a desired path given the credentials.

    Parameters
    ----------
    credentials: dict
        Dictionary that contains the information of the credentials needed to
        access s3.
    bucket: str
        Name of the bucket that contains the files
    path:
        Path where the files will be downloaded into.

    Returns
    -------
    None.

    """
    # First we generate a resource with the given credentials
    s3 = boto3.resource('s3', **credentials)
    # We know which is the bucket
    s3_bucket = s3.Bucket(bucket)
    # Let's iterate on the bucket's objects:
    for s3_object in s3_bucket.objects.all():
        object_key = s3_object.key
        _, filename = os.path.split(object_key)
        filepath = os.path.join(path, filename)
        s3_bucket.download_file(object_key, filepath)
        print(f'Downloaded `{filename}` file to {path}.')


if __name__ == '__main__':
    file_path = os.path.dirname(os.path.abspath(__file__))
    general_path = os.path.join(file_path, '..',)
    data_path = os.path.join(general_path, 'data', 'raw')
    config_path = os.path.join(general_path, 'config.yaml')
    credentials = read_config(config_path)
    bucket, _ = parse_s3_url(S3_URI)
    download_files(credentials[CREDENTIALS], bucket, data_path)
