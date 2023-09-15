import requests
import boto3
import datetime
import io
import zipfile

def read_aws_creds(credPath):
    '''
    Read AWS credentials stored in a CSV file

    Args:
        credPath (str): the path to the credentials CSV
    '''

    with open(credPath,'r') as f:
        creds = f.read()

    return creds.split('\n')[1].split(',')

def get_s3_keys(bucket, s3Client, prefix = ''):

    """
    Generate the keys in an S3 bucket.
    
    Args:
        bucket (str): the name of the S3 bucket
        s3Client (str): the AWS SDK client
    """
    
    # Build arguments dictionary
    kwargs = {'Bucket': bucket}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        resp = s3Client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix):
                yield key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def get_previous_year_keys(credPath):

    # Initialize S3 client with credentials
    keyID,key = read_aws_creds(credPath)
    s3Client = boto3.client('s3',aws_access_key_id=keyID,aws_secret_access_key=key)

    # Bucket where data is stored
    bucketName = "divvy-tripdata"

    # Get the keys from the S3 bucket
    keys = get_s3_keys(bucketName,s3Client)

    # Selecting the keys within past year
    keys = [key for key in keys] 

    # Get the current month and year
    now = datetime.datetime.now()
    yearNow = now.year
    monthNow = now.month

    # Initialize an empty list
    prefixList = []

    # Loop over the past 12 months, skip the current month
    for i in range(1,13):

        # Go back i months
        month = monthNow - i
        year = yearNow

        # Go back a year if we dip past January
        if month <= 0:
            month = 12 + month
            year = year -1 

        # Build the strings
        yearStr = str(year)
        monthStr = str(month).zfill(2)

        prefixList.append(yearStr+monthStr)

    prefixTuple = tuple(prefixList)
    return [item for item in keys if item.startswith(prefixTuple)]

def download_data(bucketName,keys):

    """
    Retrieve divvy trip data from AWS storage
    
    Args:
        keys (list of str): the AWS keys for each data to download
    """

    # Loop through listed keys
    for key in keys:

        # Build the URL to download
        url = f'https://{bucketName}.s3.amazonaws.com/{key}'

        # Download the data
        response = requests.get(url)

        # Extract the resulting zip file
        zip = zipfile.ZipFile(io.BytesIO(response.content))
        zip.extractall("./data/")