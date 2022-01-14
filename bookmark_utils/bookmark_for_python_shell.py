"""
This simple utility reads the data from S3 and does checkpoint the information about last read location
"""

import datetime

import pandas as pd
import boto3
import awswrangler as wr
import pytest
import logging

logger = logging.getLogger("root")
from typing import List
from .helpers import create_dynamodb_table_if_not_exists


class DataLoader(object):
    valid_file_formats = ["csv", "parquet", "json", "xml"]
    """
    The constructor initializes the utility with S3 location information and table to store bookmark info.
    """

    def __init__(self,
                 s3_bucket_name: str,
                 s3_location: str,
                 format_of_data: str,
                 job_name: str,
                 dynamo_db_table_for_bookmark_storage: str = "bookmark_table"):
        self.s3_bucket_name = s3_bucket_name
        self.s3_location = s3_location
        self.format_of_the_data = format_of_data
        self.job_name = job_name
        self.dynamo_db_table_for_bookmark_storage = dynamo_db_table_for_bookmark_storage

    @property
    def s3_bucket_name(self):
        return self.__s3_bucket_name

    @s3_bucket_name.setter
    def s3_bucket_name(self, value):
        self.__s3_bucket_name = value

    @property
    def s3_location(self):
        return self.__s3_location

    @s3_location.setter
    def s3_location(self, value):
        self.__s3_location = value

    @property
    def format_of_the_data(self):
        return self.__format_of_the_data

    @format_of_the_data.setter
    def format_of_the_data(self, value):
        if value not in self.valid_file_formats:
            raise Exception("File format is not valid. Format should be one of csv,parquet,json and xml")

        self.__format_of_the_data = value

    @property
    def dynamo_db_table_for_bookmark_storage(self):
        return self.__dynamo_db_table_for_bookmark_storage

    @dynamo_db_table_for_bookmark_storage.setter
    def dynamo_db_table_for_bookmark_storage(self, value):
        # Ensure the table exists (already created or just create a new one)
        dynamodb_boto3_client = boto3.client("dynamodb")
        create_table_kwargs = dict(
            TableName=value,
            KeySchema=[
                {
                    'AttributeName': 'job_name',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'bookmark_timestamp',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'job_name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'bookmark_timestamp',
                    'AttributeType': 'N'
                }

            ],
            BillingMode='PAY_PER_REQUEST'
        )
        create_dynamodb_table_if_not_exists(
            dynamodb_client=dynamodb_boto3_client,
            table_name=value,
            create_table_kwargs=create_table_kwargs,
        )

        # validate schema
        table_schema = dynamodb_boto3_client.describe_table(
            TableName=value)['Table']['KeySchema']
        partition_key = [key['AttributeName'] for key in table_schema if key['KeyType'] == 'HASH'][0]
        sort_key = [key['AttributeName'] for key in table_schema if key['KeyType'] == 'RANGE'][0]
        if partition_key != "job_name":  # pragma: no cover
            raise Exception("Partition key name is incorrect. It should be job_name")

        if sort_key != "bookmark_timestamp":  # pragma: no cover
            raise Exception("Sort key name is not incorrect. It should be bookmark_timestamp")

        self.__dynamo_db_table_for_bookmark_storage = value

    @property
    def job_name(self):
        return self.__job_name

    @job_name.setter
    def job_name(self, value):
        self.__job_name = value

    """
    process s3 file location information
    """

    def process_s3_location_information(self, s3_list_object_response):  # pragma: no cover
        """
        TODO: this method is not used, considering remove it
        """

        files = sorted((f for f in s3_list_object_response['Contents'] if f['Key'].endswith(self.format_of_the_data)),
                       key=lambda file_name: file_name['LastModified'], reverse=True)

        latest_timestamp = files[0]['LastModified'].timestamp()
        list_of_files_to_process = [f"s3://{self.s3_bucket_name}/{file['Key']}" for file in files]
        return latest_timestamp, list_of_files_to_process

    """
    This method uses the latest timestamp picked up from Dynamo DB and separates the S3 files 
    which are modified or added after that.
    """

    def get_latest_files_from_s3_using_bookmark(self, last_read_timestamp):

        import boto3
        import datetime

        # bucket Resource
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.s3_bucket_name)

        date_time_for_filter = datetime.datetime.fromtimestamp(last_read_timestamp)

        objects_to_filter = bucket.objects.filter(Prefix=self.s3_location)

        files = sorted((file for file in objects_to_filter if
                        (file.last_modified.replace(tzinfo=None) > date_time_for_filter) and file.key.endswith(
                            self.format_of_the_data)),
                       key=lambda file_name: file_name.last_modified.replace(tzinfo=None), reverse=True)

        if len(files) < 1:
            return None, []
        else:
            latest_timestamp = files[0].last_modified.replace(tzinfo=None)
            return latest_timestamp, files

    """
    This method registers the information about the last read timestamp in DynamoDB table
    """

    def register_bookmark(self, latest_timestamp, status="IN_PROGRESS"):
        dynamodb_boto3_client = boto3.client("dynamodb")
        dynamodb_boto3_client.put_item(
            TableName=self.dynamo_db_table_for_bookmark_storage,
            Item={
                'job_name': {'S': self.job_name},
                'bookmark_timestamp': {'N': latest_timestamp.strftime('%s')},
                'data_load_timestamp': {'N': datetime.datetime.now().strftime('%s')},
                'status': {'S': status}
            }
        )

    """
    get latest timestamp information from DynamoDB table
    """

    def get_latest_timestamp_from_db(self, status="COMPLETE"):
        dynamodb_boto3_client = boto3.client("dynamodb")
        result = dynamodb_boto3_client.query(
            TableName=self.dynamo_db_table_for_bookmark_storage,
            KeyConditionExpression="#job_name = :job_name",
            FilterExpression='#status = :status',
            ExpressionAttributeNames={
                '#job_name': 'job_name',
                '#status': 'status'
            },
            ExpressionAttributeValues={
                ':job_name': {
                    'S': self.job_name
                },
                ':status': {
                    'S': status
                }
            },
            ScanIndexForward=False,
            Limit=1,
        )

        if len(result['Items']) < 1:
            return 0
        else:
            return int(result['Items'][0]['bookmark_timestamp']['N'])

    """
    This method reads the data from S3
    """

    def load_data_from_s3(self) -> pd.DataFrame:
        global df
        existing_timestamp = self.get_latest_timestamp_from_db(status="COMPLETE")
        print("--------------->>>>>>>")
        print(f"existing timestamp {existing_timestamp}")
        print("--------------->>>>>>>")

        latest_timestamp, files_to_process = self.get_latest_files_from_s3_using_bookmark(existing_timestamp)
        if not files_to_process:
            print("there are no files to process")
            return pd.DataFrame()
        else:
            dataframes_to_union = []

            for file in files_to_process:
                filename = f"s3://{self.s3_bucket_name}/{file.key}"
                print(f"loading the filename: {filename}")

                if self.format_of_the_data == 'parquet':
                    df = wr.s3.read_parquet(filename)
                elif self.format_of_the_data == 'csv':
                    df = wr.s3.read_csv(filename, encoding='ISO-8859-1')
                elif self.format_of_the_data == 'json':
                    df = wr.s3.read_json(filename)
                dataframes_to_union.append(df)

            final_dataframe_with_latest_data = pd.concat(dataframes_to_union, axis=0, ignore_index=True)
            self.register_bookmark(latest_timestamp, status="IN_PROGRESS")
            print("Data Load completed successfully")
            return final_dataframe_with_latest_data

    def commit(self):
        latest_timestamp = datetime.datetime.fromtimestamp(self.get_latest_timestamp_from_db(status="IN_PROGRESS"))
        self.register_bookmark(latest_timestamp, status="COMPLETE")