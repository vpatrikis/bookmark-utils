# -*- coding: utf-8 -*-

import os
import boto3
import pytest
import bookmark_utils
from bookmark_utils import DataLoader

boto_ses = boto3.session.Session()
sts = boto_ses.client("sts")
s3 = boto_ses.client("s3")
dynamodb = boto_ses.client("dynamodb")

account_id = sts.get_caller_identity()["Account"]

package_name = bookmark_utils.__name__
dir_here = os.path.dirname(os.path.abspath(__file__))


class TestBookmark:
    # --- Tests dependencies
    test_s3_bucket = "{}-{}-test".format(
        account_id,
        package_name.replace("_", "-"),
    )
    test_s3_prefix = "data"
    test_dynamodb_table = "{}_{}_test".format(
        account_id,
        package_name.replace("-", "_"),
    )

    @classmethod
    def setup_class(cls):
        cls.setup_s3_bucket()
        cls.setup_initial_data_files()

    @classmethod
    def setup_s3_bucket(cls):
        try:
            s3.head_bucket(Bucket=cls.test_s3_bucket)
        except Exception as e:
            if "HeadBucket operation: Not Found" in str(e):
                s3.create_bucket(Bucket=cls.test_s3_bucket)
            else:
                raise

    @classmethod
    def setup_initial_data_files(cls):
        # clear existing files
        res = s3.list_objects_v2(
            Bucket=cls.test_s3_bucket, Prefix=cls.test_s3_prefix, MaxKeys=1000,
        )
        for dct in res.get("Contents", []):
            key = dct["Key"]
            s3.delete_object(Bucket=cls.test_s3_bucket, Key=key)

        # upload initial files
        for fname in os.listdir(os.path.join(dir_here, "data")):
            path = os.path.join(dir_here, "data", fname)
            file_type = fname.split(".")[-1]
            key = f"{cls.test_s3_prefix}_{file_type}/{fname}"
            s3.upload_file(path, Bucket=cls.test_s3_bucket, Key=key)

    # --- Test cases
    def test_load_data_from_s3(self):
        s3_path_test = self.test_s3_prefix + "_csv"
        data_loader = DataLoader(
            s3_bucket_name=self.test_s3_bucket,
            s3_location=s3_path_test,
            format_of_data="csv",
            job_name="job_123",
            dynamo_db_table_for_bookmark_storage=self.test_dynamodb_table,
        )

        data_loader.load_data_from_s3()

    def test_parquet_data_load(self):
        s3_path_test = self.test_s3_prefix + "_parquet"
        data_loader = DataLoader(
            s3_bucket_name=self.test_s3_bucket,
            s3_location=s3_path_test,
            format_of_data="parquet",
            job_name="job_test_parquet_data_load",
            dynamo_db_table_for_bookmark_storage=self.test_dynamodb_table)

        df = data_loader.load_data_from_s3()
        assert df.shape[0] != 0

    def test_json_data_load(self):
        s3_path_test = self.test_s3_prefix + "_json"
        data_loader = DataLoader(
            s3_bucket_name=self.test_s3_bucket,
            s3_location=s3_path_test,
            format_of_data="json",
            job_name="job_test_parquet_data_load",
            dynamo_db_table_for_bookmark_storage=self.test_dynamodb_table)

        df = data_loader.load_data_from_s3()
        assert df.shape[0] != 0

    def test_commit_and_load(self):
        data_loader = DataLoader(
            s3_bucket_name="bucket-for-datalab",
            s3_location="folder-for-bookmark-test",
            format_of_data="csv",
            job_name="job_123",
            dynamo_db_table_for_bookmark_storage=self.test_dynamodb_table)

        load1 = data_loader.load_data_from_s3()
        load2 = data_loader.load_data_from_s3()
        assert load1.shape[0] != 0 and load2.shape[0] != 0
        data_loader.commit()
        data_after_commit = data_loader.load_data_from_s3()
        assert data_after_commit.shape[0] == 0
        data_loader.load_data_from_s3()

    def test_invalid_file_type_exception(self):
        with pytest.raises(Exception) as ex:
            DataLoader(
                "bucket-for-datalab",
                "folder-for-bookmark-test",
                "avro",
                "job_123"
                "bookmark_table")

    def clean_up_dynamo_table(self):
        dynamodb_client = boto3.client("dynamodb")
        try:
            dynamodb_client.describe_table(TableName=self.test_dynamodb_table)
            dynamodb_client.delete_table(TableName=self.test_dynamodb_table)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pass

if __name__ == "__main__":
    import os

    print("started from main")
    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])

