# -*- coding: utf-8 -*-

import os
import boto3
import pytest
import bookmark_utils
from bookmark_utils import BookMarks

boto_ses = boto3.session.Session()
sts = boto_ses.client("sts")
s3 = boto_ses.client("s3")
dynamodb = boto_ses.client("dynamodb")

account_id = sts.get_caller_identity()["Account"]

package_name = bookmark_utils.__name__
dir_here = os.path.dirname(os.path.abspath(__file__))


class TestBookmark:
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
        for fname in os.listdir(os.path.join(dir_here, "data")):
            path = os.path.join(dir_here, "data", fname)
            key = f"{cls.test_s3_prefix}/data/a.csv"
            s3.upload_file(path, Bucket=cls.test_s3_bucket, Key=key)

    def test_load_data_from_s3(self):
        bm = BookMarks(
            s3_bucket_name=self.test_s3_bucket,
            s3_location=self.test_s3_prefix,
            format_of_data="csv",
            job_name="job_123",
            dynamo_db_table_for_bookmark_storage=self.test_dynamodb_table)

        bm.load_data_from_s3()


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
