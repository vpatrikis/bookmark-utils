import boto3
import numpy as np
import pandas as pd
import bookmark_utils
from bookmark_utils import DataLoader

boto_ses = boto3.session.Session()
sts = boto_ses.client("sts")
s3 = boto_ses.client("s3")
dynamodb = boto_ses.client("dynamodb")
account_id = sts.get_caller_identity()["Account"]
package_name = bookmark_utils.__name__
s3_bucket = "{}-{}-test".format(account_id, package_name.replace("_", "-"),)
s3_prefix = "data"
dynamodb_table = "{}_{}_test".format(account_id, package_name.replace("-", "_"),)

bm = DataLoader(
        s3_bucket_name=s3_bucket,
        s3_location=s3_prefix,
        format_of_data="csv",
        job_name="bookmark-utils-test",
        dynamo_db_table_for_bookmark_storage=dynamodb_table,
    )

df = bm.load_data_from_s3()
bm.commit()
print("The data in the dataframe is:\n", df)
df.to_csv("bookmark-test.csv", header=True, index=False)
s3.upload_file('bookmark-test.csv', Bucket=s3_bucket, Key='loaded/bookmark-test.csv')