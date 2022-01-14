import os
import boto3
import bookmark_utils
from bookmark_utils import DataLoader

boto_ses = boto3.session.Session()
sts = boto_ses.client("sts")
s3 = boto_ses.client("s3")
dynamodb = boto_ses.client("dynamodb")

account_id = sts.get_caller_identity()["Account"]

package_name = bookmark_utils.__name__
dir_here = os.path.dirname(os.path.abspath(__file__))

s3_bucket = "{}-{}-test".format(
    account_id,
    package_name.replace("_", "-"),
)
s3_prefix = "data"
dynamodb_table = "{}_{}_test".format(
    account_id,
    package_name.replace("-", "_"),
)

def setup_s3_bucket():
    try:
        s3.head_bucket(Bucket=s3_bucket)
    except Exception as e:
        if "HeadBucket operation: Not Found" in str(e):
            s3.create_bucket(Bucket=s3_bucket)
        else:
            raise

def setup_initial_data_files():
    # clear existing files
    res = s3.list_objects_v2(
        Bucket=s3_bucket, Prefix=s3_prefix, MaxKeys=1000,
    )
    for dct in res.get("Contents", []):
        key = dct["Key"]
        s3.delete_object(Bucket=s3_bucket, Key=key)

    # upload initial files
    path = os.path.join(dir_here, "data", "a.csv")
    key = f"{s3_prefix}/a.csv"
    s3.upload_file(path, Bucket=s3_bucket, Key=key)

def load_data_from_s3():
    bm = DataLoader(
        s3_bucket_name=s3_bucket,
        s3_location=s3_prefix,
        format_of_data="csv",
        job_name="job_123",
        dynamo_db_table_for_bookmark_storage=dynamodb_table,
    )

    df = bm.load_data_from_s3()
    bm.commit()
    print("The loaded data is:\n", df)
    
if __name__ == "__main__":
    setup_s3_bucket()
    setup_initial_data_files()
    load_data_from_s3()
    