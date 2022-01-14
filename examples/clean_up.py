import os
import boto3
import bookmark_utils

boto_ses = boto3.session.Session()
sts = boto_ses.client("sts")
account_id = sts.get_caller_identity()["Account"]
package_name = bookmark_utils.__name__

s3_bucket = "{}-{}-test".format(
    account_id,
    package_name.replace("_", "-"),
)
dynamodb_table = "{}_{}_test".format(
    account_id,
    package_name.replace("-", "_"),
)

def empty_s3_bucket():
    try:
        s3 = boto_ses.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        bucket.objects.all().delete()
        bucket.delete()
        print ("S3 bucket deleted")
    except Exception as e:
        raise

def delete_dynamodb_table():
    try:
        client = boto_ses.client('dynamodb')
        client.delete_table(TableName=dynamodb_table)
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=dynamodb_table)
        print ("DynamoDB table deleted")
    except Exception as e:
        raise

    
if __name__ == "__main__":
    delete_dynamodb_table()
    empty_s3_bucket()