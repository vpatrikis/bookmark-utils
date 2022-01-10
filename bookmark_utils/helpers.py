# -*- coding: utf-8 -*-


import time


def create_dynamodb_table_if_not_exists(
    dynamodb_client,
    table_name: str,
    create_table_kwargs: dict,
    timeout: int = 30,
) -> None:
    """
    An sync version of dynamodb client ``create_table()`` method. The
    original method immediately returns and the table will be be ready
    to use instantly. This method creates a dynamodb table and wait until
    it to be ready (usually takes 5 seconds). And if the table already
    exists, this method won't do anything.

    :param dynamodb_client: an boto3.session.Session.client("dynamodb")
        object
    :param table_name: dynamodb table name
    :param create_table_kwargs: key value arguments for ``create_table()``
        method
    :param timeout: dynamodb table creation timeout time in seconds

    :return: None
    """
    try:
        dynamodb_client.describe_table(TableName=table_name)
        return  # table already exists
    except dynamodb_client.exceptions.ResourceNotFoundException:
        pass  # continue on table creation logic
    except: # pragma: no cover
        raise

    if "TableName" not in create_table_kwargs:
        create_table_kwargs["TableName"] = table_name
    create_table_response = dynamodb_client.create_table(**create_table_kwargs)
    create_table_http_response_code = create_table_response["ResponseMetadata"]["HTTPStatusCode"]
    if create_table_http_response_code != 200: # pragma: no cover
        raise Exception("Create table operation failed")
    describe_table_response = dynamodb_client.describe_table(TableName=table_name)
    table_status = describe_table_response["Table"]["TableStatus"]
    for _ in range(timeout):
        if table_status == "CREATING":
            time.sleep(1)
            describe_table_response = dynamodb_client.describe_table(TableName=table_name)
            table_status = describe_table_response["Table"]["TableStatus"]
        else:
            return
    raise TimeoutError(f"Creating Dynamodb Table timeout in {timeout} seconds") # pragma: no cover
