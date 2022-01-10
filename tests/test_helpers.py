# -*- coding: utf-8 -*-

import time
import boto3
import random
import pytest
from datetime import datetime
from bookmark_utils import helpers

client = boto3.session.Session().client("dynamodb")


class TestCreateDynamodbTableIfNotExists:
    _table_name = "create_dynamodb_table_if_not_exists_test"
    table_name = "{}_{}".format(_table_name, random.randint(1, 100))

    @classmethod
    def setup_class(cls):
        try:
            res = client.delete_table(TableName=cls.table_name)
            time.sleep(5)
        except client.exceptions.ResourceNotFoundException:
            pass
        except:
            raise

    @classmethod
    def teardown_class(cls):
        res = client.list_tables()
        for table_name in res.get("TableNames", []):
            if table_name.startswith(cls._table_name):
                client.delete_table(TableName=table_name)

    def test_create_table(self):
        create_table_kwargs = dict(
            TableName=self.table_name,
            KeySchema=[
                {
                    "AttributeName": "the_hash_key",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "the_hash_key",
                    "AttributeType": "S"
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        # before state, table doesn't exists
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_table(TableName=self.table_name)

        # create table
        st = datetime.utcnow()
        helpers.create_dynamodb_table_if_not_exists(
            dynamodb_client=client,
            table_name=self.table_name,
            create_table_kwargs=create_table_kwargs
        )
        et = datetime.utcnow()
        assert (et - st).total_seconds() >= 2  # spend at least 2 seconds

        # after state, table is created
        res = client.describe_table(TableName=self.table_name)
        assert res["Table"]["TableName"] == self.table_name

        # since table already exists, this time will do nothing
        st = datetime.utcnow()
        helpers.create_dynamodb_table_if_not_exists(
            dynamodb_client=client,
            table_name=self.table_name,
            create_table_kwargs=create_table_kwargs
        )
        et = datetime.utcnow()
        assert (et - st).total_seconds() <= 2  # spend no more than 2 seconds


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
