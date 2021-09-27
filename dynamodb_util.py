import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


class DynamoDBUtil:
    def __init__(self):
        self._dynamodb = boto3.resource('dynamodb')
        self._table = None

    def create_table_if_not_exist(self, table_name, partition_key, partition_key_type, sort_key, sort_key_type):

        if self.is_table_exits(table_name):
            print('Table already exists')
        else:
            table = self._dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': partition_key,
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': sort_key,
                        'KeyType': 'RANGE'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': partition_key,
                        'AttributeType': partition_key_type
                    },
                    {
                        'AttributeName': sort_key,
                        'AttributeType': sort_key_type
                    }
                ],

                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            print("Creating table...")
            table.meta.client.get_waiter(
                'table_exists').wait(TableName=table_name)
            print("Table {} created".format(table.table_name))

    def get_data(self, table_name):
        table = self.get_table(table_name)
        response = table.scan()
        data = response["Items"]
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response["Items"])
        return data

    def get_data_between_time_range(self, table_name, from_time, to_time):
        table = self.get_table(table_name)
        response = table.scan(FilterExpression=Attr(
            'timestamp').between(from_time, to_time))
        data = response["Items"]
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response["Items"])
        return data

    def bulk_insert(self, table_name, items):
        table = self.get_table(table_name)
        with table.batch_writer() as batch:
            for item in items:
                print(item)
                batch.put_item(Item=item)

    def get_table(self, table_name):
        return self._dynamodb.Table(table_name)

    def is_table_exits(self, table_name):
        table = self._dynamodb.Table(table_name)
        try:
            is_table_existing = table.table_status in ("CREATING", "UPDATING",
                                                       "DELETING", "ACTIVE")
        except ClientError:
            is_table_existing = False
            print("Table {} doesn't exist.".format(table.table_name))

        return is_table_existing
