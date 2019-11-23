##########################################################################################
# AWS UTILS
##########################################################################################
from boto3.dynamodb.conditions import Key, Attr
import time
import boto3
print('\tboto3 ok')

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def save_to_s3(bucket_name, file, key):
    if isinstance(file, str):
        file = open(file, 'rb')
    s3.Bucket(bucket_name).put_object(Key=key, Body=file)


def upload_file_to_s3(bucket_name, filename, key):
    s3.Bucket(bucket_name).upload_file(filename, key)


def upload_file_obj_to_s3(bucket_name, filename, key):
    with open(filename, 'rb') as data:
        s3.Bucket(bucket_name).upload_fileobj(data, key)


def save_to_dynamo(table_name, value):
    date = int(round(time.time() * 1000))
    value['timestamp'] = date
    response = dynamodb.Table(table_name).put_item(Item=value)
    return response


def update_in_dynamo(table_name, keys, attributes_to_update):
    date = int(round(time.time() * 1000))
    attributes_to_update['update_date'] = date

    update_expression = 'SET '
    expression_attribute_values = {}
    expression_attribute_names = {}
    i = 1
    for attribute_name, attribute_value in attributes_to_update.items():
        expression_attribute_key = ':val' + str(i)
        expression_attribute_value = attribute_value
        expression_attribute_values[expression_attribute_key] = expression_attribute_value
        update_expression += '#{} = :val{}'.format(attribute_name, i)
        expression_attribute_names['#{}'.format(
            attribute_name)] = attribute_name
        if i != len(attributes_to_update):
            update_expression += ', '
        i += 1

    response = dynamodb.Table(table_name).update_item(
        Key=keys,
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )
    return response


def get_from_dynamo(table_name, key, key_name="document_id"):
    response = dynamodb.Table(table_name).get_item(Key={key_name: key})
    return response


def query_dynamo(table_name, key, key_name='document_id'):
    response = dynamodb.Table(table_name).query(
        KeyConditionExpression=Key(key_name).eq(key)
    )
    return response


def scan_dynamo(table_name, attribute, value_to_equal):
    response = dynamodb.Table(table_name).scan(
        FilterExpression=Attr(attribute).eq(value_to_equal))
    return response


def send_message_sqs(queue_name, body):
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    response = queue.send_message(MessageBody=body)
    return response
