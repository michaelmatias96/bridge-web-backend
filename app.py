import random
import time
import string
import json
import simplejson
from flask import Flask, request, jsonify
from flask_cors import CORS
import boto3
from boto3.dynamodb.conditions import Key, Attr
from aws_utils import save_to_s3, save_to_dynamo, update_in_dynamo, get_from_dynamo, query_dynamo, scan_dynamo

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# CONSTANTS
FILES_BUCKET = 'bridge-files'
MESSAGES_TABLE = 'bridge-messages'
USERS_TABLE = 'bridge-users'
DEFAULT_KEY_NAME = '¡d'
DEFAULT_USER = 'grandma'


@app.route("/messages", methods=['GET', 'POST', 'PUT'])
def messages():
    if request.method == 'GET':
        print('/messages GET')
        source = int(request.args.get('source'))
        messages = scan_dynamo(MESSAGES_TABLE, 'source', source)['Items']
        messages = list(filter(filter_unread, messages))
        messages = json.loads(simplejson.dumps(messages, use_decimal=True))
        return jsonify(messages)

    elif request.method == 'POST':
        print('/messages POST')
        posted_data = request.get_json()
        posted_data['id'] = create_id()
        timestamp = int(round(time.time() * 1000))
        posted_data['timestamp'] = timestamp
        posted_data['status'] = 'unread'
        save_to_dynamo(MESSAGES_TABLE, posted_data)
        return jsonify({"status": "success"})

    elif request.method == 'PUT':
        print('/messages PUT')
        posted_data = request.get_json()
        update_in_dynamo(MESSAGES_TABLE, {'id': posted_data['id']}, {
                         'status': posted_data['status']})
        return jsonify({"status": "success"})


def filter_unread(message):
    if message['status'] == 'unread':
        return True
    else:
        return False


@app.route("/target_status", methods=['GET', 'PUT'])
def targetStatus():
    if request.method == 'GET':
        response = query_dynamo('bridge-users', 'grandma', 'id')['Items'][0]
        print(response)
        return jsonify(response)
    elif request.method == 'PUT':
        posted_data = request.get_json()
        update_in_dynamo(USERS_TABLE, {'id': DEFAULT_USER}, posted_data)
        return jsonify({"status": "success"})


def create_id(size=10, chars=string.ascii_uppercase + string.digits):
    while True:
        id = ''.join(random.choice(chars) for _ in range(size))
        response = query_dynamo(USERS_TABLE, id, 'id')
        if len(response['Items']) < 1:
            break
    return id


if __name__ == "__main__":
    app.run()
