import json
import os
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse


username = 'bhuvan'
password = 'Bhuvan@123'


region = 'us-east-1'
host = 'https://search-resturants-64y3hpvewixh7f7drwesmqesfi.aos.us-east-1.on.aws'
index = 'restaurants'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
db = boto3.resource('dynamodb').Table('yelp-restaurants')


def query(term):
    # Example query (match all)
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}
    
    # Elasticsearch URL (replace with your actual host and index)
    url = host + '/' + index + '/_search'
    
    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = {"Content-Type": "application/json"}

    # Make the signed HTTP request with Basic Auth
    r = requests.get(url, auth=HTTPBasicAuth(username, password), headers=headers, data=json.dumps(q))
    
    print("Elastic Search res ",r.text)
    if r.status_code == 200:
        response_json = r.json()
        print("Elasticsearch Response:", response_json)
        
        # Extract the _source from each hit and store it in a list
        sources = [hit['_source'] for hit in response_json['hits']['hits']]
        print("Extracted _source data:", sources)
        
        return sources
    else:
        print(f"Error: Status Code {r.status_code}")
        print("Response:", r.text)
        return None


    return r
                    
def queryDynamo(ids):
    results = []
    for id in ids:
        response = db.query(KeyConditionExpression = Key("Business ID").eq(id))
        results.append(response["Items"][0])
    return results


def update_user_state(session_id, restaurant_ids):
    # Update the UserSearchState DynamoDB table with the restaurant IDs for the given session
    table = dynamodb.Table('UserSearchState')
    
    # Update the item with the new restaurant ids
    table.update_item(
        Key={'UserId': session_id},
        UpdateExpression="SET RestaurantIDs = :ids",
        ExpressionAttributeValues={
            ':ids': restaurant_ids
        }
    )
    
def lambda_handler(event, context):
    sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/640168439397/First_queue'  # Replace with your SQS queue URL
    
    # Receive a message from the SQS queue
    sqs = boto3.client('sqs')
    response = sqs.receive_message(
        QueueUrl=sqs_queue_url,
        AttributeNames=[
            'All'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0)
    
    print("response ", response)
  
    d = response['Messages'][0]
    msg_body = json.loads(d['Body'])
    print("SQS Message ",msg_body)  

    if 'Messages' in response:
        # Extract the message body from the received message
        message = msg_body
        # Extract data from the message slots
        city = msg_body.get('City')
        cuisine = msg_body.get('Cuisine')
        date = msg_body.get('Date')
        people = msg_body.get('People')
        time = msg_body.get('Time')
        email = msg_body.get('Email')
        session_id = msg_body.get('Session_Id') 
        
        # Getting the message from the open source
        query_resp = query(cuisine)
    
    
        ids = []
        for i in range(0,5):
            ids.append(query_resp[i]['RestaurantID'])
            
        
        print("ids ", ids)
        # Pulling the restaurant information from the dynamoDB
        db_rest = queryDynamo(ids)
        
        update_user_state(session_id, ids)

        
        # Sending the confirmation to the email
        
        client = boto3.client("ses")
        subject = "Reservation Details"
        
        # Create the HTML body using the data from the message slots
        body = f"Hello! Here are my  restaurant suggestions for {people} people at {time} in {city} on {date} for {cuisine} cuisine.<br><br>"
        for i in range(0, 5):
            body += f"{i + 1}: {db_rest[i]['Name']} at {db_rest[i]['Address']}<br>"
            
        body += "Enjoy your mea"
        # Send the email
        email_response = client.send_email(
            Source="bk2988@nyu.edu",
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": body}}
            }
        )
        
        # Delete the received message from the SQS queue
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=sqs_queue_url,
            ReceiptHandle=receipt_handle
        )
        
        return email_response
    
    else:
        return "No messages available in the queue."