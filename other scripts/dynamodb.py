import json
import boto3
from decimal import Decimal
from collections import defaultdict

dynamodb = boto3.resource('dynamodb')
table_name = 'yelp-restaurants' 
table = dynamodb.Table(table_name)

def float_to_decimal(event):
    return json.loads(json.dumps(event), parse_float=Decimal)

def lambda_handler(event, context):
    try:
        # Convert float values to Decimal
        data = float_to_decimal(event)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]

        # Group restaurants by cuisine
        cuisine_dict = defaultdict(list)
        for item in data:
            cuisine = item['CuisineType/S']
            if cuisine == "italian":
                cuisine_dict[cuisine].append(item)

        # Prepare items for DynamoDB, limiting to 50 per cuisine
        prepared_items = []
        for cuisine, restaurants in cuisine_dict.items():
            for restaurant in restaurants[:50]:  # Limit to 50 restaurants per cuisine
                dynamo_item = {
                    'Business ID': restaurant['Business ID'],
                    'Name': restaurant['Name'],
                    'Address': restaurant['Address'],
                    'Latitude': restaurant['Coordinates/Latitude'],
                    'Longitude': restaurant['Coordinates/Longitude'],
                    'NumberOfReviews': restaurant['Number of Reviews'],
                    'Rating': restaurant['Rating'],
                    'ZipCode': restaurant['Zip Code'],
                    'CuisineType': restaurant['CuisineType/S'],
                    'InsertedAtTimestamp': restaurant['InsertedAtTimestamp/N']
                }
                prepared_items.append(dynamo_item)

        # Add items to DynamoDB
        with table.batch_writer() as batch:
            for item in prepared_items:
                batch.put_item(Item=item)

        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully added to DynamoDB')
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error adding data to DynamoDB: {str(e)}')
        }