import json
import boto3
from datetime import datetime
import re
import dateutil.utils
    # Send the message to the SQS queue
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = "https://sqs.us-east-1.amazonaws.com/640168439397/First_queue"


def get_session_data(session_id):
    table = dynamodb.Table('UserSearchState')  # Replace with your table name
    response = table.get_item(Key={'UserId': session_id})
    
    # Return the session data if it exists
    if 'Item' in response:
        return response['Item']
    else:
        return None
        
def delete_session_data(session_id):
    table = dynamodb.Table('UserSearchState')
    
    try:
        response = table.delete_item(
            Key={'UserId': session_id}
        )
        print(f"Deleted session data for user_id: {session_id}")
        return response
    except Exception as e:
        print(f"Error deleting session data for user_id {session_id}: {str(e)}")
        return None        
        
        
        
        
def lambda_handler(event, context):
    print("event: ", event)
    
    intent = event['sessionState']['intent']['name']
    
    if intent == 'GreetingIntent':
        return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'GreetingIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': 'Hi there, how can I help you today?'
        }]
    }
    elif intent == 'ThankYouIntent':
        return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'ThankYouIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': "You're welcome!"
        }]
    }
    elif intent == 'DiningSuggestion':
        if event['sessionState']['intent']['confirmationState'] == 'Confirmed':
            user_id = event['sessionId']
            
            # Fetch previous session data from DynamoDB using sessionId
            session_data = get_session_data(user_id)
            
            if session_data:
                # Fetch all values from the retrieved session data
                previous_location = session_data['LastLocation']
                previous_category = session_data['LastCategory']
                participant_count = session_data['People']
                date = session_data['Date']
                time = session_data['Time']
                customer_email = session_data['Email']
                
                print("Previous Category:", previous_category)
                print("Previous Location:", previous_location)

                # Prepare the payload for SQS
                load_for_sqs = {
                    'City': previous_location,
                    'Cuisine': previous_category,
                    'People': participant_count,
                    'Date': date,
                    'Time': time,
                    'Email': customer_email,
                    'Session_Id': user_id
                }

                # Convert to JSON string and print the entire payload
                json_payload = json.dumps(load_for_sqs)
                print("Complete SQS Payload to send:", json_payload)

                # Push the complete JSON message to SQS
                try:
                    response = sqs.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json_payload  # Pass the complete JSON string here
                    )
                except Exception as e:
                    print(f"Error sending message to SQS: {str(e)}")
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': str(e)})
                    }
            else:
                return {
                    'statusCode': 404,
                    'body': 'No session data found in DynamoDB for the given session ID.'
                }
            
            return close_session(
                event,
                f"You're all set! Expect my suggestions for {previous_category} food in {previous_location} shortly."
            )
        elif event['sessionState']['intent']['confirmationState'] == 'Denied':
            user_id = event['sessionId']
            
            # Delete the entry from DynamoDB
            delete_session_data(user_id)
            
            # Proceed with the new preferences
            return lambda_check(event['sessionState']['intent']['slots'], event)
   
        else:
            # If user says "no" or this is the first time, proceed to get the new preferences
            return lambda_check(event['sessionState']['intent']['slots'], event)
    else:
        # Default response if the intent is unrecognized
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close',
                },
                'intent': {
                    'name': intent,
                    'state': 'Failed'
                }
            },
            'messages': [
                {
                    'contentType': 'PlainText',
                    'content': 'Sorry, I did not understand your request.'
                }
            ]
        }
def close_session(event, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Fulfilled'
            },
            'intent': event['sessionState']['intent']
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }


dynamodb = boto3.resource('dynamodb')

def store_last_search(user_id, location, category, people, date, time, email):
    table = dynamodb.Table('UserSearchState')
    
    # Put the last search information into DynamoDB
    table.put_item(
        Item={
            'UserId': user_id,
            'LastLocation': location,
            'LastCategory': category,
            'People': people,
            'Date': date,
            'Time': time,
            'Email': email
        }
    )
    
    return {
        'statusCode': 200,
        'messages': [
            {
                'contentType': 'PlainText',
                'content': f"Your search for {category} food in {location} has been saved."
            }
        ]
    }
    
    
def push_to_sqs(event):
    # Safely extract the values from event slots
    cuisine = event['sessionState']['intent']['slots']['Cuisine']['value']['interpretedValue']
    city = event['sessionState']['intent']['slots']['City']['value']['interpretedValue']
    ParticipantCount = event['sessionState']['intent']['slots']['People']['value']['interpretedValue']
    date = event['sessionState']['intent']['slots']['Date']['value']['interpretedValue']
    time = event['sessionState']['intent']['slots']['Time']['value']['interpretedValue']
    customerEmail = event['sessionState']['intent']['slots']['Email']['value']['interpretedValue']
    user_id = event['sessionId']

    # Combine the extracted values into a single JSON payload
    load_for_sqs = {
        'City': city,
        'Cuisine': cuisine,
        'People': ParticipantCount,
        'Date': date,
        'Time': time,
        'Email': customerEmail,
        'Session_Id': user_id
    }

    # Convert to JSON string and print the entire payload at once
    json_payload = json.dumps(load_for_sqs)
    print("Complete SQS Payload to send:", json_payload)

    try:
        # Send the complete JSON message as a single message
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json_payload  # Pass the complete JSON string here
        )

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Message sent to SQS successfully!',
                'SQSMessageId': response['MessageId']
            })
        }

    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def elicit_slot(event, slot_name, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_name
            },
            'intent': event['sessionState']['intent']
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }   
    
def valid_city(resolved_values):
    city_list = ["nyc", "queens", "flushing", "NYC","Manhattan"]
    return resolved_values in city_list


def valid_participant_count(count):
    try:
        count = int(count)
        return 1 <= count <= 12
    except ValueError:
        return False

def valid_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.fullmatch(regex, email) is not None

def valid_cuisine(cuisine):
    cuisine_list = ["indian", "italian","mexican", "japanese", "french"]
    return cuisine.lower() in cuisine_list

def valid_time(time, date):
    date = dateutil.parser.parse(date).date()
    time = dateutil.parser.parse(time).time()

    if datetime.combine(date, time) < datetime.now():
        return False
    return True

def valid_date(date):
    parsed_date = dateutil.parser.parse(date)
    return parsed_date >= dateutil.utils.today()


def get_last_search(user_id):
    table = dynamodb.Table('UserSearchState')
    response = table.get_item(Key={'UserId': user_id})
    return response.get('Item')
    
def lambda_check(slots, event):
    user_id = event['sessionId']
    
    # Fetch the last search
    last_search = get_last_search(user_id)
    if last_search:
        # Ask the user if they want to use the previous search
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'ConfirmIntent'
                },
                'intent': event['sessionState']['intent'],
                'sessionAttributes': {
                    'PreviousLocation': last_search['LastLocation'],
                    'PreviousCategory': last_search['LastCategory']
                }
            },
            'messages': [{
                'contentType': 'PlainText',
                'content': f"I have your previous preferences: {last_search['LastCategory']} food in {last_search['LastLocation']}. Do you want to use them?"
            }]
        }
    
    if not slots.get('City') or not slots['City'].get('value'):
        return elicit_slot(event, 'City', 'Which city are looking to dine in')
    try:
        print(slots['City']['value']['interpretedValue'].lower())
        if not valid_city(slots['City']['value']['interpretedValue'].lower()):
            return elicit_slot(event, 'City', 'Sorry,We dont serve in that area.What city are you looking to dine in?')
    except:
        return elicit_slot(event, 'City', 'Please enter a valid City')
    
    if not slots.get('People') or not slots['People'].get('value'):
        return elicit_slot(event, 'People', 'How many people are dining?')
    try:
        if not valid_participant_count(slots['People']['value']['interpretedValue']):
            return elicit_slot(event, 'People', 'Please enter a value between 1 and 12')
    except:
        return elicit_slot(event, 'People', 'Please enter a value between 1 and 12')
    
    if not slots.get('Cuisine') or not slots['Cuisine'].get('value'):
        return elicit_slot(event, 'Cuisine', 'What type of cuisine u wanna try?')
    try:
        print(slots['Cuisine']['value']['interpretedValue'])
        if not valid_cuisine(slots['Cuisine']['value']['interpretedValue']):
            return elicit_slot(event, 'Cuisine', 'We only provide suggestions for Indian,French,Japanese,Mexican and Italian food. What type of cuisine would you like to enjoy?')
    except:
        return elicit_slot(event, 'Cuisine', 'What type of cuisine would you like to enjoy?')
    
    if not slots.get('Date') or not slots['Date'].get('value'):
        return elicit_slot(event, 'Date', 'On what date would you like to dine?')
    try:
        if not valid_date(slots['Date']['value']['interpretedValue']):
            return elicit_slot(event, 'Date', 'Please choose a date in upcoming days.')
    except:
        return elicit_slot(event, 'Date', 'Please enter a valid date')
    
    if not slots.get('Time') or not slots['Time'].get('value') or not slots['Time']['value'].get('interpretedValue'):
        return elicit_slot(event, 'Time', 'At what time would you like to dine?')
    try:
        if not valid_time(slots['Time']['value']['interpretedValue'],slots['Date']['value']['interpretedValue']):
            return elicit_slot(event, 'Time', 'Please choose a date in upcoming days.')
    except:
        return elicit_slot(event, 'Time', 'Please enter a valid time')

    
    if not slots.get('Email') or not slots['Email'].get('value') or not slots['Email']['value'].get('interpretedValue'):
        return elicit_slot(event, 'Email', 'Please provide your email address.')
    if not valid_email(slots['Email']['value']['interpretedValue']):
        return elicit_slot(event, 'Email', 'Please enter a valid email address.')
        
    # pushing to   sqs  
    push_to_sqs(event)
    cuisine = event['sessionState']['intent']['slots']['Cuisine']['value']['interpretedValue']
    city = event['sessionState']['intent']['slots']['City']['value']['interpretedValue']
    ParticipantCount = event['sessionState']['intent']['slots']['People']['value']['interpretedValue']
    date = event['sessionState']['intent']['slots']['Date']['value']['interpretedValue']
    time = event['sessionState']['intent']['slots']['Time']['value']['interpretedValue']
    customerEmail = event['sessionState']['intent']['slots']['Email']['value']['interpretedValue']
    user_id = event['sessionId']
    
    # Store the user’s last search in DynamoDB
    print(user_id)
    
    store_last_search(user_id, city, cuisine, ParticipantCount, date, time, customerEmail)
    
    return close_session(
        event,
        f"You’re all set to receive a mail on {slots['Email']['value']['interpretedValue']}. "
        f"Expect my suggestions shortly for {slots['Cuisine']['value']['interpretedValue']} food "
        f"in {slots['City']['value']['interpretedValue']} for {slots['People']['value']['interpretedValue']} people "
        f"on {slots['Date']['value']['interpretedValue']} at {slots['Time']['value']['interpretedValue']}. "
        "Have a good day."
    )

