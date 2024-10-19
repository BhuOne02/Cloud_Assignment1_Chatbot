import json
import boto3
import uuid
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Initialize the Lex client
lex_client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    BOT_ID = '1CPQZZTXYW'
    BOT_ALIAS_ID = '8CFRZ92EJ9'
    LOCALE_ID = 'en_US'
    
    print("event ",event)
    
    
    if 'body' in event and event['body'] is not None:
        try:
            input_data = json.loads(event['body'])
            user_message = input_data['messages'][0]['unstructured']['text']
            print(user_message)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error parsing input: {str(e)}")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': "application/json",
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    "error": "Invalid JSON format"
                })
            }
    else:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': "application/json",
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                "error": "Missing request body"
            })
        }
        
    try:
        # Initiate conversation with Lex
        lex_response = lex_client.recognize_text(
        botId=BOT_ID,         
        botAliasId=BOT_ALIAS_ID,    
        localeId=LOCALE_ID,
        sessionId="jugal",
        text=user_message)
        
        # Log the Lex response for debugging
        logger.info(f"Lex response: {lex_response}")
        print("Lex Response:", lex_response)
        
        # Check if Lex returned any messages
        if 'messages' in lex_response:
            lex_message = lex_response['messages'][0]['content']
        else:
            lex_message = "Sorry, I didn't understand that."
            
        message_id = str(uuid.uuid4()) 
        timestamp = datetime.now(timezone.utc).isoformat() 
        
        # Create the response message
        res_message = {
            "messages": [{
                "type": "text",
                "unstructured": {
                    "id": message_id,
                    "text": lex_message,
                    "timestamp": timestamp
                }
            }]
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': "application/json",
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                "error": str(e)
            })
        }
        
    #Return response from Lex
    return{
        'statusCode': 200,
        'headers': {
            'Content-Type': "application/json",
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(res_message)
    }
       