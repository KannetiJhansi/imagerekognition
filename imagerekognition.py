import boto3
import json
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')
sns_client = boto3.client('sns')

def detect_widgets(image_bytes):
    required_widgets = ["Dog", "Animal", "Mammal"]
    min_confidence = 90
    
    response = rekognition_client.detect_labels(Image={'Bytes': image_bytes}, MinConfidence=min_confidence)
    detected_labels = {label['Name']: label['Confidence'] for label in response['Labels']}
    
    success = all(label in detected_labels and detected_labels[label] >= min_confidence for label in required_widgets)
    return success, detected_labels

def lambda_handler(event, context):
    try:
        # Limiting concurrency to process only one event at a time
        event_count = len(event['Records'])
        if event_count > 1:
            logger.info(f"Received {event_count} S3 events. Processing one event at a time.")
            return
        
        for record in event['Records']:
            # Get the bucket and key of the S3 object triggering the lambda function
            s3_event = json.loads(record['body'])['Records'][0]['s3']
            bucket = s3_event['bucket']['name']
            key = s3_event['object']['key']
            
            # Skip if the event originated from the analyzed folders
            if key.startswith("analyzed/"):
                continue
            
            logger.info(f"Processing image {key}...")
            
            # Get the object from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            image_bytes = response['Body'].read()
            
            # Detect widgets in the image
            success, detected_labels = detect_widgets(image_bytes)
            
            # Define the destination folder based on success or failure
            destination_folder = "analyzed/success" if success else "analyzed/failure"
            
            # Move the object to the appropriate folder in the same S3 bucket
            new_key = f"{destination_folder}/{key.split('/')[-1]}"
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=new_key
            )
            
           # Send email notification
            if success:
                email_subject = "Image Processing Success"
                email_message = f"Hi Team,\n\nThe image {key} is processed and is a dog.\n\nThank you!"
            else:
                email_subject = "Image Processing Failure"
                email_message = f"Hi Team,\n\nThe image {key} is processed and is not a dog.\n\nThank you!"
                
            sns_message = email_message
            sns_subject = email_subject
            sns_topic_arn = 'arn:aws:sns:us-east-1:851725399843:sendemail'  
            response = sns_client.publish(TopicArn=sns_topic_arn, Subject=sns_subject, Message=sns_message)
            
            # Delete the object from the input bucket
            s3_client.delete_object(Bucket=bucket, Key=key)
            
            logger.info(f"Image moved to {new_key} in bucket {bucket}")
            logger.info(f"Image {key} deleted from bucket {bucket}")
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Send message to DLQ or handle error as required
