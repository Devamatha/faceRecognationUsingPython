import boto3

# Initialize clients for S3, Rekognition, and DynamoDB
s3 = boto3.client('s3')
rekognition = boto3.client('rekognition', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# DynamoDB table for employees
dynamodbTableName = 'facerecognition'
employeeTable = dynamodb.Table(dynamodbTableName)

def lambda_handler(event, context):
    print(event)
    
    # Extract bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Index employee image
        response = index_employee_image(bucket, key)
        print(response, "key " + key + " bucket " + bucket)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            faceId = response['FaceRecords'][0]['Face']['FaceId']
            FacePrintsName = key
            
            # Register or update employee in DynamoDB
            register_employee(faceId, FacePrintsName)
        return response
    except Exception as e:
        print(e)
        print(f"Error processing employee image {key} from bucket {bucket}.")
        raise e

def index_employee_image(bucket, key):
    # Use Rekognition to index faces
    response = rekognition.index_faces(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        CollectionId="imageComparision"
    )
    return response

def register_employee(newFaceId, FacePrintsName):
    print(newFaceId, "new FaceId")
    print(FacePrintsName, "FacePrintsName")
    
    # Check if FacePrintsName already exists in DynamoDB
    existing_items = employeeTable.scan(
        FilterExpression="FacePrintsName = :name",
        ExpressionAttributeValues={
            ":name": FacePrintsName
        }
    )
    
    if existing_items['Items']:
        # FacePrintsName exists, get the old RekognitionId
        oldFaceId = existing_items['Items'][0]['RekognitionId']
        print(f"Existing FacePrintsName found with FaceId {oldFaceId}. Replacing with new FaceId {newFaceId}.")
        
        # Remove the old face from Rekognition collection
        rekognition.delete_faces(
            CollectionId="imageComparision",
            FaceIds=[oldFaceId]
        )
        print("Old FaceId deleted in Rekognition collection.")
        
        # Delete the old record from DynamoDB
        employeeTable.delete_item(
            Key={
                'RekognitionId': oldFaceId  # Deleting the old item
            }
        )
        print(f"Deleted old record with FaceId {oldFaceId}.")
    
    # Insert a new record with the new FaceId
    employeeTable.put_item(
        Item={
            'RekognitionId': newFaceId,   # Insert new primary key
            'FacePrintsName': FacePrintsName
        }
    )
    print(f"Inserted new record for {FacePrintsName} with FaceId {newFaceId}.")
