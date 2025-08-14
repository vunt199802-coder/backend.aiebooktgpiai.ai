import boto3
import os

# S3 Client
S3_CLIENT = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("S3_REGION")
)
S3_REGION = os.getenv('S3_REGION')
region = os.getenv('S3_REGION')

# DynamoDB Resource
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("DYNAMODB_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

EBOOK_TAG_TABLE = dynamodb.Table('id_and_tag')
rewards_table = dynamodb.Table('Rewards')
quizzes_table = dynamodb.Table('quizzes')
ebook_tag_table = dynamodb.Table('id_and_tag')
highlights_table = dynamodb.Table('highlights')

reading_history_table = dynamodb.Table('reading_history')
ebook_table = dynamodb.Table('ebook-store')
reading_statistics_table = dynamodb.Table('reading_statistics')
ic_numbers_table = dynamodb.Table('IC_Numbers')
rewards_table = dynamodb.Table('Rewards')

cognito = boto3.client(
    'cognito-idp', 
    region_name=os.getenv("COGNITO_REGION"), 
    aws_access_key_id=os.getenv("DYNAMODB_ACCESS_KEY_ID"), 
    aws_secret_access_key=os.getenv("DYNAMODB_SECRET_ACCESS_KEY")
)