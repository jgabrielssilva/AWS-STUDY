import boto3
import json


def get_db_credentials(secret_name):
    client = boto3.client('secretsmanager', region_name='us-east-2')
    response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(response['SecretString'])
    return secrets