import boto3
import json

def lambda_handler(event, context):
    openmeteo_s3_path = event["openmeteo_key"]
    seventimer_s3_path = event["7timer_key"]
    
    openmeteo_key = openmeteo_s3_path.replace("s3://esgi-lyon-iabd-m2-cloud/", "")
    seventimer_key = seventimer_s3_path.replace("s3://esgi-lyon-iabd-m2-cloud/", "")
    
    s3 = boto3.client("s3")
    
    openmeteo_data = json.loads(
        s3.get_object(Bucket="esgi-lyon-iabd-m2-cloud", Key=openmeteo_key)
        ["Body"].read().decode()
    )
    
    seventimer_data = json.loads(
        s3.get_object(Bucket="esgi-lyon-iabd-m2-cloud", Key=seventimer_key)
        ["Body"].read().decode()
    )
    
    data = {}
    
    return {"status_code": 200}

