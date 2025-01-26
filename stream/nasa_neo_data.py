import requests
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timedelta
from kafka import KafkaProducer,KafkaConsumer
import json


BUCKET_NAME = "nasa-neo-data-real-time"
aws_access_key_id = ''
aws_secret_access_key = ''
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
kafka_topic = "nasa-neo-data"
start_date = datetime.today()

def nasa_neo_data_api(**kwargs):

    current_date = start_date
    
    # NASA NEO API
    nasa_api_url = "https://api.nasa.gov/neo/rest/v1/feed"
    
   
    params = {
        "start_date": current_date.strftime('%Y-%m-%d'),
        "end_date": current_date.strftime('%Y-%m-%d'),
        "api_key": "DEMO_KEY"
    }
                
    response = requests.get(nasa_api_url, params=params)
    data = response.json()
    
    kwargs['ti'].xcom_push(key='rows', value=data)
        
    return "Done"


def produce_nasa_data(**kwargs):
    
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='read_data_nasa_neo')
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    if data:
        producer.send(kafka_topic, value=data)
        producer.flush()
        print(f"Data sent to Kafka topic {kafka_topic}")
    producer.close()
    
    
def nasa_neo_consumer_data(**kwargs):

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    message = next(consumer)

    data = message.value
    print("Received  -->",data)
        
    total_count = data['element_count']
    data_raw = data['near_earth_objects'][start_date.strftime('%Y-%m-%d')]
    
    kwargs['ti'].xcom_push(key='rows', value=data_raw)
    kwargs['ti'].xcom_push(key='total_count', value=total_count)

    return "Done"
        

def nasa_neo_data_transformation(**kwargs):
    
    total_count = kwargs['ti'].xcom_pull(key='total_count', task_ids='consumer_data')
    data_raw = kwargs['ti'].xcom_pull(key='rows', task_ids='consumer_data')
    
    print(total_count)

    neo_data = []
    for i in range(total_count):        
        x = data_raw[i]
        values = {
            'id' : x['id'],
            'name': x['name'],
            'nasa_jpl_url': x['nasa_jpl_url'],
            'absolute_magnitude_h': x['absolute_magnitude_h'],
            'estimated_diameter_min_kilometers': x['estimated_diameter']['kilometers']['estimated_diameter_min'],
            'estimated_diameter_max_kilometers': x['estimated_diameter']['kilometers']['estimated_diameter_max'],
            'estimated_diameter_min_meters': x['estimated_diameter']['meters']['estimated_diameter_min'],
            'estimated_diameter_max_meters': x['estimated_diameter']['meters']['estimated_diameter_max'],
            'estimated_diameter_min_miles': x['estimated_diameter']['miles']['estimated_diameter_min'],
            'estimated_diameter_max_miles': x['estimated_diameter']['miles']['estimated_diameter_max'],
            'estimated_diameter_min_feet': x['estimated_diameter']['feet']['estimated_diameter_min'],
            'estimated_diameter_max_feet': x['estimated_diameter']['feet']['estimated_diameter_max'],
            'is_potentially_hazardous_asteroid': x['is_potentially_hazardous_asteroid'],
            'close_approach_date': x['close_approach_data'][0]['close_approach_date'],
            'close_approach_date_full': x['close_approach_data'][0]['close_approach_date_full'],
            'epoch_date_close_approach': x['close_approach_data'][0]['epoch_date_close_approach'],
            'relative_velocity_kilometers_per_second': x['close_approach_data'][0]['relative_velocity']['kilometers_per_second'],
            'relative_velocity_kilometers_per_hour': x['close_approach_data'][0]['relative_velocity']['kilometers_per_hour'],
            'relative_velocity_miles_per_hour': x['close_approach_data'][0]['relative_velocity']['miles_per_hour'],
            'miss_distance_astronomical': x['close_approach_data'][0]['miss_distance']['astronomical'],
            'miss_distance_lunar': x['close_approach_data'][0]['miss_distance']['lunar'],
            'miss_distance_kilometers': x['close_approach_data'][0]['miss_distance']['kilometers'],
            'miss_distance_miles': x['close_approach_data'][0]['miss_distance']['miles'],
            'orbiting_body': x['close_approach_data'][0]['orbiting_body'],
            'is_sentry_object': x['is_sentry_object']
        }
        neo_data.append(values)
        
    kwargs['ti'].xcom_push(key='rows', value=neo_data)
    
    return "Done"
        
# Function to upload data to S3
def upload_to_s3(**kwargs):
    
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_data')
    
    df = pd.DataFrame(data)
    
    if data:
        # Get year, month, and day from the current date
        year = start_date.year
        month = start_date.month
        day = start_date.day

        # Define the S3 path (e.g., year/month/day.json)
        s3_path = f"{year}/{month:02d}/{day:02d}.parquet"
        
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer)
    
        # Upload Parquet file to S3
        buffer.seek(0)  # Rewind the buffer to the start
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_path, Body=buffer.read())
        
        
        # df.to_parquet(s3_path, index=False, storage_options={"key": aws_access_key_id, "secret": aws_secret_access_key})
        print(f"Uploaded data for {year}-{month:02d}-{day:02d} to S3")
        
    return "Done"
   
        
        
        
        
        
     