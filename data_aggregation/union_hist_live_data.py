from pyspark.sql import SparkSession
import boto3

aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = "nasa-neo-transformation"
folder_prefix="hist_union_live/"

    # Initialize the Spark session
spark = SparkSession.builder \
    .appName("S3 Data Fetch") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
    .getOrCreate()
    

spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

        # List all files in the folder
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    
    # If there are files in the folder, delete them
if 'Contents' in response:
    for obj in response['Contents']:
        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Deleted {obj['Key']}")
else:
    print("No files found to delete in this folder.")
    

    # Read historical and live data
df_hist = spark.read.parquet("s3a://nasa-neo-data-hist/*/*/*")
df_live = spark.read.parquet("s3a://nasa-neo-data-live/*/*/*")

union_df = df_hist.union(df_live)
    
    # Assuming 'df' is your DataFrame
union_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/hist_union_live/asteroid_union_data/")

    # Show the data
# union_df.show(10)

spark.stop()
    
    
    
    
