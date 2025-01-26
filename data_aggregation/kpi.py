from pyspark.sql import SparkSession
import requests
import boto3

aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = "nasa-neo-transformation"
folder_prefix="kpi/"

# Initialize Spark session

def kpi_data():
    
    spark = SparkSession.builder \
        .appName("Asteroid Data Analysis") \
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
        
        
        
    # Load the Asteroid data from S3
    union_df = spark.read.parquet("s3a://nasa-neo-transformation/hist_union_live/asteroid_union_data/")

    # Create a temporary view for SQL queries
    union_df.createOrReplaceTempView("AsteroidData")

    # 1. Total Close Approaches
    total_close_approaches = spark.sql("""
        SELECT COUNT(*) AS Total_Close_Approaches 
        FROM AsteroidData
    """)
    total_close_approaches.write.parquet("s3a://nasa-neo-transformation/kpi/total_close_approaches")

    # 2. Monthly Average Close Approaches
    monthly_avg_close_approaches = spark.sql("""
    SELECT Month,
           Close_Approaches_Count,
           AVG(Close_Approaches_Count) OVER (PARTITION BY Year) AS Avg_Monthly_Close_Approaches
    FROM (
        SELECT DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
               COUNT(*) AS Close_Approaches_Count,
               YEAR(close_approach_date) AS Year
        FROM AsteroidData
        GROUP BY DATE_FORMAT(close_approach_date, 'yyyy-MM'), YEAR(close_approach_date)
    ) AS MonthlyCloseApproaches
""")
    
    monthly_avg_close_approaches.write.parquet("s3a://nasa-neo-transformation/kpi/monthly_avg_close_approaches")

    # 3. Yearly Average Close Approaches
    yearly_avg_close_approaches = spark.sql("""
        WITH cte AS (
            SELECT YEAR(close_approach_date) AS Year,
                COUNT(*) AS Close_Approaches_Count
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        )
        SELECT Year,
            Close_Approaches_Count,
            AVG(Close_Approaches_Count) OVER () AS Avg_Yearly_Close_Approaches
        FROM cte
        ORDER BY Year
    """)
    yearly_avg_close_approaches.write.parquet("s3a://nasa-neo-transformation/kpi/yearly_avg_close_approaches")

    # 4. Peak Month
    peak_month = spark.sql("""
        WITH cte AS (
            SELECT DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
                COUNT(*) AS Close_Approaches_Count
            FROM AsteroidData
            GROUP BY DATE_FORMAT(close_approach_date, 'yyyy-MM')
        )
        SELECT Month, Close_Approaches_Count
        FROM cte
        ORDER BY Close_Approaches_Count DESC
        LIMIT 1
    """)
    peak_month.write.parquet("s3a://nasa-neo-transformation/kpi/peak_month")

    # 5. Peak Year
    peak_year = spark.sql("""
        WITH cte AS (
            SELECT YEAR(close_approach_date) AS Year,
                COUNT(*) AS Close_Approaches_Count
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        )
        SELECT Year, Close_Approaches_Count
        FROM cte
        ORDER BY Close_Approaches_Count DESC
        LIMIT 1
    """)
    peak_year.write.parquet("s3a://nasa-neo-transformation/kpi/peak_year")

    # 6. Monthly Change in Close Approaches
    monthly_change = spark.sql("""
        WITH cte AS (
            SELECT DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
                COUNT(*) AS Close_Approaches_Count
            FROM AsteroidData
            GROUP BY DATE_FORMAT(close_approach_date, 'yyyy-MM')
        ),
        cte2 AS (
            SELECT Month,
                Close_Approaches_Count,
                LAG(Close_Approaches_Count) OVER (ORDER BY Month) AS Prev_Close_Approaches_Count
            FROM cte
        )
        SELECT Month,
            Close_Approaches_Count,
            Prev_Close_Approaches_Count,
            (Close_Approaches_Count - Prev_Close_Approaches_Count) AS Month_Change
        FROM cte2
    """)
    monthly_change.write.parquet("s3a://nasa-neo-transformation/kpi/monthly_change")

    # 7. Yearly Change in Close Approaches
    yearly_change = spark.sql("""
        WITH cte AS (
            SELECT YEAR(close_approach_date) AS Year,
                COUNT(*) AS Close_Approaches_Count
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        ),
        cte2 AS (
            SELECT Year,
                Close_Approaches_Count,
                LAG(Close_Approaches_Count) OVER (ORDER BY Year) AS Prev_Close_Approaches_Count
            FROM cte
        )
        SELECT Year,
            Close_Approaches_Count,
            Prev_Close_Approaches_Count,
            (Close_Approaches_Count - Prev_Close_Approaches_Count) AS Year_Change
        FROM cte2
    """)
    yearly_change.write.parquet("s3a://nasa-neo-transformation/kpi/yearly_change/")

    # Stop the Spark session
    spark.stop()
