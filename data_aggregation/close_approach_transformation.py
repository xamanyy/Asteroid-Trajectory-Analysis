from pyspark.sql import SparkSession
import requests
import boto3

# Initialize the Spark session

aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = "nasa-neo-transformation"
folder_prefix="close-approach-analysis"

def close_approach_analysis():

    spark = SparkSession.builder \
        .appName("S3 Data Fetch") \
        .config("spark.hadoop.fs.s3a.access.key", "") \
        .config("spark.hadoop.fs.s3a.secret.key", "") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
        .getOrCreate()
        
        
    union_df = spark.read.parquet("s3a://nasa-neo-transformation/hist_union_live/asteroid_union_data/")
    
    union_df.createOrReplaceTempView("AsteroidData")
    
    aws_access_key_id = 'AKIAQEIP3OY76BNSW74J'
    aws_secret_access_key = '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'
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
        

     # MONTHLY AVERAGE CLOSE APPROACHES
    monthly_avg_df = spark.sql('''
        WITH cte AS (
            SELECT 
                DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
                COUNT(*) AS total_approaches,
                YEAR(close_approach_date) AS year
            FROM AsteroidData
            GROUP BY 
                DATE_FORMAT(close_approach_date, 'yyyy-MM'),
                YEAR(close_approach_date)
        )
        SELECT 
            Month,
            total_approaches AS `Close_Approaches_Count`,
            AVG(total_approaches) OVER (PARTITION BY year) AS `Avg_Monthly_Approaches`
        FROM cte
    ''')

    monthly_avg_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_monthly_average_close_approaches")

    # YEARLY AVERAGE CLOSE APPROACHES
    yearly_avg_df = spark.sql('''
        WITH cte AS (
            SELECT 
                YEAR(close_approach_date) AS year,
                COUNT(*) AS total_approaches
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        )
        SELECT 
            year AS `Year`,
            total_approaches AS `Total_Approaches`,
            AVG(total_approaches) OVER () AS `Avg_Yearly_Approaches`
        FROM cte
        ORDER BY year
    ''')

    yearly_avg_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_yearly_average_close_approaches")

    # Identifying Peak Months (Highest number of asteroid close approaches)
    monthly_highest_df = spark.sql('''
        WITH cte AS (
            SELECT
                DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
                COUNT(*) AS total_approaches
            FROM AsteroidData
            GROUP BY DATE_FORMAT(close_approach_date, 'yyyy-MM')
        ),
        cte2 AS (
            SELECT 
                Month,
                total_approaches,
                DENSE_RANK() OVER (ORDER BY total_approaches DESC) AS rnk
            FROM cte
        )
        SELECT 
            Month,
            total_approaches AS `Total_Approaches`
        FROM cte2
        WHERE rnk = 1
    ''')

    monthly_highest_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_monthly_no_close_approaches")

    # Identifying Peak Years (Highest number of asteroid close approaches)
    yearly_highest_df = spark.sql('''
        WITH cte AS (
            SELECT
                YEAR(close_approach_date) AS year,
                COUNT(*) AS total_approaches
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        ),
        cte2 AS (
            SELECT 
                year,
                total_approaches,
                DENSE_RANK() OVER (ORDER BY total_approaches DESC) AS rnk
            FROM cte
        )
        SELECT 
            year AS `Year`,
            total_approaches AS `Total_Approaches`
        FROM cte2
        WHERE rnk = 1
    ''')

    yearly_highest_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_yearly_no_close_approach")

    # Monthly Trends (Month-over-month changes in asteroid close approaches)
    mom_trend_df = spark.sql('''
        WITH cte AS (
            SELECT 
                DATE_FORMAT(close_approach_date, 'yyyy-MM') AS Month,
                YEAR(close_approach_date) AS year,
                COUNT(*) AS total_approaches
            FROM AsteroidData
            GROUP BY DATE_FORMAT(close_approach_date, 'yyyy-MM'), YEAR(close_approach_date)
        ),
        cte2 AS (
            SELECT
                Month,
                total_approaches,
                LAG(total_approaches, 1, NULL) OVER (PARTITION BY year ORDER BY Month) AS prev_month_approaches
            FROM cte
        )
        SELECT
            Month,
            total_approaches AS `Total_Approaches`,
            prev_month_approaches AS `Prev_Month_Approaches`,
            (total_approaches - prev_month_approaches) AS `MoM_Change`
        FROM cte2
    ''')

    mom_trend_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_monthly_trend_analysis")

    # Yearly Trends (Year-over-year changes in asteroid close approaches)
    yoy_trend_df = spark.sql('''
        WITH cte AS (
            SELECT 
                YEAR(close_approach_date) AS year,
                COUNT(*) AS total_approaches
            FROM AsteroidData
            GROUP BY YEAR(close_approach_date)
        ),
        cte2 AS (
            SELECT 
                year,
                total_approaches,
                LAG(total_approaches, 1, NULL) OVER (ORDER BY year) AS prev_year_approaches
            FROM cte
        )
        SELECT 
            year AS `Year`,
            total_approaches AS `Total_Approaches`,
            prev_year_approaches AS `Prev_Year_Approaches`,
            (total_approaches - prev_year_approaches) AS `YoY_Change`
        FROM cte2
    ''')

    yoy_trend_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/close-approach-analysis/asteroid_yearly_trend")
