from pyspark.sql import SparkSession
import requests
import boto3

# Initialize the Spark session

aws_access_key_id = ''
aws_secret_access_key = ''
bucket_name = "nasa-neo-transformation"
folder_prefix="clustering-based-analysis/"

def cluster_analysis():

    spark = SparkSession.builder \
        .appName("S3 Data Fetch") \
        .config("spark.hadoop.fs.s3a.access.key", "") \
        .config("spark.hadoop.fs.s3a.secret.key", "") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
        .config("spark.hadoop.fs.s3a.region", "ap-south-1") \
        .getOrCreate()
        
        
    union_df = spark.read.parquet("s3a://nasa-neo-transformation/hist_union_live/asteroid_union_data/")
    
    union_df.createOrReplaceTempView("asteroid")
    
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

    # -- 1. Clustering of Asteroids Based on Hazardous Potential and Close Approach Date
    # '''
    # Problem:
    # Group asteroids based on their potential hazard level and the timing of their close approaches to identify 
    # potential future risks.
    # '''

    hazardous_close_approach_df = spark.sql('''
        WITH ClusteredAsteroids AS (
            SELECT 
                id,
                name,
                is_potentially_hazardous_asteroid,
                close_approach_date,
                -- Cluster based on hazardous potential
                CASE
                    WHEN is_potentially_hazardous_asteroid = 1 THEN 'Hazardous'
                    ELSE 'Non-Hazardous'
                END AS hazard_cluster,
                -- Cluster based on close approach date (near future, far future)
                CASE
                    WHEN close_approach_date <= CURRENT_DATE + INTERVAL 1 MONTH THEN 'Near Future'
                    ELSE 'Far Future'
                END AS approach_cluster
            FROM 
                asteroid
        )

        SELECT 
            hazard_cluster,
            approach_cluster,
            COUNT(id) AS num_asteroids,
            MIN(close_approach_date) AS first_approach,
            MAX(close_approach_date) AS last_approach
        FROM 
            ClusteredAsteroids
        GROUP BY 
            hazard_cluster, approach_cluster
        ORDER BY 
            hazard_cluster, approach_cluster;
    ''')
    
    hazardous_close_approach_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/clustering-based-analysis/asteroid_hazardous_close_approach_clusters")


    # -- 2 Clustering of Asteroids Based on Size and Velocity
    # ''' 
    # Problem Statement:
    # Group asteroids based on their size (diameter) and velocity to identify patterns such as smaller but 
    # faster asteroids or larger but slower asteroids'''

    size_velocity_df = spark.sql('''
    WITH ClusteredAsteroids AS (
        SELECT 
            id,
            name,
            estimated_diameter_min_kilometers,
            relative_velocity_kilometers_per_second,
            -- Create a cluster based on diameter size
            CASE
                WHEN estimated_diameter_min_kilometers <= 1 THEN 'Small'
                WHEN estimated_diameter_min_kilometers BETWEEN 1 AND 5 THEN 'Medium'
                ELSE 'Large'
            END AS size_cluster,
            
            -- Create a cluster based on velocity range
            CASE
                WHEN relative_velocity_kilometers_per_second <= 10 THEN 'Slow'
                WHEN relative_velocity_kilometers_per_second BETWEEN 10 AND 25 THEN 'Moderate'
                ELSE 'Fast'
            END AS velocity_cluster
        FROM 
            asteroid
    )

    SELECT 
        size_cluster,
        velocity_cluster,
        COUNT(id) AS num_asteroids,
        AVG(estimated_diameter_min_kilometers) AS avg_diameter,
        AVG(relative_velocity_kilometers_per_second) AS avg_velocity,
        MIN(estimated_diameter_min_kilometers) AS min_diameter,
        MAX(estimated_diameter_min_kilometers) AS max_diameter,
        MIN(relative_velocity_kilometers_per_second) AS min_velocity,
        MAX(relative_velocity_kilometers_per_second) AS max_velocity
    FROM 
        ClusteredAsteroids
    GROUP BY 
        size_cluster, velocity_cluster
    ORDER BY 
        size_cluster, velocity_cluster;
    ''')


    size_velocity_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/clustering-based-analysis/asteroid_size_velocity_cluster")

    # -- 3. Clustering of Asteroids Based on Miss Distance and Orbiting Body
    # '''
    # Problem:
    # Group asteroids based on miss distance and the orbiting body they approach (e.g., Earth, Mars, etc.).
    # '''

    distance_orbit_body_df = spark.sql('''
        WITH ClusteredAsteroids AS (
            SELECT 
                id,
                name,
                orbiting_body,
                miss_distance_astronomical,
                -- Cluster based on miss distance range
                CASE
                    WHEN miss_distance_astronomical <= 0.1 THEN 'Very Close'
                    WHEN miss_distance_astronomical BETWEEN 0.1 AND 1 THEN 'Moderate'
                    ELSE 'Far'
                END AS distance_cluster
            FROM 
                asteroid
        )

        SELECT 
            orbiting_body,
            distance_cluster,
            COUNT(id) AS num_asteroids,
            AVG(miss_distance_astronomical) AS avg_miss_distance,
            MIN(miss_distance_astronomical) AS min_miss_distance,
            MAX(miss_distance_astronomical) AS max_miss_distance
        FROM 
            ClusteredAsteroids
        GROUP BY 
            orbiting_body, distance_cluster
        ORDER BY 
            orbiting_body, distance_cluster;
    ''')



    distance_orbit_body_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/clustering-based-analysis/asteroid_distance_orbit_body_clusters")


    #  -- 4 Clustering of Asteroids by Absolute Magnitude and Diameter
    # '''
    # Problem:
    # Identify clusters of asteroids based on their absolute magnitude (brightness) and diameter to find patterns linking 
    # brightness and size.
    # '''

    abs_magnitude_diameter_df = spark.sql(
    '''
    WITH ClusteredAsteroids AS (
        SELECT 
            id,
            name,
            absolute_magnitude_h,
            estimated_diameter_min_kilometers,
            -- Cluster based on absolute magnitude (brightness)
            CASE
                WHEN absolute_magnitude_h <= 15 THEN 'Bright'
                WHEN absolute_magnitude_h BETWEEN 15 AND 20 THEN 'Moderately Bright'
                ELSE 'Dim'
            END AS magnitude_cluster,
            
            -- Cluster based on diameter size
            CASE
                WHEN estimated_diameter_min_kilometers <= 1 THEN 'Small'
                WHEN estimated_diameter_min_kilometers BETWEEN 1 AND 5 THEN 'Medium'
                ELSE 'Large'
            END AS diameter_cluster
        FROM 
            asteroid
    )

    SELECT 
        magnitude_cluster,
        diameter_cluster,
        COUNT(id) AS num_asteroids,
        AVG(absolute_magnitude_h) AS avg_absolute_magnitude,
        AVG(estimated_diameter_min_kilometers) AS avg_diameter,
        MIN(absolute_magnitude_h) AS min_absolute_magnitude,
        MAX(absolute_magnitude_h) AS max_absolute_magnitude
    FROM 
        ClusteredAsteroids
    GROUP BY 
        magnitude_cluster, diameter_cluster
    ORDER BY 
        magnitude_cluster, diameter_cluster;
    '''   
    )
    
    abs_magnitude_diameter_df.write.mode("overwrite").parquet("s3a://nasa-neo-transformation/clustering-based-analysis/clustered_asteroids_by_magnitude_and_diameter")