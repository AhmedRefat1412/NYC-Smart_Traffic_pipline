#task 7
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("load")
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.4.0,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "net.snowflake:spark-snowflake_2.13:2.15.0-spark_3.4") 
    
    # MinIO Config
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

    
#===============================================
# 1-git Dim_Borough
Dim_Borough = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Borough",
     header=True,
     inferSchema=True
)

#===============================================
# 2-git Dim_Complaint
Dim_Complaint = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Complaint",
     header=True,
     inferSchema=True
)
    
#===============================================
# 3- git Dim_Contributing
Dim_Contributing = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Contributing",
     header=True,
     inferSchema=True
)
#===============================================
# 4-git Dim_Status
Dim_Status = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Status",
     header=True,
     inferSchema=True
)
#===============================================
# 5-git Dim_Date
Dim_Date = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Date",
     header=True,
     inferSchema=True
)
#===============================================
# 6-git Dim_Street
Dim_Street = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Street",
     header=True,
     inferSchema=True
)
#===============================================
# 7-git Dim_Traffic_Volume_Level
Dim_Traffic_Volume_Level = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Traffic_Volume_Level",
     header=True,
     inferSchema=True
)
#===============================================
# 8-git Dim_Weather
Dim_Weather = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Weather",
     header=True,
     inferSchema=True
)
#===============================================
# 9-git Dim_Vehicle_Type
Dim_Vehicle_Type = spark.read.csv(
     "s3a://datalake/data_warehouse(dim)/Dim_Vehicle_Type",
     header=True,
     inferSchema=True
)

#========================================================================================================
# 1-git Fact_Complaint
Fact_Complaint = spark.read.csv(
     "s3a://datalake/data_warehouse(fact)/Fact_Complaint",
     header=True,
     inferSchema=True
)

#===============================================
# 2-git Fact_Collisions
Fact_Collisions = spark.read.csv(
     "s3a://datalake/data_warehouse(fact)/Fact_Collisions",
     header=True,
     inferSchema=True
)

#================================================
# connect with snowflake 
sf_options = {
    "sfURL": "dpaatty-ts25706.snowflakecomputing.com",
    "sfUser": "#$#4%$#4",
    "sfPassword": "$$$@#3@",
    "sfDatabase": "NYC_DW",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}

#==================================================
def write_to_snowflake(df, table_name, sf_options, mode="overwrite"):
    df.write \
      .format("snowflake") \
      .options(**sf_options) \
      .option("dbtable", table_name) \
      .mode(mode) \
      .save()


dimensions = {
    "Dim_Borough": Dim_Borough,
    "Dim_Complaint": Dim_Complaint,
    "Dim_Contributing": Dim_Contributing,
    "Dim_Status": Dim_Status,
    "Dim_Date": Dim_Date,
    "Dim_Street": Dim_Street,
    "Dim_Traffic_Volume_Level": Dim_Traffic_Volume_Level,
    "Dim_Weather": Dim_Weather,
    "Dim_Vehicle_Type": Dim_Vehicle_Type
}

for table_name, df in dimensions.items():
    write_to_snowflake(df, table_name, sf_options, mode="overwrite")



facts = {
    "Fact_Collisions": Fact_Collisions,
    "Fact_Complaint": Fact_Complaint
}

for table_name, df in facts.items():
    write_to_snowflake(df, table_name, sf_options, mode="overwrite")
