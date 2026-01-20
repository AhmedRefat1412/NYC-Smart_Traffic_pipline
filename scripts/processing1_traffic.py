# task 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , lower ,to_date
spark = (
    SparkSession.builder
    .appName("transform1")

    # ✅ Hadoop S3A jars 
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()
)

#spark.sparkContext.setLogLevel("WARN")
# هنجيب الداتا بقا ونحولها لداتا فريم

df = spark.read.json(
    "s3a://datalake/raw/mongo_traffic.json"
)

#--------------------------------------------------------
# تعديل القيم في الاعمده لتصبح كلها سمول 

cols_to_lower = ["borough", "incident_type", "severity", "status", "street_name", "weather_condition"]

for c in cols_to_lower:
    df = df.withColumn(c, lower(col(c)))

#--------------------------------------------------------
# تغير الداتا تايب بتاع الديت 
df1 = df.withColumn(
    "created_date", to_date(col("created_date"), "yyyy-MM-dd"))

# تغير اسماء الاعمده 
rename_coulmn={
    "fatalities":"pepole_killed",
    "incident_type":"complaint_type",
    "incident_id":"complaint_311_id",
    "injuries_reported":"pepole_injuries",
}

for  old,new in rename_coulmn.items():
    df1=df1.withColumnRenamed(old,new)

#------------------------------------
df1.write \
    .mode("overwrite") \
    .option("header","true") \
    .csv("s3a://datalake/processed/complaint_311")

df1.show(10)

