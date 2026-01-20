# task 4
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_date, col ,date_format , to_timestamp , lower

spark = (
    SparkSession.builder
    .appName("transform3")

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
#==============================================================
# هنجيب الداتا  ونحولها لداتا فريم

df = spark.read.csv(
    "s3a://datalake/raw/Centerline.csv",
    header=True,
    inferSchema=True
)

#--------------------------------------
#هنجيب الاعمده الي محتجينها بس 
df=df.select("STREET NAME","GlobalID","Number Park Lanes","Number Travel Lanes","Snow Priority",
          "Street Width","BIKE_LANE")

#--------------------------------------
# fillna
df=df.fillna(
    {
        "Number Park Lanes":0, # تعويض الارقام الي ب نل ب 0
        "Number Travel Lanes":2,
        "Snow Priority":"UNKNOWN",
        "Street Width":30,
        "BIKE_LANE":0

    }
)
#--------------------------------------
# تغير اسماء الاعمده 
rename_dic={
    "STREET NAME":"street_name",
    "GlobalID":"global_id",
    "Number Park Lanes":"number_park_lanes",
    "Number Travel Lanes":"number_travel_lanes",
    "Snow Priority":"snow_priority",
    "Street Width":"street_width",
    "BIKE_LANE":"bike_lane"

}

for old , new in rename_dic.items():
     df=df.withColumnRenamed(old, new)
#  هنحول القيم الي في الاعمده الاسترينج الي قيم كلها سمول 
df = df.withColumn("street_name",lower(col("street_name")))


df.show(10)
#--------------------------------------
# هنحفظ الفايل الجديد علي س3 في الفولدر بتاع الداتا المتعالجه 
df.write \
     .mode("overwrite") \
     .option("header","true") \
     .csv("s3a://datalake/processed/streets_data")