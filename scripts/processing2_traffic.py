# task 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import expr
from pyspark.sql.functions import to_date, col ,date_format , to_timestamp , lower



spark = (
    SparkSession.builder
    .appName("transform2")

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
    "s3a://datalake/raw/Motor_Vehicle_Collisions_Crashes.csv",
    inferSchema=True,
    header=True
)



#----------------------------------------------------------
# معالجه الداتا الي في الفايل 
#----------------------------------------------------------



# تحدبد الاعمده الي محتجنيها بس 
df_selected=df.select("COLLISION_ID","CRASH DATE","CRASH TIME","BOROUGH","VEHICLE TYPE CODE 1"
              ,"ON STREET NAME","NUMBER OF PERSONS INJURED","NUMBER OF PERSONS KILLED"
              , "CONTRIBUTING FACTOR VEHICLE 1") 

#----------------------------------
# تغير الداتا تيب بتاع عمود التاريخ والقيم الي مش هتفع تبقا تاريخ تيبقا نل
df1 = df_selected.withColumn(
    "CRASH DATE",
    expr("try_to_date(`CRASH DATE`, 'M/d/yyyy')")
)

#----------------------------------
#  في عمود الديت هنعمل فلتره بحيث نجيب  القيم الي مش بنل 
df2 = df1.filter(col("CRASH DATE").isNotNull())

#----------------------------------
# فلتره الداتا لعام 2025 بس 
df_2025_plus = df2.filter(
    col("CRASH DATE") > "2025-01-01"
)


#----------------------------------
# هنحاول اننا نعدل القيم الفاضيه بنل دي بعدين ان كان في وقت 
# بس دلوقتي هنحط  القيم الفاضيه دي unknown
df_fill = df_2025_plus.fillna({
    "BOROUGH": "UNKNOWN",          # أي Null في BOROUGH هيتحول لـ "UNKNOWN"
    "ON STREET NAME": "UNKNOWN STREET" , # أي Null في ON STREET NAME هيتحول لـ "UNKNOWN STREET"
    "NUMBER OF PERSONS INJURED":"0", # عدد الاصابات الي بنل هحط مكانه 0
    "VEHICLE TYPE CODE 1":"Sedan" # عندي 1417 قيمه  بنل هعوضهم  ب اكتر قيمه اتكرت 
    
})

#----------------------------------------------------------
# تحويل الداتا تايب بتاع العمود الاصابات الي  int 
df_4 = df_fill.withColumn(
    "NUMBER OF PERSONS INJURED",
    col("NUMBER OF PERSONS INJURED").cast("int")
)
#----------------------------------
# تحويل الداتا الي في العمود بتاع الوقت لصيغه ديت
df_time = df_4.withColumn(
    "CRASH TIME",
    to_timestamp(col("CRASH TIME"), "H:mm")
)

#----------------------------------
# HH:MM تحديد ان الديت يبقا بصيغه الساعه والدقائق فقط  
df_final = df_time.withColumn(
    "CRASH TIME",
    date_format("CRASH TIME", "HH:MM")
)

#=========================================
# تغير اسماء الاعمده
rename_dict = {
    "CRASH DATE": "crash_date",
    "CRASH TIME": "crash_time",
    "ON STREET NAME": "street_name",
    "NUMBER OF PERSONS INJURED": "persons_injured",
    "NUMBER OF PERSONS KILLED": "persons_killed",
    "VEHICLE TYPE CODE 1": "vehicle_type",
    "CONTRIBUTING FACTOR VEHICLE 1": "contributing_factor",
    "COLLISION_ID": "collision_id",
    "BOROUGH": "borough"
}

for old, new in rename_dict.items():
    df_final = df_final.withColumnRenamed(old, new)

# هنحول الداتا الي في الاعمده كلها لسمول  
df_final = df_final.withColumn("borough", lower(col("borough")))
df_final = df_final.withColumn("vehicle_type", lower(col("vehicle_type")))
df_final = df_final.withColumn("street_name", lower(col("street_name")))
df_final = df_final.withColumn("contributing_factor", lower(col("contributing_factor")))

df_final.show(10)



# هنحفظ الفايل الجديد علي س3 في الفولدر بتاع الداتا المتعالجه 
df_final.write \
    .mode("overwrite") \
    .option("header","true") \
    .csv("s3a://datalake/processed/Motor_Vehicle_Collisions_Crashes")


