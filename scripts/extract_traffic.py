#task 1
from pymongo import MongoClient
import json
import boto3

# اتصال Mongo
client = MongoClient("mongodb://mongodb:27017")
db = client["nyc_data"]
collection = db["nyc_traffic"]

#-----------------------------------------------
# connect with s3 
s3 = boto3.resource(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123'
)
Bucket="datalake"

#------------------------------------------------
# create bucket لو مش موجود 
try:
     s3.create_bucket(Bucket="datalake")
except:
     pass

#------------------------------------------------
# create 2 folder (raw , processed)
try:
     s3.put_object(Bucket="datalake", Key="raw/")
     s3.put_object(Bucket="datalake", Key="processed/")
except:
     pass

#----------------------------------------------------------
# upload first file to s3 (json file)
cursor = collection.find({}, {"_id": 0})

data = list(cursor)          # in-memory
json_bytes = json.dumps(
    data,
    ensure_ascii=False
).encode("utf-8")

#load to s3
s3.Bucket("datalake").put_object(
    Key="raw/mongo_traffic.json",
    Body=json_bytes,
    ContentType="application/json"
)


#---------------------------------------------------==
# upload second file csv to s3
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123'
)


s3_client.upload_file(
    Filename="/opt/airflow/data/Motor_Vehicle_Collisions_Crashes.csv",  # المسار  للفايل
    Bucket="datalake",                    # اسم الباكيت في S3/MinIO
    Key="raw/Motor_Vehicle_Collisions_Crashes.csv",    # المسار داخل الباكيت
    ExtraArgs={"ContentType": "text/csv"}   # دي زي ميتا داتا بعرفه ايه الكونتنت الي جوا الفايل ده 

)

#--------------------------------------
# upload the last file 

s3_client.upload_file(
    Filename="/opt/airflow/data/Centerline.csv",  # المسار المحلي للفايل
    Bucket="datalake",                    # اسم الباكيت في S3/MinIO
    Key="raw/Centerline.csv" ,   # المسار داخل الباكيت
    ExtraArgs={"ContentType": "application/octet-stream"}  # دي زي ميتا داتا بعرفه ايه الكونتنت الي جوا الفايل ده 

)


