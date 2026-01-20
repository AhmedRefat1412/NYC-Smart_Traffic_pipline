#task 6
from pyspark.sql.functions import monotonically_increasing_id , col
from pyspark.sql import SparkSession

#------------------------------------------
# create Fact_Collisions
def Fact_Collisions(Motor_Vehicle,Dim_Date,Dim_Borough,Dim_Vehicle_Type,Dim_Street,Dim_Contributing):
     
     # هنعمل جوين مع ال Dim_Date     
     fact_collisions = Motor_Vehicle.join(
         Dim_Date,
         Motor_Vehicle.crash_date == Dim_Date.full_date,
         "left"
      )
     #------------------------------------------------
     #هنعمل جوين مع Dim_Borough
     fact_collisions = fact_collisions.join(
         Dim_Borough,
         fact_collisions.borough == Dim_Borough.borough,
         "left"
     )

     #------------------------------------------------
     # هنعمل جوين مع Dim_Vehicle_Type
     fact_collisions= fact_collisions.join(
         Dim_Vehicle_Type,
         fact_collisions.vehicle_type==Dim_Vehicle_Type.vehicle_name,
         "left"
     )

     #--------------------------------------------------
     # هنعمل جوين مع Dim_Street
     fact_collisions= fact_collisions.join(
         Dim_Street,
         fact_collisions.street_name==Dim_Street.street_name,
         "left"
     )

     #--------------------------------------------------
     # هنعمل جوين مع Dim_Contributing
     fact_collisions= fact_collisions.join(
          Dim_Contributing,
         fact_collisions.contributing_factor==Dim_Contributing.contributing_factor,
        "left"
     )

     #--------------------------------------------------------
     # هنبدء بقا نختار الاعمده الي عوزينها بس عشان نكون الفاكت تيبول بتاعنا
     fact_collisions=fact_collisions.select(
         "collision_id",
         "date_key",
         "borough_id",
         "vehicle_type_id",
         "street_id",
         "factor_key",
         "crash_time",
         "persons_injured",
         "persons_killed"
     )
     #--------------------------------------------------------------------
     #  نعرض عينة للتأكد
     #fact_collisions.show(10)
     return(fact_collisions)

#------------------------------------------
#write in s3
def write_fact(df,path):
     df.write \
     .mode("overwrite") \
     .option("header","true") \
     .csv(path)

#------------------------------------------
# create Fact_Complaint
def Fact_Complaint(complaint_311,Dim_Date,Dim_Borough,Dim_Status,Dim_Traffic_Volume_Level,Dim_Street,Dim_Weather,Dim_Complaint):
     
     # هنعمل جوين مع ال Dim_Date     
     fact_complaint = complaint_311.join(
         Dim_Date,
         complaint_311.created_date == Dim_Date.full_date,
         "left"
     )
     #------------------------------------------------
     #هنعمل جوين مع Dim_Borough
     fact_complaint = fact_complaint.join(
         Dim_Borough,
         fact_complaint.borough == Dim_Borough.borough,
         "left"
     )

     #------------------------------------------------
     # هنعمل جوين مع Dim_Status
     fact_complaint= fact_complaint.join(
         Dim_Status,
         fact_complaint.status==Dim_Status.status,
         "left"
     )

     #--------------------------------------------------
     # هنعمل جوين مع Dim_Traffic_Volume_Level
     fact_complaint= fact_complaint.join(
         Dim_Traffic_Volume_Level,
         fact_complaint.traffic_volume_level==Dim_Traffic_Volume_Level.volume_level,
         "left"
     )

     #--------------------------------------------------
     # هنعمل جوين مع Dim_Street
     fact_complaint= fact_complaint.join(
         Dim_Street,
         fact_complaint.street_name==Dim_Street.street_name,
         "left"
     )
     #--------------------------------------------------
     # هنعمل جوين مع Dim_Weather
     fact_complaint= fact_complaint.join(
         Dim_Weather,
         fact_complaint.weather_condition==Dim_Weather.weather_condition,
         "left"
     )
     #--------------------------------------------------
     # هنعمل جوين مع Dim_Complaint
     fact_complaint= fact_complaint.join(
         Dim_Complaint,
         fact_complaint.complaint_type==Dim_Complaint.complaint_type,
         "left"
     )


     #--------------------------------------------------------
     # هنبدء بقا نختار الاعمده الي عوزينها بس عشان نكون الفاكت تيبول بتاعنا
     fact_complaint=fact_complaint.select(
         "complaint_311_id",
         "date_key",
         "borough_id",
         "status_key",
         "street_id",
         "volume_level_key",
         "complaint_key",
         "weather_key",
         "severity",
         "pepole_injuries",
         "pepole_killed",
         "response_time_minutes",
         "vehicles_involved"
     )
     #--------------------------------------------------------------------
     
     return(fact_complaint)




def main():
     
     spark = (
         SparkSession.builder
         .appName("dw(facts)")

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
    
     #===============================================
     # هنجيب اول داتا ونحطها في داتا فريم streets_data
     streets_data = spark.read.csv(
         "s3a://datalake/processed/streets_data",
         header=True,
         inferSchema=True
     )

     #===============================================
     # هنجيب ثاني  داتا ونحطها في داتا فريم Motor_Vehicle_Collisions_Crashes
     Motor_Vehicle = spark.read.csv(
         "s3a://datalake/processed/Motor_Vehicle_Collisions_Crashes",
         header=True,
         inferSchema=True
     )

     #===============================================
     # هنجيب ثالث  داتا ونحطها في داتا فريم mongo_traffic
     complaint_311 = spark.read.csv(
         "s3a://datalake/processed/complaint_311",
         header=True,
         inferSchema=True
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
     # git Dim_Weather
     Dim_Weather = spark.read.csv(
         "s3a://datalake/data_warehouse(dim)/Dim_Weather",
         header=True,
         inferSchema=True
     )
       #===============================================
     # git Dim_Vehicle_Type
     Dim_Vehicle_Type = spark.read.csv(
         "s3a://datalake/data_warehouse(dim)/Dim_Vehicle_Type",
         header=True,
         inferSchema=True
     )






     facts={
          "Fact_Collisions":Fact_Collisions(Motor_Vehicle,Dim_Date,Dim_Borough,Dim_Vehicle_Type,Dim_Street,Dim_Contributing),
          "Fact_Complaint":Fact_Complaint(complaint_311,Dim_Date,Dim_Borough,Dim_Status,Dim_Traffic_Volume_Level,Dim_Street,Dim_Weather,Dim_Complaint),
         
     }
     for fact_name , fact_df in facts.items():
          write_fact(fact_df,f"s3a://datalake/data_warehouse(fact)/{fact_name}")


if __name__ == "__main__":
     main()
