#task 5
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window





# 1-create Dim_Borough
def Dim_Borough(df):
        dim_borough=df.select("borough").distinct()

        #  نضيف عمود ID فريد لكل بورغو
        dim_borough = dim_borough.withColumn("borough_id", monotonically_increasing_id())

        # 4- نرتب الأعمدة بحيث يكون ID الأول
        dim_borough = dim_borough.select("borough_id", "borough")

        # 5- نعرض عينة للتأكد
        #dim_borough.show(10, truncate=False)
        return(dim_borough)
    
#==================================================
#  2-create Dim_Status
def Dim_Status(df):
     dim_status=df.select("status").distinct()

     #  نضيف عمود ID فريد لكل بورغو
     dim_status = dim_status.withColumn("status_key", monotonically_increasing_id())

     # 4- نرتب الأعمدة بحيث يكون ID الأول
     dim_status = dim_status.select("status_key", "status")

    # 5- نعرض عينة للتأكد
     dim_status.show(10, truncate=False)

     return(dim_status)


#=================================================
# 3-create Dim_Traffic_Volume_Level
def Dim_Traffic_Volume_Level(df):
     dim_traffic_volume_level=df.select("traffic_volume_level").distinct()

     #  نضيف عمود ID فريد لكل بورغو
     dim_traffic_volume_level = dim_traffic_volume_level.withColumn("volume_level_key", monotonically_increasing_id())

     # - نرتب الأعمدة بحيث يكون ID الأول
     dim_traffic_volume_level = dim_traffic_volume_level.select("volume_level_key", "traffic_volume_level")

     #تغير اسماء الاعمده 
     dim_traffic_volume_level=dim_traffic_volume_level.withColumnRenamed("traffic_volume_level","volume_level")


     #  5- نعرض عينة للتأكد
     #dim_traffic_volume_level.show(10, truncate=False)
     return(dim_traffic_volume_level)


#====================================================
# 4-create Dim_Vehicle_Type 
def Dim_Vehicle_Type(df):
     dim_vehicle_type=df.select("vehicle_type").distinct()

     #  نضيف عمود ID فريد لكل سياره 
     dim_vehicle_type = dim_vehicle_type.withColumn("vehicle_type_id", monotonically_increasing_id())

     #  نرتب الأعمدة بحيث يكون ID الأول
     dim_vehicle_type = dim_vehicle_type.select("vehicle_type_id", "vehicle_type")


     #تغير اسماء الاعمده 
     dim_vehicle_type=dim_vehicle_type.withColumnRenamed("vehicle_type","vehicle_name")

     #  نعرض عينة للتأكد
     #print(dim_vehicle_type.count())
     return(dim_vehicle_type)


#============================================================
# 5-create Dim_Weather 
def Dim_Weather(df):
     dim_weather=df.select("weather_condition").distinct()

     #  نضيف عمود ID فريد لكل طقس 
     dim_weather = dim_weather.withColumn("weather_key", monotonically_increasing_id())

     #  نرتب الأعمدة بحيث يكون ID الأول
     dim_weather = dim_weather.select("weather_key", "weather_condition")

     #  نعرض عينة للتأكد
     #print(dim_weather.count())
     #dim_weather.show()
     return(dim_weather)


#============================================================
# 6-create Dim_Contributing
def Dim_Contributing(df):
     dim_contributing=df.select("contributing_factor").distinct()

     #  نضيف عمود ID فريد لكل سبب حادث 
     dim_contributing = dim_contributing.withColumn("factor_key", monotonically_increasing_id())

     #  نرتب الأعمدة بحيث يكون ID الأول
     dim_contributing = dim_contributing.select("factor_key", "contributing_factor")

     #  نعرض عينة للتأكد
     #print(dim_contributing.count())
     #dim_contributing.show()
     return(dim_contributing)

#=====================================================================

# 7-create Dim_Street 
def Dim_Street(df1,df2,df3):

     # هنحاول اننا نجمع اسماء الشوارع الي في ال 3 داتا فريم هنا 
     # مع العلم ان الشوارع الي في الداتا فريمز الي هي غير بتاعت الشارع ملهاش قيم غير اسم الشارع بنحاول اننا نعوض ده ب فيمه نن

     # في الاول هنجيب اسماء الشوارع الي الداتا فريم بتاعت الحوادث 
     streets_collisions = df1.select("street_name").distinct()

     #  وبعدين نجيب الشوارع الي داتا فريم بتاعت complaint_311
     streets_complaint_311= df2.select("street_name").distinct()

     #  وهنا هنجيب اسماء الشوارع من الداتا فريم الي خاصه بشوارع اصلا 
     streets_streets_data=df3.select(
         "street_name",
         "number_park_lanes",
         "number_travel_lanes",
         "snow_priority",
         "street_width",
         "bike_lane")


     dim_street = (
         streets_streets_data
    .unionByName(
        streets_collisions
        .withColumn("number_park_lanes", lit(None))
        .withColumn("number_travel_lanes", lit(None))
        .withColumn("street_width", lit(None))
        .withColumn("bike_lane", lit(None))
        .withColumn("snow_priority", lit(None)),
        allowMissingColumns=True
    )
              .unionByName(
               streets_complaint_311
              .withColumn("number_park_lanes", lit(None))
              .withColumn("number_travel_lanes", lit(None))
              .withColumn("street_width", lit(None))
              .withColumn("bike_lanes", lit(None))
              .withColumn("snow_priority", lit(None)),
              allowMissingColumns=True

             )
        )

     #هنمسح  الشوارع المكرره 
     dim_street =dim_street.dropDuplicates(['street_name'])

     #  نضيف عمود ID فريد لكل شارع  
     dim_street = dim_street.withColumn("street_id", monotonically_increasing_id())

     #  نرتب الأعمدة بحيث يكون ID الأول
     dim_street = dim_street.select(
         "street_id", 
         "street_name",
         "number_park_lanes",
         "number_travel_lanes",
         "snow_priority",
         "street_width",
         "bike_lane")

     # هنحاول اننا نملي القيم الفاضيه بقيم مناسبه 

     dim_street=dim_street.fillna({
         "snow_priority":"UNKNOWN",
         "street_width":30,
         "bike_lane":0,
         "number_park_lanes":1,
         "number_travel_lanes":2

         })
     #  نعرض عينة للتأكد
     #print(dim_street.count())
     #dim_street.show(10)
     return(dim_street)

#=====================================================
# 8-create Dim_Complaint
def Dim_Complaint(df):
     dim_complaint=df.select("complaint_type")
    
     #هنمسح  انواع الشكاوي  المكرره  المكرره 
     dim_complaint =dim_complaint.dropDuplicates(['complaint_type'])

     #  نضيف عمود ID فريد لكل شارع  
     dim_complaint = dim_complaint.withColumn("complaint_key", monotonically_increasing_id())

     #  نرتب الأعمدة بحيث يكون ID الأول
     dim_complaint = dim_complaint.select(
         "complaint_key", 
         "complaint_type")

     #  نعرض عينة للتأكد
     #print(dim_complaint.count())
     #dim_complaint.show(10)
     return(dim_complaint)


#=======================================================
# 9-Dim_Date 
def Dim_Date(spark):
     dim_date = spark.sql("""
         SELECT explode(
             sequence(
                 to_date('2025-01-01'),
                  to_date('2025-12-31'),
                  interval 1 day
                          
                 )
             ) AS date
      """) 
     window_spec = Window.orderBy("date")

     dim_date = dim_date.select(
         F.row_number().over(window_spec).alias("date_key"),   # key فريد 1 → 365
         F.date_format("date", "dd-MM-yyyy").alias("full_date"),
         F.date_format("date", "EEEE").alias("day_name"),
         F.date_format("date", "MMMM").alias("month_name")
     )
         

     # تغير الداتا تايب بتاع الديت 
     dim_date = dim_date.withColumn(
          "full_date",
         F.to_date(F.col("full_date"),"dd-MM-yyyy")
         )

     #dim_date.show(10, truncate=False)
     return(dim_date)


#=====================================================
# witer in s3 
def write_dimention (df ,path):
        
    df.write \
          .mode("overwrite") \
          .option("header","true") \
          .csv(path)

#===================================================
#main def 
def main():
     
     spark = (
         SparkSession.builder
         .appName("dw")

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

     # هنجيب اول داتا ونحطها في داتا فريم streets_data
     streets_data = spark.read.csv(
         "s3a://datalake/processed/streets_data",
         header=True,
         inferSchema=True
     )

     # هنجيب ثاني  داتا ونحطها في داتا فريم Motor_Vehicle_Collisions_Crashes
     Motor_Vehicle = spark.read.csv(
         "s3a://datalake/processed/Motor_Vehicle_Collisions_Crashes",
         header=True,
         inferSchema=True
     )

     # هنجيب ثالث  داتا ونحطها في داتا فريم mongo_traffic
     complaint_311 = spark.read.csv(
         "s3a://datalake/processed/complaint_311",
         header=True,
         inferSchema=True
     )
     
     dimentions={
          "Dim_Borough":Dim_Borough(Motor_Vehicle),
          "Dim_Status":Dim_Status(complaint_311),
          "Dim_Traffic_Volume_Level":Dim_Traffic_Volume_Level(complaint_311),
          "Dim_Vehicle_Type":Dim_Vehicle_Type(Motor_Vehicle),
          "Dim_Weather":Dim_Weather(complaint_311),
          "Dim_Contributing":Dim_Contributing(Motor_Vehicle),
          "Dim_Street":Dim_Street(Motor_Vehicle,complaint_311,streets_data),
          "Dim_Complaint":Dim_Complaint(complaint_311),
          "Dim_Date":Dim_Date(spark)
     }
     for dim_name , dim_df in dimentions.items():
          write_dimention(dim_df,f"s3a://datalake/data_warehouse(dim)/{dim_name}")


if __name__ == "__main__":
     main()