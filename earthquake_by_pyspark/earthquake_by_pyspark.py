from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import os,requests
from pyspark.sql.functions import col, current_timestamp, explode, regexp_extract, from_unixtime, count
# from utils import historic_data_accesssing
from  google.cloud import bigquery
from pyspark.sql import functions as f



if __name__ == '__main__':


    os.environ ['GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Demp_Pycharm\GCP\gcp_sessions\my-bwt-learning-2024-a6b2387d3aeb.json"
    spark = (SparkSession.builder.appName("transformation_questions")
            .getOrCreate())

    #accessing Historic raw data from gcs bucket
    path=r"gs://historic_data_earth/bronze_historic_data/raw_data_20241022.json"
    #
    bronze_dataframe_month_data=spark.read.json(path,mode = "PERMISSIVE",multiLine = True)
    #
    # #
    explode_data = bronze_dataframe_month_data.select (explode ( "features" ).alias ( "feature" ) )
    # # # #
    silver_historic = (explode_data.select (
        col ( "feature.type" ). alias ( "feature_type" ) ,
        col ( "feature.properties.mag" ).alias ( "magnitude" ) ,
        col ( "feature.properties.place" ).alias ( "place" ) ,
        col ( "feature.properties.time" ).alias ( "time" ) ,
        col ( "feature.properties.updated" ).alias ( "updated" ) ,
        col ( "feature.properties.tz" ).alias ( "timezone" ) ,
        col ( "feature.properties.url" ).alias ( "event_url" ) ,
        col ( "feature.properties.detail" ).alias ( "detail_url" ) ,
        col ( "feature.properties.felt" ).alias ( "felt" ) ,
        col ( "feature.properties.cdi" ).alias ( "cdi" ) ,
        col ( "feature.properties.mmi" ).alias ( "mmi" ) ,
        col ( "feature.properties.alert" ).alias ( "alert" ) ,
        col ( "feature.properties.status" ).alias ( "status" ) ,
        col ( "feature.properties.tsunami" ).alias ( "tsunami" ) ,
        col ( "feature.properties.sig" ).alias ( "sig" ) ,
        col ( "feature.properties.net" ).alias ( "net" ) ,
        col ( "feature.properties.code" ).alias ( "code" ) ,
        col ( "feature.properties.ids" ).alias ( "ids" ) ,
        col ( "feature.properties.sources" ).alias ( "sources" ) ,
        col ( "feature.properties.types" ).alias ( "type_properties" ) ,
        col ( "feature.properties.nst" ).alias ( "nst" ) ,
        col ( "feature.properties.dmin" ).alias ( "dmin" ) ,
        col ( "feature.properties.rms" ).alias ( "rms" ) ,
        col ( "feature.properties.gap" ).alias ( "gap" ) ,
        col ( "feature.properties.magType" ).alias ( "magType" ) ,
        col ( "feature.properties.type" ).alias ( "earthquake_type" ) ,
        col ( "feature.properties.title" ).alias ( "earthquake_title" ) ,
        col ( "feature.geometry.coordinates" ) [0].alias ( "geometry_longitude" ) ,
        col ( "feature.geometry.coordinates" ) [1].alias ( "geometry_latitude" ) ,
        col ( "feature.geometry.coordinates" ) [2].alias ( "geometry_depth"))
                              .withColumn ( "time" , from_unixtime ( col ( "time" ) / 1000 ) )
                              .withColumn ( "updated" , from_unixtime ( col ( "updated" ) / 1000 ) )
                              .withColumn ( "area" , regexp_extract ( "place" , r"\b(?:of\s+)([A-Za-z\s,]+)$", 1))
                              .withColumn("area",col("area").cast("string")))
    # # #

    silver_historic.write.mode("overwrite").parquet(
        r"gs://historic_data_earth/historic_silver_data/silver_historic_data.json")

    #
    #
    #
    #writing data to bigquery warehouse after adding column insert_dt

    gold_historic_data = silver_historic.withColumn("insert_dt", current_timestamp())

    gold_layer_monthly_formation =( gold_historic_data .write.format("bigquery")
                          .option("table", "my-bwt-learning-2024.earthquake_database.earthquake_table")
                          .option("autodetect", "true")
                          .option("createDisposition", "CREATE_IF_NEEDED")
                          .option("temporaryGcsBucket", "bwtp1")
                          .mode("overwrite")
                          .save())


    ##daily_data
    path=r"gs://daily_data_earth/bronze_daily_data/raw_data_20241022.json"
    daily_eartquake_dataframe = spark.read.json(path, mode = "PERMISSIVE", multiLine = True)

    explode_data1= daily_eartquake_dataframe.select( explode( "features").alias( "feature"))
    silver_daily= (explode_data1.select (
        col ( "feature.type" ).alias ( "feature_type" ),
        col ( "feature.properties.mag" ).alias ( "magnitude" ) ,
        col ( "feature.properties.place" ).alias ( "place" ) ,
        col ( "feature.properties.time" ).alias ( "time" ) ,
        col ( "feature.properties.updated" ).alias ( "updated" ) ,
        col ( "feature.properties.tz" ).alias ( "timezone" ) ,
        col ( "feature.properties.url" ).alias ( "event_url" ) ,
        col ( "feature.properties.detail" ).alias ( "detail_url" ) ,
        col ( "feature.properties.felt" ).alias ( "felt" ) ,
        col ( "feature.properties.cdi" ).alias ( "cdi" ) ,
        col ( "feature.properties.mmi" ).alias ( "mmi" ) ,
        col ( "feature.properties.alert" ).alias ( "alert" ) ,
        col ( "feature.properties.status" ).alias ( "status" ) ,
        col ( "feature.properties.tsunami" ).alias ( "tsunami" ) ,
        col ( "feature.properties.sig" ).alias ( "sig" ) ,
        col ( "feature.properties.net" ).alias ( "net" ) ,
        col ( "feature.properties.code" ).alias ( "code" ) ,
        col ( "feature.properties.ids" ).alias ( "ids" ) ,
        col ( "feature.properties.sources" ).alias ( "sources" ) ,
        col ( "feature.properties.types" ).alias ( "type_properties" ) ,
        col ( "feature.properties.nst" ).alias ( "nst" ) ,
        col ( "feature.properties.dmin" ).alias ( "dmin" ) ,
        col ( "feature.properties.rms" ).alias ( "rms" ) ,
        col ( "feature.properties.gap" ).alias ( "gap" ) ,
        col ( "feature.properties.magType" ).alias ( "magType" ) ,
        col ( "feature.properties.type" ).alias ( "earthquake_type" ) ,
        col ( "feature.properties.title" ).alias ( "earthquake_title" ) ,
        col ( "feature.geometry.coordinates" ) [0].alias ( "geometry_longitude" ) ,
        col ( "feature.geometry.coordinates" ) [1].alias ( "geometry_latitude" ) ,
        col ( "feature.geometry.coordinates" ) [2].alias ("geometry_depth" ))
                              .withColumn ( "time" , from_unixtime ( col ( "time" ) / 1000 ) )
                              .withColumn ( "updated" , from_unixtime ( col ( "updated" ) / 1000 ) )
                              .withColumn ( "area" , regexp_extract ( "place" , r"\b(?:of\s+)([A-Za-z\s,]+)$" , 1 ) ))




    silver_daily.write.mode("overwrite").parquet(r"gs://daily_data_earth/daily_silver_data/daily_silver.json")
    golden_daily= silver_daily.withColumn("insert_dt",current_timestamp())
    golden_daily.write.mode("overwrite").parquet(r"gs://daily_data_earth/daily_golden_data/daily_golden.json")

    #taking_data_from_bigquery

    df_from_historic_goldenlayer = (spark.read
                                    .format ( "bigquery" )
                                    .option ("parentProject", "my-bwt-learning-2024" )
                                    .load ("earthquake_database.earthquake_table"))

    # #non matching data
    anti_left=golden_daily.join(df_from_historic_goldenlayer, ["ids"], "anti")
    anti_right=df_from_historic_goldenlayer.join(golden_daily, ["ids"], "anti")
    non_matching_record_tobe_updated=anti_right.union(anti_left)
    #
    # #matching record
    matching_record_current_dataframe_tobe_updated=golden_daily.join(silver_historic,["ids"],"semi")
    #
    # #incremental load-scd1
    gold_layer_updated=non_matching_record_tobe_updated.union(matching_record_current_dataframe_tobe_updated).orderBy(col("ids").asc())
    #
    gold_layer_updated = gold_layer_updated.withColumn("insert_dt", current_timestamp())

    gold_layer_incremental= (gold_layer_updated.write.format ( "bigquery" )
                            .option ( "table" , "my-bwt-learning-2024.earthquake_database.earthquake_table")
                            .option ( "autodetect" , "true" )
                            .option ( "createDisposition" , "CREATE_IF_NEEDED" )
                            .option ( "temporaryGcsBucket" , "bwtp1" )
                            .mode ( "overwrite" )
                            .save ())

    # Corrected Analysis Queries
# 1. Count the number of earthquakes by region
df_earthquake_count = gold_layer_updated.groupBy("area").agg(count("*").alias("no_earthquakes"))

# 2. Find the average magnitude by the region
df_avg_mag = gold_layer_updated.groupBy("area").agg(f.avg("magnitude").alias("avg_magnitude"))

# 3. Count earthquakes happening on the same day
df_earthquake_sameday = gold_layer_updated.groupBy(f.to_date("time")).agg(count("*").alias("noofearthquakes"))

# 4. Count earthquakes happening on the same day and in the same region
df_earth_dayregion = gold_layer_updated.groupBy(f.to_date("time").alias("date"), "area").agg(
    count("*").alias("no_of_earthquakes"))

# 5. Find average earthquakes happening on the same day.
df_avg_earthquakes = gold_layer_updated.groupBy(f.to_date("time").alias("date")).agg(
    (count("*") / 2).alias("avg_earthquakes"))

# 6. Find average earthquakes happening on the same day and in the same region
df_avg_day_region = gold_layer_updated.groupBy(f.to_date("time").alias("date"), "area").agg(
    (count("*") / 2).alias("avg_earthquakes"))

# 7. Find the region name with the highest magnitude earthquake last week
one_week_ago = (datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%d')
df_highest_mag = gold_layer_updated.filter(f.to_date("time") >= one_week_ago).orderBy(f.desc("magnitude")).select(
    "area").limit(1)

# 8. Find the region name having magnitudes higher than 5
df_morethan5_mag = gold_layer_updated.filter(col("magnitude") > 5).select("area")






