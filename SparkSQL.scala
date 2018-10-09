// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("SparkSql").getOrCreate()

import spark.implicits._

// COMMAND ----------

val mySchema = StructType(Array(
StructField("sid", StringType, true),
StructField("date", DateType, true),
StructField("mtype", StringType, true),
StructField("value", DoubleType, true)))

// COMMAND ----------

val df = spark.read
.schema(mySchema)
.option("dateFormat", "yyyyMMdd")
.format("csv")
.load("/FileStore/tables/2017.csv").cache()

// COMMAND ----------

df.printSchema()
df.show(5)

// COMMAND ----------

// max temperature of 2017
val dfTmax = df
.filter(col("mtype") === "TMAX")
.limit(1000)
.drop("mtype")
.withColumnRenamed("value", "tmax")

// COMMAND ----------

dfTmax.show(5)

// COMMAND ----------

//min temperature of 2017
val dfTmin = df.filter(col("mtype") === "TMIN").limit(1000).drop("mtype").withColumnRenamed("value", "tmin")

// COMMAND ----------

dfTmin.show(5)

// COMMAND ----------

//creating joined dataframe
val joinedDf = dfTmax.join(dfTmin, Seq("sid", "date"), "inner")

// COMMAND ----------

joinedDf.show(5)

// COMMAND ----------

//average temperature
val avgDf = joinedDf
.select(col("sid"), col("date"), (col("tmax") + col("tmin") / 2))
.withColumnRenamed("(tmax + (tmin / 2))", "avgTemp")

// COMMAND ----------

avgDf.show(5)

// COMMAND ----------

//schema for reading the other file
val sSchema = StructType(Array(
StructField("sid", StringType, true),
StructField("lat", DoubleType, true),
StructField("lon", DoubleType, true),
StructField("name", StringType, true)))

// COMMAND ----------

//creating rdd of row
import org.apache.spark.SparkContext._
val filesRdd = spark.sparkContext.textFile("/FileStore/tables/ghcnd_stations-948fb.txt")
val stationRdd = filesRdd.map{line =>
  val id = line.substring(0,11)
  val lon = line.substring(12,20).toDouble
  val lan = line.substring(21,30).toDouble
  val name = line.substring(41, 71)
  Row(id, lon, lan, name)
}


// COMMAND ----------

//create dataframe from rdd and manual schema

val stationDf = spark.createDataFrame(stationRdd, sSchema)

// COMMAND ----------

stationDf.show(5)

// COMMAND ----------

val stationTemp = avgDf.groupBy(col("sid")).agg(avg(col("avgTemp")))

// COMMAND ----------

stationTemp.show(5)

// COMMAND ----------

val stationJoined = stationTemp.join(stationDf, "sid")

// COMMAND ----------

stationJoined.show(5)
