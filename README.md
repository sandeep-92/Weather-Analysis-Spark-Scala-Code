# Weather-Analysis-Spark-Scala-Code

//Weather Analysis done in Spark using Dataframe API
//files can be downloaded from :


import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession.builder().appName("SparkSql").getOrCreate()

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@50d4a68d
import spark.implicits._

val mySchema = StructType(Array(
StructField("sid", StringType, true),
StructField("date", DateType, true),
StructField("mtype", StringType, true),
StructField("value", DoubleType, true)))

mySchema: org.apache.spark.sql.types.StructType = StructType(StructField(sid,StringType,true), StructField(date,DateType,true), StructField(mtype,StringType,true), StructField(value,DoubleType,true))

val df = spark.read
.schema(mySchema)
.option("dateFormat", "yyyyMMdd")
.format("csv")
.load("/FileStore/tables/2017.csv").cache()

df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [sid: string, date: date ... 2 more fields]
df.printSchema()
df.show(5)

root
 |-- sid: string (nullable = true)
 |-- date: date (nullable = true)
 |-- mtype: string (nullable = true)
 |-- value: double (nullable = true)

+-----------+----------+-----+-----+
|        sid|      date|mtype|value|
+-----------+----------+-----+-----+
|US1MISW0005|2017-01-01| PRCP|  0.0|
|US1MISW0005|2017-01-01| SNOW|  0.0|
|US1MISW0005|2017-01-01| SNWD|  0.0|
|US1MNCV0008|2017-01-01| PRCP|  0.0|
|US1MNCV0008|2017-01-01| SNOW|  0.0|
+-----------+----------+-----+-----+
only showing top 5 rows

// max temperature of 2017
val dfTmax = df
.filter(col("mtype") === "TMAX")
.limit(1000)
.drop("mtype")
.withColumnRenamed("value", "tmax")

dfTmax: org.apache.spark.sql.DataFrame = [sid: string, date: date ... 1 more field]

dfTmax.show(5)
+-----------+----------+------+
|        sid|      date|  tmax|
+-----------+----------+------+
|ASN00015643|2017-01-01| 274.0|
|ASN00085296|2017-01-01| 217.0|
|ASN00085280|2017-01-01| 215.0|
|ASN00040209|2017-01-01| 293.0|
|CA005030984|2017-01-01|-109.0|
+-----------+----------+------+
only showing top 5 rows

//min temperature of 2017
val dfTmin = df.filter(col("mtype") === "TMIN").limit(1000).drop("mtype").withColumnRenamed("value", "tmin")

dfTmin: org.apache.spark.sql.DataFrame = [sid: string, date: date ... 1 more field]

dfTmin.show(5)
+-----------+----------+------+
|        sid|      date|  tmin|
+-----------+----------+------+
|ASN00015643|2017-01-01| 218.0|
|ASN00085296|2017-01-01| 127.0|
|ASN00085280|2017-01-01| 156.0|
|ASN00040209|2017-01-01| 250.0|
|CA005030984|2017-01-01|-192.0|
+-----------+----------+------+
only showing top 5 rows

//creating joined dataframe
val joinedDf = dfTmax.join(dfTmin, Seq("sid", "date"), "inner")

joinedDf: org.apache.spark.sql.DataFrame = [sid: string, date: date ... 2 more fields]

joinedDf.show(5)
+-----------+----------+------+------+
|        sid|      date|  tmax|  tmin|
+-----------+----------+------+------+
|ASN00015643|2017-01-01| 274.0| 218.0|
|ASN00085296|2017-01-01| 217.0| 127.0|
|ASN00085280|2017-01-01| 215.0| 156.0|
|ASN00040209|2017-01-01| 293.0| 250.0|
|CA005030984|2017-01-01|-109.0|-192.0|
+-----------+----------+------+------+
only showing top 5 rows

//average temperature
val avgDf = joinedDf
.select(col("sid"), col("date"), (col("tmax") + col("tmin") / 2))
.withColumnRenamed("(tmax + (tmin / 2))", "avgTemp")

avgDf: org.apache.spark.sql.DataFrame = [sid: string, date: date ... 1 more field]

avgDf.show(5)
+-----------+----------+-------+
|        sid|      date|avgTemp|
+-----------+----------+-------+
|ASN00015643|2017-01-01|  383.0|
|ASN00085296|2017-01-01|  280.5|
|ASN00085280|2017-01-01|  293.0|
|ASN00040209|2017-01-01|  418.0|
|CA005030984|2017-01-01| -205.0|
+-----------+----------+-------+
only showing top 5 rows

//schema for reading the other file
val sSchema = StructType(Array(
StructField("sid", StringType, true),
StructField("lat", DoubleType, true),
StructField("lon", DoubleType, true),
StructField("name", StringType, true)))

sSchema: org.apache.spark.sql.types.StructType = StructType(StructField(sid,StringType,true), StructField(lat,DoubleType,true), StructField(lon,DoubleType,true), StructField(name,StringType,true))
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

import org.apache.spark.SparkContext._

filesRdd: org.apache.spark.rdd.RDD[String] = /FileStore/tables/ghcnd_stations-948fb.txt MapPartitionsRDD[95] at textFile at command-2199024747775294:3
stationRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[96] at map at command-2199024747775294:4
//create dataframe from rdd and manual schema

val stationDf = spark.createDataFrame(stationRdd, sSchema)
stationDf: org.apache.spark.sql.DataFrame = [sid: string, lat: double ... 2 more fields]
stationDf.show(5)
+-----------+-------+--------+--------------------+
|        sid|    lat|     lon|                name|
+-----------+-------+--------+--------------------+
|ACW00011604|17.1167|-61.7833|ST JOHNS COOLIDGE...|
|ACW00011647|17.1333|-61.7833|ST JOHNS         ...|
|AE000041196| 25.333|  55.517|SHARJAH INTER. AI...|
|AEM00041194| 25.255|  55.364|DUBAI INTL       ...|
|AEM00041217| 24.433|  54.651|ABU DHABI INTL   ...|
+-----------+-------+--------+--------------------+
only showing top 5 rows

val stationTemp = avgDf.groupBy(col("sid")).agg(avg(col("avgTemp")))

stationTemp: org.apache.spark.sql.DataFrame = [sid: string, avg(avgTemp): double]

stationTemp.show(5)
+-----------+------------+
|        sid|avg(avgTemp)|
+-----------+------------+
|ASN00015643|       383.0|
|ASN00085296|       280.5|
|ASN00085280|       293.0|
|ASN00040209|       418.0|
|CA005030984|      -205.0|
+-----------+------------+
only showing top 5 rows

val stationJoined = stationTemp.join(stationDf, "sid")

stationJoined: org.apache.spark.sql.DataFrame = [sid: string, avg(avgTemp): double ... 3 more fields]

stationJoined.show(5)
+-----------+------------+-------+--------+--------------------+
|        sid|avg(avgTemp)|    lat|     lon|                name|
+-----------+------------+-------+--------+--------------------+
|FIE00145157|         7.0|63.8442| 23.1281|KOKKOLA HOLLIHAKA...|
|USC00340027|       139.0|34.7986|-96.6692|ADA 2NNE MESONET ...|
|USC00431580|       -17.0|43.9572|-73.2106|CORNWALL         ...|
|USW00063899|       301.5| 31.145|-87.0517|BREWTON 3 NNE    ...|
|FRE00171627|       135.5|48.8256| -3.4731|PLOUMANAC'H      ...|
+-----------+------------+-------+--------+--------------------+
only showing top 5 rows
