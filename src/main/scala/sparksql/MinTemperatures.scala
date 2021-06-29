package com.minhc.basicscala
package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemperatures extends App {

  case class Temperature(stationId : String, date : Int, measureType: String, temperature: Float)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("MinTemperature")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  val schema = new StructType()
    .add("stationId", StringType,nullable = true)
    .add("date", IntegerType, nullable = true)
    .add("measureType", StringType, nullable = true)
    .add("temperature", FloatType, nullable = true)

  val dataset = session.read
    .option("header", "false")
    .schema(schema)
    .csv("data/1800.csv")
    .as[Temperature]

  val result: Unit = dataset
    .filter('measureType === "TMIN")
    .groupBy('stationId)
    .min("temperature")
    .collect()
    .foreach(println)




}
