package com.minhc.basicscala
package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object FakeFriends extends App {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession
    .builder()
    .appName("SparkSQL")
    .master("local[*]")
    .getOrCreate()

  import sparkSession.implicits._

  val ds = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/fakefriends.csv")
    .as[Person]

  ds.printSchema()
  ds.createOrReplaceTempView("people")

  val teens: Unit = sparkSession.sql("select * from people where age >=13 and age <=19")
    .collect()
    .foreach(println)

}
