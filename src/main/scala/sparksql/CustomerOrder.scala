package com.minhc.basicscala
package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

object CustomerOrder extends App {

  case class CustomerOrderDetail(customerId: Int, itemId: Int, amountSpent: Float)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("CustomerOrder")
    .master("local[*]")
    .getOrCreate()

  val schema = new StructType()
    .add(StructField("customerId", IntegerType, nullable = true))
    .add(StructField("itemId", IntegerType, nullable = true))
    .add(StructField("amountSpent", FloatType, nullable = true))

  import session.implicits._

  val dataset = session
    .read
    .option("header", "false")
    .schema(schema)
    .csv("data/customer-orders.csv")
    .as[CustomerOrderDetail]

  val result: Unit = dataset
    .groupBy('customerId)
    .agg(functions.round(functions.sum('amountSpent), 2).alias("total_amount"))
    .sort(functions.desc("total_amount"))
    .show()
}
