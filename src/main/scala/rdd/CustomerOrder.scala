package com.minhc.basicscala
package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrder {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "CustomerOrder");

    val rdd = sc.textFile("data/customer-orders.csv");

    val result = rdd
      .map(x => {
        val arr = x.split(",")
        (arr(0).toInt, arr(2).toFloat)
      })
      .reduceByKey(_+_) //(x,y) => x+y
      .collect();

    result.sorted.foreach(println)
  }

}
