package com.minhc.basicscala
package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math

object MinTemperatures {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkContext = new SparkContext("local[*]", "RatingsCounter");

    val rdd = sparkContext.textFile("data/1800.csv");

    val result = rdd.map(x => {
      val arr = x.split(",");
      (arr(0), arr(2), arr(3))
    })
      .filter(x => x._2.equals("TMIN"))
      .map(x => (x._1, x._3.toFloat))
      .reduceByKey((x, y) => math.min(x, y))
      .collect().foreach(println)

  }

}
