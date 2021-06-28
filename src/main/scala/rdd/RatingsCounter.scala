package com.minhc.basicscala
package rdd

import org.apache.log4j._
import org.apache.spark._

object RatingsCounter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkContext = new SparkContext("local[*]", "RatingsCounter");

    val lines = sparkContext.textFile("data/ml-100k/u.data");

    val ratings = lines.map(x => x.split("\t")(2))

    val results = ratings.countByValue();

    println(results.toSeq.sortBy(_._1))








  }

}
