package com.minhc.basicscala
package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FriendsByAge {

  def parseLine(line: String): (Int, Int) = {
    val arr = line.split(",");
    (arr(2).toInt, arr(3).toInt);
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "FriendsByAge");

    val rdd: RDD[String] = sc.textFile("data/fakefriends-noheader.csv")

    //RDD element (age, numFriends)
    val averagesByAge = rdd.map(parseLine)
      //after using mapValues RDD element form is (age, (numFriends,1))
      .mapValues(x => (x, 1))
      //use to sum up total friends and total instances for each age, by add ding together all the numFriends and 1's respectively
      .reduceByKey((x: (Int, Int), y : (Int, Int)) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1/x._2);

    val result =  averagesByAge.collect();

    result.toSeq.sorted.foreach(println)



  }

}
