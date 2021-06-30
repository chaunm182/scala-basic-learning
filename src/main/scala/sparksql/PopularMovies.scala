package com.minhc.basicscala
package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import scala.io.{Codec, Source}

object PopularMovies extends App {

  case class Movie(userId: Int, movieId: Int, rating: Int, timestamp: Long)

  def loadMovieName : Map[Int, String] = {
    implicit val codec: Codec = Codec("ISO-8859-1")

    var movieNames : Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")

    for(line <- lines.getLines()){
      val fields = line.split("\\|")
      if(fields.length>1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    lines.close()
    movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder()
    .appName("PopularMovie")
    .master("local[*]")
    .getOrCreate()

  val schema = new StructType()
    .add(StructField("userId", IntegerType, nullable = true))
    .add(StructField("movieId", IntegerType, nullable = true))
    .add(StructField("rating", IntegerType, nullable = true))
    .add(StructField("timestamp", LongType, nullable = true))

  val nameDict = session.sparkContext.broadcast(loadMovieName)


  import session.implicits._
  val movies = session.read
    .option("sep", "\t")
    .schema(schema)
    .csv("data/ml-100k/u.data")
    .as[Movie]

  val movieCount = movies.groupBy("movieId").count()

  val lookupName = (movieId: Int) => {
    nameDict.value(movieId)
  }

  val lookupNameUDF = functions.udf(lookupName)

  val result: Unit = movieCount
    .withColumn("movie_name", lookupNameUDF(functions.col("movieId")))
    .sort(functions.desc("count"))
    .show(truncate = false)





}
