package org.cs239cfr.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

import scala.io.Source
import java.nio.charset.CodingErrorAction

import scala.collection.mutable
import scala.io.Codec
import scala.math.sqrt

object MovieCFR {

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2

    ((movie1, movie2), (rating1, rating2))
  }

  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx * sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    (score, numPairs)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
    conf.setAppName("MovieCFR")
    val sc = new SparkContext(conf)
//    val sc = new SparkContext("local[*]", "MovieCFR")

//    val data = sc.textFile("s3n://xgwang-spark-demo/ml-20m/ratings.csv")
//    val data = sc.textFile("ml-100k/ratings.csv")
    val data = sc.textFile("hdfs:/user/hadoop/ratings.csv")

    // Map ratings to key / value pairs: user ID => movie ID, rating
    val ratings = data
      .map(l => l.split(","))
      .map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratings.join(ratings)

    // userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(new HashPartitioner(100))

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()
//    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    //Save the results if desired
    println("\nSaving the sorted similarities...")
    val sortedSim = moviePairSimilarities.sortByKey()
//    sorted.saveAsTextFile("movie-sims")
//    sortedSim.saveAsTextFile("s3n://xgwang-spark-demo/movie-sims")

    // Extract similarities for the movie we care about that are "good".

    if (args.length > 0) {

      println("\nLoading movie names...")
//      val nameDict = sc.textFile("ml-100k/movies.csv")
      val nameDict = sc.textFile("movies.csv")
        .map(line => {
          val fields = line.split(",")
          (fields(0).toInt, fields(1))
        })
      // Calculate top recommended movie based on user
      val userID: Int = args(0).toInt

      //  movieID -> rating
      val userRatings = ratings.filter(l => l._1 == userID)
        .map(_._2)

//      val userRecs = nameDict.map(l => l._1 -> 0.0)
//      val simMap = nameDict.map(l => l._1 -> 0.0)
      // movieID2 -> (movieID1, simValue)
      val userRecsNSim = sortedSim.map(l => {
        val row = l._1._1
        val col = l._1._2
        val simValue = l._2._1
        (col, (row, simValue))
      }).join(userRatings)
      .map(l => {
        val row = l._2._1._1
        val simValue = l._2._1._2
        val uRating = l._2._2
        (row, (simValue * uRating, simValue))
      })
      val finalRecs = userRecsNSim.reduceByKey((x, y) => {
        (x._1 + y._1, x._2 + y._2)
      }).map(l => {
        var normalizedValue = 0.0
        val simV = l._2._2
        if (simV != 0.0) {
          normalizedValue = l._2._1 / simV
        }
        (normalizedValue, l._1)
      })
      val recommendations = finalRecs.sortByKey(false).take(10)
      recommendations.foreach(println)

    }
  }
}

