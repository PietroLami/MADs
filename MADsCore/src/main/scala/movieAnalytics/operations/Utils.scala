package movieAnalytics.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.joda.time.DateTime


class InvalidOp(s:String) extends Exception(s) {}


object Utils {
  /**
    * Initializes 'spark' by adding all the required udf.
    *
    * @param spark SparkSession context.
    */
  def initSession(spark: SparkSession): Unit = {
    spark.udf.register("year", (t: String) => {
      val dt = new DateTime(t.toLong * 1000)
      dt.getYear.toString
    })
    spark.udf.register("month", (t: String) => {
      val dt = new DateTime(t.toLong * 1000)
      dt.getMonthOfYear.toString
    })
    spark.udf.register("square", (v: Double, avg: Double) => (v - avg) * (v - avg))
    spark.udf.register("product", (v1: Double, v2: Double, avg1: Double, avg2: Double) => (v1 - avg1) * (v2 - avg2))
    spark.udf.register("bayes", (avg: Double, cnt: Double, avgTot: Double, minVotes: Int) =>
      (avg * cnt + avgTot * minVotes) / (cnt + minVotes))
  }

  /**
    * Computes the df (userId, rating of 'movie').
    *
    * @param df DataFrame with at least (userId, movieId, rating).
    * @param movie Movie identifier.
    * @return DataFrame with schema (userId, rating).
    */
  def movieRatings(df: DataFrame, movie: String): DataFrame = {
    df.where(s"movieId = '$movie'").select("userId", "rating")
  }

  /**
    * Computes the df of all the distinct movies in 'df'.
    *
    * @param df DataFrame with at least (movieId).
    * @return DataFrame with schema (movieId).
    */
  def distinctMovies(df: DataFrame): DataFrame = {
    df.select("movieId").distinct()
  }

  /**
    * Computes the df of all the distinct users in 'df'.
    *
    * @param dfFilm DataFrame with at least (userId).
    * @return DataFrame with schema (userId).
    */
  def distinctUsers(dfFilm: DataFrame): DataFrame = {
    dfFilm.select("userId").distinct()
  }


  /**
    * Divides the element of 'm1' for the corresponding element of 'm2'.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double, with the same keys of 'm1'.
    * @return Map String->Double.
    */
  def divideMaps(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    m1.map(r => (r._1, r._2 / m2(r._1)))
  }

  /**
    * Computes the df (group identifier, average rating of the group identifier).
    *
    * @param df DataFrame (userId, movieId, rating).
    * @param group Column for aggregate.
    * @param cnt Column for count.
    * @return DataFrame ('group', avgRating).
    */
  def avgGroupRating(df: DataFrame, group: String = "userId", cnt: String = "movieId"): DataFrame = {
    df.groupBy(group).agg(sum("rating") / count(cnt) as "avgRating")
  }

  /**
    * Computes the average of the ratings over the whole DataFrame.
    *
    * @param df DataFrame with at least (rating).
    * @return The average of the ratings.
    */
  def avgRating(df: DataFrame): Double = {
    df.agg(sum("rating") / count("rating") as "avgRating").first().get(0).toString.toDouble
  }

  /**
    * Computes the intersection between 'm1' and 'm2', where the keys are the common items and the values are the
    * corresponding sums.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double.
    * @return Map String->Double.
    */
  def intersect(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    val k1 = Set(m1.keysIterator.toList: _*)
    val k2 = Set(m2.keysIterator.toList: _*)
    val intersection = k1 & k2
    val r1: Set[(String, Double)] = for (key <- intersection) yield key -> (m1(key) + m2(key))
    r1.toMap
  }
  /**
    * Computes the union between 'm1' and 'm2'
    * @note For common keys, the values are the sums of the two values.
    *
    * @param m1 Map String->Double.
    * @param m2 Map String->Double.
    * @return Map String->Double.
    */
  def union(m1: Map[String, Double], m2: Map[String, Double]): Map[String, Double] = {
    val k1 = Set(m1.keysIterator.toList: _*)
    val k2 = Set(m2.keysIterator.toList: _*)
    val intersection = k1 & k2
    val r1: Set[(String, Double)] = for (key <- intersection) yield key -> (m1(key) + m2(key))
    val r2 = m1.filterKeys(!intersection.contains(_)) ++ m2.filterKeys(!intersection.contains(_))
    r2 ++ r1
  }

  /**
    * Checks if an id exists as 'col' of 'rumRDD'.
    *
    * @param rumRDD RDD containing at least 'col'.
    * @param id Identifier.
    * @param key Key of the RDD.
    * @return Boolean true if the 'rumRDD' contains 'id', false otherwise.
    */
  def idExists(rumRDD:RDD[Map[String,String]], id:String, key:String): Boolean = {
    try {
      rumRDD.filter(r => r.getOrElse(key, "-1").equals(id)).count() > 0
    } catch {
      case _:AnalysisException => false
    }
  }

  /**
    * Checks if an id exists in the column 'col' of 'rumDf'.
    *
    * @param rumDf DataFrame with at least 'col'.
    * @param id Identifier.
    * @param colId Column identifier.
    * @return Boolean true if the 'rumDf' contains 'id', false otherwise.
    */
  def idExists(rumDf: DataFrame, id: String, colId:String): Boolean = {
    try {
      !rumDf.where(s"$colId = '$id'").isEmpty
    } catch {
      case _:AnalysisException => false
    }
  }

  /**
    * Gets movie name by movieId.
    *
    * @param movieDF DataFrame (movieId, name).
    * @param movieId Required movie identifier.
    * @return Name of 'movieId'.
    * @throws InvalidOp if 'movieId' doesn't exist.
    */
  def getMovieName(movieDF:DataFrame, movieId:String): String = {
    if (!idExists(movieDF, movieId, "movieId"))
      throw new InvalidOp("Not existing id")
    movieDF.where(s"movieId = '$movieId'").first().get(1).toString
  }

  /**
    * Gets the DataFrame (movieId, title) for all the movies in result.
    *
    * @param result DataFrame with at least (movieId).
    * @param titlesDf DataFrame with at least (movieId, title).
    * @return DataFrame with (movieId, title).
    */
  def getDfTitles(result:DataFrame, titlesDf:DataFrame): DataFrame = {
    result.select("movieId").join(titlesDf.select("movieId", "title"), "movieId")
  }
}
