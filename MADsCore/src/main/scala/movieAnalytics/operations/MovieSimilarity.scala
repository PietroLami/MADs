package movieAnalytics.operations

import Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, desc, sum}

class MovieSimilarity extends MovieAnalyzer {

  /**
    * Computes the similarity between the given movies and the movies in rumDf.
    *
    * @param rumDf DataFrame containing at least (rating, userId, movieId).
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return Sequence of DataFrames, one for each movie.
    * @throws InvalidOp if one of the movieIds (movie + movies) doesn't exist.
    */
  private def similar(rumDf: DataFrame, movie:String, movies: String*): Seq[DataFrame] = {
    val simDfs = (movie +: movies).map {
      mId => {
        if (!idExists(rumDf, mId, "movieId"))
          throw new InvalidOp("Not existing id")
        val movieRatingsDf = movieRatings(rumDf, mId)
        val comparativeDf = comparativeInfo(movieRatingsDf, rumDf.where(s"not movieId = '$mId'"), "movieId")
        similarityNeighbors(similarityDf(rumDf, mId, comparativeDf, "movieId"), "movieId")
      }
    }
    simDfs
  }

  /**
    * Computes the similarity between the given movies and the movies in rumDf.
    *
    * @param rumRDD RDD where each Map has the keys (movieId, userId, rating).
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return Sequence of (Map (movieId, similarity), Map(first map movieIds, 1), one for each movie.
    * @throws InvalidOp if one of the movieIds (movie + movies) doesn't exist.
    */
  private def similar(rumRDD:RDD[Map[String,String]], movie:String, movies: String*): Seq[(Map[String, Double], Map[String, Double])] = {
    val simRDDs = (movie +: movies).map {
      mId => {
        if (!idExists(rumRDD, mId, "movieId"))
          throw new InvalidOp("Not existing id")
        val s = similarityRDD(rumRDD, mId, "movieId")
        (s, s.map(r => (r._1, 1.0)))
      }
    }
    simRDDs
  }

  /**
    * Computes the movies similar to the given one(s), by considering the others that received a similar rating.
    * @note If a movie is similar to only one of those given as input, its similarity is the one for that movie,
    *       otherwise, if it's similar to multiple movies, its similarity is the average of the ones for those movies.
    *
    * @param rumDf DataFrame containing at least (rating, userId, movieId).
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return DataFrame with schema (movieId, similarity).
    */
  def similarMoviesUnion(rumDf: DataFrame, movie:String, movies: String*): DataFrame = {
    val simDfs = similar(rumDf, movie, movies:_*)
    val simDf:DataFrame = simDfs.reduce(_.unionByName(_))
    simDf.groupBy("movieId").agg(sum("similarity")/count("similarity") as "similarity")
      .orderBy(desc("similarity"))
  }

  /**
    * Computes the movies similar to the given one(s), by considering the others that received a similar rating.
    * @note A movie is considered only if it's similar to all the ones given as input, and its similarity is the average
    *       of all the results.
    *
    * @param rumDf DataFrame containing at least (rating, userId, movieId).
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return DataFrame with schema (movieId, similarity).
    */
  def similarMoviesIntersect(rumDf: DataFrame, movie:String, movies: String*): DataFrame = {
    val simDfs = similar(rumDf, movie, movies:_*)
    val simDf = simDfs.reduce((x,y)=> {
      val z = x.select("movieId").withColumnRenamed("movieId", "mid")
      val w = y.select("movieId")
      val commonMovies = z.join(w, z("mid") === y("movieId")).drop("movieId")
      commonMovies.join(x, x("movieId") === commonMovies("mid"))
        .unionByName(commonMovies.join(y, x("movieId") === commonMovies("mid")))
        .drop("mid")
    })
    simDf.groupBy("movieId").agg(sum("similarity")/count("similarity") as "similarity")
      .orderBy(desc("similarity"))
  }

  /**
    * Computes the movies similar to the given one(s), by considering the others that received a similar rating.
    * @note If a movie is similar to only one of those given as input, its similarity is the one for that movie,
    *       otherwise, if it's similar to multiple movies, its similarity is the average of the ones for those movies.
    *
    * @param rumRDD RDD where each Map has the keys movieId, userId, rating.
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return Map (movieId, similarity).
    */
  def similarMoviesUnion(rumRDD:RDD[Map[String,String]], movie:String, movies: String*): Map[String, Double] = {
    val simRDDs= similar(rumRDD, movie, movies:_*)
    val combinedMaps = simRDDs.reduce((m1, m2) => (union(m1._1, m2._1), union(m1._2, m2._2)))
    divideMaps(combinedMaps._1, combinedMaps._2)
  }

  /**
    * Computes the movies similar to the given one(s), by considering the others that received a similar rating.
    * @note A movie is considered only if it's similar to all the ones given as input, and its similarity is the average
    *       of all the results.
    *
    * @param rumRDD RDD where each Map has the keys movieId, userId, rating.
    * @param movie Movie identifier.
    * @param movies Movie identifiers.
    * @return Map (movieId, similarity).
    */
  def similarMoviesIntersect(rumRDD:RDD[Map[String,String]], movie:String, movies: String*): Map[String, Double] = {
    val simRDDs= similar(rumRDD, movie, movies:_*)
    val combinedMaps = simRDDs.reduce((m1, m2) => (intersect(m1._1, m2._1), intersect(m1._2, m2._2)))
    divideMaps(combinedMaps._1, combinedMaps._2)
  }
}
