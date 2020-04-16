package movieAnalytics.operations

import Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, sum}

class MovieRecommendation extends MovieAnalyzer {
  /**
    * Computes a df (movie, rating of 'user' on the movie).
    *
    * @param df DataFrame with at least (userId, movieId, rating).
    * @param user User identifier.
    * @return DataFrame (movieId, rating) containing the movies and ratings of 'user'.
    */
  private def userRatings(df:DataFrame, user:String): DataFrame = {
    df.where(s"userId = '$user'").select("movieId","rating")
  }

  /**
    * Computes the movies seen ONLY by similar users.
    *
    * @param rumDf DataFrame (userId, movieId, rating).
    * @param simDf DataFrame (userId,similarity) of the most similar users.
    * @param userFilmsDf DataFrame with at least (movieId).
    * @return DataFrame (userId, movieId, rating) of the movies seen by neighbors and unseen by the requester.
    */
  private def usefulUnseenMovieRatings(rumDf:DataFrame, simDf:DataFrame, userFilmsDf:DataFrame): DataFrame = {
    val df= simDf.select("userId").join(rumDf, "userId")
    df.join(userFilmsDf, df("movieId") === userFilmsDf("movieId"), "left_anti")

  }
  /**
    * Computes the movies seen by similar users.
    *
    * @param rumDf DataFrame (userId, movieId, rating).
    * @param simDf DataFrame with at least (userId) of the most similar users.
    * @return DataFrame (userId, movieId, rating) containing the movies seen by neighbors.
    */
  private def usefulTotalMovieRatings(rumDf:DataFrame, simDf:DataFrame): DataFrame = {
    simDf.select("userId").join(rumDf, "userId")
  }

  /**
    * Predicts the score of the movies seen by the neighbors but unseen by the requester.
    *
    * @param rumDf DataFrame (userId, movieId, rating).
    * @param simDf DataFrame (userId, similarity).
    * @param userFilmsDf DataFrame (movieId, rating) of the requester.
    * @return DataFrame (movieId, moviePrediction).
    */
  private def predictScore(rumDf:DataFrame, simDf:DataFrame, userFilmsDf:DataFrame): DataFrame = {
    val usefulUnseen:DataFrame = usefulUnseenMovieRatings(rumDf, simDf, userFilmsDf)
    val avgU:Double = avgRating(userFilmsDf)
    val avgDf:DataFrame = avgGroupRating(usefulTotalMovieRatings(rumDf, simDf))
    val simAvgDf:DataFrame = simDf.join(avgDf, "userId")
    val sumSimilarity = simAvgDf.agg(sum("similarity")).first().get(0).toString.toDouble
    val predDf0:DataFrame = usefulUnseen.join(simAvgDf, "userId")
    val predDf1 = predDf0.withColumn("prediction", col("similarity")*(col("rating") - col("avgRating")))
    val predDf2 = predDf1.groupBy("movieId").agg(sum("prediction") as "moviePrediction")
    predDf2.withColumn("moviePrediction", (predDf2("moviePrediction")/sumSimilarity) + avgU)
            .orderBy(desc("moviePrediction"))
  }

  /**
    * Suggests movies to the given user and predicts his ratings.
    *
    * @param rumDf DataFrame containing at least (movieId, userId, rating).
    * @param user User identifier.
    * @return DataFrame (movieId, moviePrediction).
    * @throws InvalidOp if 'user' doesn't exist.
    */
  def movieRecommendation(rumDf:DataFrame, user:String): DataFrame = {
    if(!idExists(rumDf, user, "userId"))
      throw new InvalidOp("Not existing id")
    val userDf = userRatings(rumDf, user)
    val comparativeDf = comparativeInfo(userDf, rumDf.where(s"not userId = '$user'"), "userId")
    val simDf = similarityNeighbors(similarityDf(rumDf, user, comparativeDf, "userId"), "userId")
    predictScore(rumDf, simDf, userDf)
  }

  /**
    * Suggests movies to the given user and predicts his ratings.
    *
    * @param rumRDD RDD where each Map has the keys (movieId, userId, rating).
    * @param user User identifier.
    * @return RDD (movieId, moviePrediction).
    * @throws InvalidOp is the user doesn't exist.
    */
  def movieRecommendation(rumRDD:RDD[Map[String,String]], user:String): RDD[(String, Double)] = {
    if(!idExists(rumRDD, user, "userId"))
      throw new InvalidOp("Not existing id")
    val userRDD = rumRDD.filter(r => r.getOrElse("userId", "-1").equals(user)).map(k =>
      (k.getOrElse("movieId", "-1"), k.getOrElse("rating", "0.0").toDouble))
    val avgRequester = userRDD.map(_._2.toDouble).reduce((v1, v2) => v1+v2)/userRDD.count()
    val mUserSet: Set[String] = userRDD.map(_._1).collect().toSet
    val sim = similarityRDD(rumRDD, user, "userId")
    val simSum: Double = sim.values.sum
    //RDD[userId,(movieId,rate)] with movies not seen by requesterUser
    val neighSet: Set[String] = sim.keys.toSet
    //RDD[users similar to user request]
    val neighRDD = rumRDD.filter(r => neighSet.contains(r.getOrElse("userId","-1")))
    //RDD [userId,his avg]
    val neighAvgs: RDD[(String, Double)] = neighRDD
      .map(r => (r.getOrElse("userId", "-1"), (r.getOrElse("rating", "0.0").toDouble, 1.0)))
      .reduceByKey((m1, m2) => (m1._1.toDouble + m2._1.toDouble, m1._2.toDouble + m2._2.toDouble))
      .map(u => (u._1, u._2._1.toDouble/u._2._2.toDouble))
    //RDD[movieId,first part of similarity] = sum(simUID* rateMovie - avgUID)
    val movies2Pred: RDD[(String, Double)] = neighRDD
      .filter(r => !mUserSet.contains(r.getOrElse("movieId", "-1")))
      .map(r=> (r.getOrElse("userId", "-1"), (r.getOrElse("movieId", "-1"), r.getOrElse("rating", "0.0").toDouble)))
      .join(neighAvgs)
      .map(u => (u._2._1._1, sim.getOrElse(u._1, 0.0) * (u._2._1._2  - u._2._2)))
    // avgREQ + 1/simSum *  [first part of similarity] last part of computation of similarity
    movies2Pred.reduceByKey((u1,u2) => u1 + u2).map(r => (r._1, r._2/simSum + avgRequester))
  }
}
