package movieAnalytics.operations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, sum, when}


object UserHelpfulness {
  /**
    * Computes the helpfulness percentage (the average) associated with each couple (movieId, rating).
    *
    * @param mDf DataFrame containing at least (movieId, rating, percentage).
    * @return DataFrame (rating, movieId, movieScoreHelpfulness).
    */
  private def movieScoreHelpfulness(mDf: DataFrame) = {
    mDf.groupBy("rating", "movieId")
        .agg(sum(col("percentage"))/count(col("percentage")) as "movieScoreHelpfulness")
  }

  /**
    * Computes the helpfulness associated to each user.
    *
    * @note If an user has an helpfulness score that is lower than the average (of the other users that gave the same
    *       rating to the same movie), its score is incremented by adding the average score to the initial score and
    *       dividing by 2. The final helpfulness score is the average of the user helpfulness for the evaluated movies.
    *
    * @param mDf DataFrame containing at least (userId, movieId, rating, percentage).
    * @return DataFrame with columns (userId, userHelpfulness).
    */
  def userHelpfulness(mDf:DataFrame): DataFrame = {
    val userHelpDf =  mDf.groupBy("userId")
          .agg(sum("percentage")/count("percentage") as "userHelpfulness")
    // movies helpfulness by score
    val smhDf = movieScoreHelpfulness(mDf)
    val smhJDf = mDf.join(smhDf, smhDf("movieId") === mDf("movieId") && smhDf("rating") === mDf("rating"))
    val helpDf = smhJDf.join(userHelpDf, "userId")
    val p1 = helpDf.withColumn("userHelpfulness", when
      (
        col("userHelpfulness") < col("movieScoreHelpfulness"),
        (col("userHelpfulness") + col("movieScoreHelpfulness")) / lit(2)
      ).otherwise(col("userHelpfulness")))
    // since an user can have different scores (because of the different averages for each movie and score)
    // recompute again the average for each user
    p1.select("userId", "userHelpfulness")
      .groupBy("userId")
      .agg(sum(col("userHelpfulness"))/count(col("userHelpfulness")) as "userHelpfulness")
  }

  /**
    * Computes the helpfulness associated to each user.
    *
    * @note If an user has an helpfulness score that is lower than the average (of the other users that gave the same
    *       rating to the same movie), its score is incremented by adding the average score to the initial score and
    *       dividing by 2. The final helpfulness score is the average of the user helpfulness for the evaluated movies.
    *
    * @param mRDD RDD of Maps where each Map contains (userId, movieId, rating, percentage).
    * @return RDD (userId, userHelpfulness).
    */
  def userHelpfulness(mRDD:RDD[Map[String,String]]): RDD[(String, Double)] = {
    val percentageRDD = mRDD.map(r => (
        r.getOrElse("userId", "-1"), (r.getOrElse("percentage", "0").toDouble, 1)))
    // RDD (userId, helpfulness)
    val userHelpRDD = percentageRDD.reduceByKey((p1,p2) => (p1._1+p2._1, p1._2+p2._2)).map(r => (r._1, r._2._1/r._2._2))
    // RDD ((movie, rating), helpfulness)
    val avgHelpRDD = mRDD.map(r => (
        (r.getOrElse("movieId", "-1"), r.getOrElse("rating", "0").toDouble),
        (r.getOrElse("percentage", "0").toDouble, 1)))
        .reduceByKey((mr1, mr2) => (mr1._1 + mr2._1, mr1._2 + mr2._2))
        .map(mr => (mr._1, mr._2._1/mr._2._2))
    // join on the same key
    // RDD (userId, (movieId, rating, avgHelpfulness))
    val jAvg = mRDD.map(r => ((r.getOrElse("movieId", "-1"), r.getOrElse("rating", "0").toDouble), r.getOrElse("userId", "-1")))
        .join(avgHelpRDD)
        .map(r => (r._2._1, (r._1._1, r._1._2, r._2._2)))
    // RDD (userId, helpfulness avg dependant, 1 for cnt)
    val jUserH: RDD[(String, (Double, Int))] = jAvg.join(userHelpRDD)
        .map(r => {
          val aHelp: Double = r._2._1._3
          val uHelp: Double = r._2._2
          val fHelp: Double = if (aHelp <= uHelp) uHelp else (uHelp+aHelp)/2
          (r._1, (fHelp, 1))
        })
    val fH = jUserH.reduceByKey((c1, c2) => (c1._1 + c2._1, c1._2 + c2._2)).map(r => (r._1, r._2._1/r._2._2))
    fH
  }
}
