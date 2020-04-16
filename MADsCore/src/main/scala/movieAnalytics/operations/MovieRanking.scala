package movieAnalytics.operations

import Utils.avgRating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class MovieRanking {

  protected var _minVotes = 150

  def minVotes: Int = _minVotes
  def minVotes_=(v:Int):Unit= _minVotes= v

  /**
    * Computes the df (movieId, #users that watched the movie, average of the movie).
    *
    * @param df DataFrame with at least (movieId, userId, rating).
    * @return DataFrame with schema (movieId, count, avgRating).
    */
  private def cntAvgByMovie(df: DataFrame): DataFrame = {
    df.groupBy("movieId").agg(count("userId") as "count",
      sum("rating") / count("userId") as "avgRating")
  }

  /**
    * Computes a true bayesian estimate on 'rumDf', to obtain the movie rank over the whole DataFrame.
    *
    * @param rumDf DataFrame with at least (movieId, userId, rating).
    * @return DataFrame with schema (movieId, rank).
    */
  def trueBayesianEstimate(rumDf:DataFrame): DataFrame = {
    val ascDf:DataFrame = cntAvgByMovie(rumDf)
    val avgTot:Double = avgRating(rumDf)
    ascDf.select(
      col("movieId"),
      callUDF("bayes", col("avgRating"), col("count"), lit(avgTot), lit(_minVotes)) as "rank")
  }

  /**
    * Computes a true bayesian estimate on 'rumRDD', to obtain the movie rank over the whole RDD.
    *
    * @param rumRDD RDD where each Map has the keys movieId, userId, rating.
    * @return RDD of (movieId, rank).
    */
  def trueBayesianEstimate(rumRDD: RDD[Map[String,String]]): RDD[(String, Double)] = {
    val mrRDD : RDD[(String, (Double, Int))]= rumRDD.map({x=>
      (x.getOrElse("movieId","-1"),(x.getOrElse("rating","0.0").toDouble,1))
    })
    // macRDD: RDD[movieId,(avg,cnt)]
    val macRDD= mrRDD.reduceByKey((u1,u2)=>(u1._1+u2._1,u1._2+u2._2)).map(u=>(u._1,(u._2._1/u._2._2,u._2._2)))
    val avgTot:Double = rumRDD.map(_.getOrElse("rating","0.0").toDouble).reduce(_+_)/rumRDD.count
    val min= _minVotes
    macRDD.map(u=>(u._1,(u._2._1*u._2._2  + avgTot* min)/(min+u._2._2)))
  }
}
