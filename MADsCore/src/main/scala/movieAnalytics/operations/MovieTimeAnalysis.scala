package movieAnalytics.operations

import Utils.idExists
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col, count, sum}
import org.joda.time.DateTime

object MovieTimeAnalysis {

  /**
    * Computes the DataFrame (movieId, year, average rating of the year) from 'yBegin' to 'yEnd'.
    *
    * @param tsDf DataFrame with at least (rating, timestamp).
    * @param yBegin First year of the interval (if given, otherwise first year of a rating for the movie).
    * @param yEnd Last year of the interval (if given, otherwise last year of a rating for the movie).
    * @return DataFrame (movieId, year, avgRating).
    */
  private def yearDF(tsDf:DataFrame, yBegin:String=null, yEnd:String=null): DataFrame = {
    val ratingDf = tsDf.select(
      col("movieId"),
      col("rating"),
      callUDF("year", col("timestamp")) as "year"
    )
    val ratingBeginDf = yBegin match {
      case null => ratingDf
      case _ => ratingDf.where(s"year >=  $yBegin")
    }
    val ratingEndDf = yEnd match {
      case null => ratingBeginDf
      case _ => ratingBeginDf.where(s"year <= $yEnd")
    }
    ratingEndDf.groupBy("movieId", "year")
      .agg(sum("rating")/count("rating") as "avgRating")
  }

  /**
    * Computes the DataFrame (movieId, month of 'year', avgRating).
    *
    * @param tsDf DataFrame with at least (rating, timestamp).
    * @param year Year.
    * @return DataFrame (movieId, month of 'year', avgRating).
    */
  private def monthDF(tsDf:DataFrame, year:String): DataFrame = {
    val ratingDf = tsDf.select(
      col("movieId"),
      col("rating"),
      col("timestamp"),
      callUDF("year", col("timestamp")) as "year"
    ).where(s"year = $year")
    ratingDf.select(
      col("movieId"),
      col("rating"),
      callUDF("month", col("timestamp")) as "month"
    ).groupBy("movieId", "month").agg(sum("rating")/count("rating") as "avgRating")
  }

  /**
    * Computes a DF containing data about the evolution of the average rating for the given movies.
    *
    * @note Data analysis can be performed either in a given interval of years by year
    *       or in a given year by month.
    *
    * @param rumDf DataFrame with at least (movieId, rating, timestamp).
    * @param movieId Identifier of a movie.
    * @param movieIds Identifier of movie(s).
    * @param byMonth Boolean true if the analysis is by month in the given year, false if it's by year.
    * @param yBegin First year of the interval, if 'byMonth' is false, year of the analysis otherwise.
    * @param yEnd Last year of the interval, optional if 'byMonth' is false.
    * @return DataFrame of time evolution.
    * @throws InvalidOp if one of the movieIds (movie + movies) doesn't exist.
    */
  def timeDFD(rumDf: DataFrame, movieId:String, movieIds: String*)
             (byMonth: Boolean=false, yBegin: String=null, yEnd: String=null): DataFrame = {
    (movieId+: movieIds).foreach(mId => {
      if(!idExists(rumDf, mId, "movieId"))
        throw new InvalidOp("Not existing id " + mId)
    })
    val whereCond = (movieId +: movieIds).map(x => s"movieId = '$x'").reduce( (a,b) => s"$a or $b" )
    val usefulDf = rumDf.select("movieId", "timestamp", "rating").where(whereCond)
    if (byMonth) monthDF(usefulDf, yBegin) else yearDF(usefulDf, yBegin, yEnd)
  }

  /**
    * Computes the RDD (movieId, year, average rating of the year).
    *
    * @param tsRDD RDD of Maps with keys (rating, timestamp).
    * @param yBegin First year of the interval (if given, otherwise first year of the movie).
    * @param yEnd Last year of the interval (if given, otherwise last year of the movie).
    * @return RDD (movieId, year, average rating of the year).
    */
  private def yearRDD(tsRDD:RDD[Map[String,String]], yBegin:String=null, yEnd:String=null): RDD[(String, Int, Double)] = {
    // RDD ((year, mId), (rating, 1 for cnt))
    val ratingRDD = tsRDD.map(r=>{
      val dt = new DateTime(r.getOrElse("timestamp","0").toLong * 1000)
      ((r.getOrElse("movieId", "-1"), dt.getYear.toString.toInt),(r.getOrElse("rating","0.0").toDouble,1))
    })
    val ratingBeginRDD = yBegin match {
      case null => ratingRDD
      case _ => ratingRDD.filter(_._1._2 >= yBegin.toInt)
    }
    val ratingEndRDD = yEnd match {
      case null => ratingBeginRDD
      case _ => ratingRDD.filter(_._1._2 <= yEnd.toInt)
    }
    // RDD(mId, year, avgRating)
    ratingEndRDD.reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2)).map(y=>(y._1._1, y._1._2,y._2._1/y._2._2))
  }

  /**
    * Computes the RDD (movieId, month of 'year', avgRating).
    *
    * @param tsRDD RDD of Maps with keys (rating, timestamp).
    * @param year Year.
    * @return RDD (movieId, month of 'year', avgRating).
    */
  private def monthRDD(tsRDD:RDD[Map[String,String]], year:String): RDD[(String, Int, Double)]  = {
    // RDD ((mId, month), (rating, 1 for cnt))
    val ratingRDD = tsRDD.map(r=>{
      val dt = new DateTime(r.getOrElse("timestamp","0").toLong * 1000)
      (dt.getYear.toString.toInt,
      (r.getOrElse("movieId", "-1"), dt.getMonthOfYear.toString.toInt, r.getOrElse("rating","0.0").toDouble,1))
    })
    .filter(_._1==year.toInt).map(m=>((m._2._1, m._2._2),(m._2._3, m._2._4)))
    ratingRDD
      // RDD (sum rating, cnt)
      .reduceByKey((v1,v2)=>(v1._1+v2._1,v1._2+v2._2))
      // RDD (mId, month, avgRating)
      .map(y=>(y._1._1, y._1._2, y._2._1/y._2._2))
    }


  /**
    * Computes a DF containing data about the evolution of the average rating for the given movies.
    *
    * @note Data analysis can be performed either in a given interval of years by year
    *       or in a given year by month.
    *
    * @param spark Spark context for the conversion from RDD to DF
    * @param rumRDD RDD of Maps with keys (movieId, rating, timestamp).
    * @param movieId Identifier of a movie.
    * @param movieIds Identifier of movie(s).
    * @param byMonth Boolean true if the analysis is by month in the given year, false if it's by year.
    * @param yBegin First year of the interval, if 'byMonth' is false, year of the analysis otherwise.
    * @param yEnd Last year of the interval, optional if 'byMonth' is false.
    * @return DataFrame of time evolution.
    * @throws InvalidOp if one of the movieIds (movie + movies) doesn't exist.
    */
  def timeDFR(spark: SparkSession, rumRDD: RDD[Map[String,String]], movieId:String, movieIds: String*)
             (byMonth: Boolean=false, yBegin: String=null, yEnd: String=null): DataFrame = {
    (movieId+: movieIds).foreach(mId => {
      if(!idExists(rumRDD, mId, "movieId"))
        throw new InvalidOp("Not existing id " + mId)
    })
    val mRDD = rumRDD.filter(r=> (movieId +: movieIds).contains(r.getOrElse("movieId", "-1")))
    val (toPlotRDD, axis) = if (byMonth) {
        (monthRDD(mRDD, yBegin), "month")
      } else {
        (yearRDD(mRDD, yBegin, yEnd), "year")
      }
    spark.createDataFrame(toPlotRDD).toDF("movieId", axis, "avgRating")
  }
}
