package movieAnalytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.math.min
import scala.util.Random


object Ds {

  /**
    * Gets dataset from csv as DataFrame.
    *
    * @param spark SparkSession.
    * @param path Path of the csv.
    * @return DataFrame corresponding to the given csv.
    */
  def importCsvDF(spark:SparkSession, path:String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(path)
  }

  /**
    * Gets dataset from csv as RDD.
    *
    * @param path Path of the csv.
    * @param spark SparkSession.
    * @return RDD of Maps corresponding to the given csv.
    */
  def importCsvRDD(spark:SparkSession, path:String): RDD[Map[String, String]] = {
    val csv: RDD[String] = spark.sparkContext.textFile(path)
    val headerAndRows: RDD[Array[String]] = csv.map(line => line.split(",").map(_.trim))
    val header: Seq[String] = headerAndRows.first()
    val data = headerAndRows.filter(_(0) != header.head)
    data.map(splits => header.zip(splits).toMap)
  }

  /**
    * Converts standford txt in RDD of Maps where each Map has "userId", "movieId", "rating" "timestamp" and "percentage"
    * as keys.
    *
    * @param spark SparkSession.
    * @param path Path of the txt.
    * @return RDD of Maps.
    */
  def importStanfordRDD(spark:SparkSession, path:String): RDD[Map[String, String]] = {
    val conf = new org.apache.hadoop.mapreduce.Job().getConfiguration
    conf.set("textinputformat.record.delimiter", "\n\n")
    val lines = spark.sparkContext
      .newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(_._2.toString)
    val res: RDD[Map[String, String]] = lines.map(b => {
      val blockMap = Map[String, String]()
      var r1 = Map[String, String]()
      r1 = b.split("\n").foldLeft(blockMap)((bm:Map[String, String], line:String) => {
        try{
          val tmp = line.split(':')
          val tmpId = tmp(0).split('/')(1)
          val genericId = tmpId match {
            case "userId" => "userId"
            case "productId" => "movieId"
            case "score" => "rating"
            case "time" => "timestamp"
            case "helpfulness" => "percentage"
            case _ => ""
          }
          var value = ""
          val tmpVal = tmp.slice(1, tmp.length).foldLeft("")((a, b) => a+b.trim)
          value =  genericId match {
              case "percentage" => if (tmpVal.split("/")(1).toInt == 0) "0" else min(tmpVal.split("/")(0).toInt * 100 / tmpVal.split("/")(1).toInt, 100).toString
              case _ => tmpVal
            }
          if (!genericId.equals("")) bm.updated(genericId, value) else bm
        } catch {
          case _:Exception => bm
        }
    })
      r1
    }).filter(m => Set("userId", "movieId", "rating", "timestamp", "percentage") == m.keySet)
    res
  }

  /**
    * Gets dataset from txt as DataFrame.
    *
    * @param spark SparkSession.
    * @param path Path of the txt.
    * @return DataFrame corresponding to the given txt.
    */
  def importStanfordDF(spark:SparkSession, path:String): DataFrame = {
    import spark.implicits._
    val s = importStanfordRDD(spark, path).map(m => {(
      m.getOrElse("userId", ""),
      m.getOrElse("movieId", ""),
      m.getOrElse("rating", ""),
      m.getOrElse("timestamp", ""),
      m.getOrElse("percentage", ""))
    }).toDF()
      .withColumnRenamed("_1", "userId")
      .withColumnRenamed("_2", "movieId")
      .withColumnRenamed("_3", "rating")
      .withColumnRenamed("_4", "timestamp")
      .withColumnRenamed("_5", "percentage")
    s
  }

  /**
    * Doubles the size of 'rumDf' with random userIds and movieIds.
    * Function useful for stress testing.
    *
    * @param rumDf DataFrame.
    * @return DataFrame with the same schema of 'rumDf' but with double size.
    */
  def increaseSizeDf(rumDf:DataFrame): DataFrame = {
    val df = rumDf
    val maxMovie = rumDf.select("movieId").agg(max("movieId")).head.get(0).toString.toInt
    val maxUser = rumDf.select("userId").agg(max("userId")).head.get(0).toString.toInt
    df.withColumn("movieId", col("movieId").cast("Int") + Random.nextInt(maxMovie))
      .withColumn("userId", col("userId").cast("Int") + Random.nextInt(maxUser))
      .union(rumDf)
  }

  /**
    * Stores the input DataFrame as a csv file in 'path'.
    * 'path' should include the csv extension.
    *
    * @param df DataFrame to store.
    * @param path Path storage location.
    */
  def storeDf(df:DataFrame, path:String):Unit = {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }
  /**
    * Stores the input DataFrame as a csv file in 'path'.
    * 'path' should include the csv extension.
    * @note number of partitions is fixed to 1.
    *
    * @param df DataFrame to store.
    * @param path Path storage location.
    */
  def storeDfPt1(df:DataFrame, path:String):Unit = {
    df.repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }
}
