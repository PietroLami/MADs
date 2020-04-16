package plotter

import java.io.File

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.regions.Regions
import org.apache.spark.sql.{DataFrame, SparkSession}
import vegas._
import vegas.sparkExt._
import com.typesafe.config.{Config, ConfigFactory}
import com.amazonaws.services.s3.model.{GetObjectRequest, S3ObjectSummary}
import scala.collection.JavaConversions.{collectionAsScalaIterable => asScala}


object MADsPlot {
  val config: Config = ConfigFactory.load("madsPlot.conf")
  val AWS_ACCESS_KEY: String = config.getString("aws_s3.aws_access_key_id")
  val AWS_SECRET_ACCESS_KEY: String = config.getString("aws_s3.aws_secret_access_key")
  val AWS_SESSION_TOKEN: String = config.getString("aws_s3.aws_session_token")
  val bucket: String = "moviesanalyticstp"
  val localPath: String = "/Users/signax/Cloud/db/"

  /**
    * Plots a line chart containing the info in 'pDf'.
    *
    * @param pDf DataFrame to plot.
    * @param xAxis Time specific for the plot. It can be month or year.
    */
  private def plotter(pDf:DataFrame, xAxis:String): Unit = {
    val xFormat = xAxis match {
      case "month" => Quant
      case "year" => Temp
    }
    Vegas.layered("Movie Time Analysis", width = 600.0, height = 400.0).withLayers(
      Layer()
        .withDataFrame(pDf)
        .mark(Line)
        .encodeX(xAxis, xFormat)
        .encodeY("avgRating", Quant)
        .encodeColor(
          field="movieId",
          dataType=Nominal,
          legend=Legend(orient="left", title="MovieIds"))
        .encodeDetailFields(Field(field="movieId", dataType=Nominal)),
      Layer()
        .withDataFrame(pDf)
        .mark(Point)
        .encodeX(xAxis, xFormat)
        .encodeY("avgRating", Quant)
        .encodeColor(
          field="movieId",
          dataType=Nominal,
          legend=Legend(orient="left", title="MovieIds"))
        .encodeDetailFields(Field(field="movieId", dataType=Nominal))
    ).show
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("MADsPlot")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      if (args.length == 2 && args(1).endsWith(".csv")) {
        val loc = args(0)
        val madsOut = args(1)
        var df = spark.emptyDataFrame
        val xAxis = if (madsOut.startsWith("PM")) "month" else "year"
        loc match {
          case "A" | "a" | "aws" =>
            val awsS3Client: AmazonS3 = AmazonS3ClientBuilder.standard()
              .withCredentials(new AWSStaticCredentialsProvider(new BasicSessionCredentials(AWS_ACCESS_KEY,
                AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)))
              .withRegion(Regions.US_EAST_1).build()
            val listObjectsRequest = new ListObjectsRequest().
              withBucketName(bucket).
              withPrefix(s"$madsOut/").
              withDelimiter("/")
            val objects = awsS3Client.listObjects(listObjectsRequest)
            val summaries = asScala[S3ObjectSummary](objects.getObjectSummaries)
            for (summary <- summaries) {
              val sk = summary.getKey
              if (sk.endsWith(".csv")) {
                awsS3Client.getObject(
                  new GetObjectRequest(bucket, sk),
                  new File(s"/tmp/$sk"))
                df = spark.read
                  .format("csv")
                  .option("header", "true")
                  .load(s"/tmp/$sk")
                plotter(df, xAxis)
              }
            }
          case "L" | "l" | "local" =>
            df = spark.read
              .format("csv")
              .option("header", "true")
              .load(s"$localPath/$madsOut")
            plotter(df, xAxis)
          case _ => println("Not existing command")
        }
      } else {
        println("Invalid number of parameters")
      }
    } catch {
      case ex: org.apache.spark.sql.AnalysisException => println(ex.getMessage())
    }
    spark.sparkContext.stop()
  }
}
