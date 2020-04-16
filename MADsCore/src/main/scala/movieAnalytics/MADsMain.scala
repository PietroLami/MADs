package movieAnalytics

import movieAnalytics.operations._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.StdIn
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
// import java.io.FileWriter // Import required to keep track of elapsed time


object AllDone extends Exception { }


object MADsMain{

  val logo: String = "\n" +
    "888b     d888        d8888 8888888b.           \n" +
    "8888b   d8888       d88888 888  \"Y88b          \n" +
    "88888b.d88888      d88P888 888    888          \n" +
    "888Y88888P888     d88P 888 888    888 .d8888b  \n" +
    "888 Y888P 888    d88P  888 888    888 88K      \n" +
    "888  Y8P  888   d88P   888 888    888 \"Y8888b. \n" +
    "888   \"   888  d8888888888 888  .d88P      X88 \n" +
    "888       888 d88P     888 8888888P\"   88888P' \n" +
    "                                               \n" +
    "                                               \n" +
    "                                               "

  val awsPath: String = "s3://moviesanalyticstp/"
  val localPath: String = "/Users/signax/Cloud/db/"

  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val _ = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")
    (t1 - t0)/1000000000.0
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Invalid number of parameters")
      System.exit(1)
    }

    val (path:String, spark:SparkSession) = args(0) match {
      case "aws"|"a"|"A" => (awsPath, org.apache.spark.sql.SparkSession.builder
                              .appName("MADs")
                              .config("spark.sql.broadcastTimeout", "360000")
                              .getOrCreate)
      case "local"|"l"|"L" => (localPath, org.apache.spark.sql.SparkSession.builder
                              .master("local[*]")
                              .appName("MADs")
                              .config("spark.sql.broadcastTimeout", "360000")
                              .getOrCreate)
      case _ => System.exit(1)
    }
    spark.sparkContext.setLogLevel("ERROR")



    val intro = "MADs - Movie Analyzer Dataset - Beta Version"
    println(logo + intro)

    val pathML = path + "ratings.csv"           // path for MovieLens csv
    val pathStanford = path + "stanford.csv"    // path for Stanford as csv
    //val pathStanford = path + "movies.txt"    // path for Stanford as txt
    //val timePath: String = path + "time.csv"  // path for elapsed time. Remove comment to keep track of elapsed time

    val mrk = new MovieRanking()
    val mrc = new MovieRecommendation()
    val ms = new MovieSimilarity()
    Utils.initSession(spark)
    import spark.implicits._
    println("Initializing MADs...\n")

    val rumLDf: DataFrame = Ds.importCsvDF(spark, pathML)
    val rumLRDD: RDD[Map[String, String]] = Ds.importCsvRDD(spark, pathML)
    /*// Import Stanford dataset from txt
    val rumSDf: DataFrame = Ds.importStanfordDF(spark, pathStanford)
    val rumSRDD: RDD[Map[String, String]] = Ds.importStanfordRDD(spark, pathStanford)*/
    // Import Stanford dataset from csv
    val rumSDf: DataFrame = Ds.importCsvDF(spark, pathStanford)
    val rumSRDD: RDD[Map[String, String]] = Ds.importCsvRDD(spark, pathStanford)
    val titlesDf = Ds.importCsvDF(spark, path+"movies.csv")

    val help = "In the following:\n" +
      "-\t dataset   = \tL \t|\tS\n" +
      "\t where L corresponds to the dataset of movieLens and S corresponds to the Stanford one\n" +
      "-\t repr      = \tDF\t|\tRDD\n" +
      "\t the representation type, where DF stands for dataframe and RDD stands for resilient distributed dataset\n\n" +
      "Allowed operations:\n" +
      "*\t Movie Recommendation: computes user recommended movies, considering its previous visualizations and ratings.\n" +
      "\t usage\t: recommend dataset repr userID\n" +
      "\n" +
      "*\t Movie Ranking: computes movie ranking over the whole dataset.\n" +
      "\t usage\t: rank dataset repr\n" +
      "\n" +
      "*\t Movie Similarity: computes movies similar to the given one(s), considering the ratings of all the users.\n" +
      "\t The similarity can be computed either as union of all the results for each movie given as input or as their \n" +
      "\t intersection. In both cases, for results in the intersection, the overall score will be given by the average.\n" +
      "\t Similarity intersection\n" +
      "\t Computes the results similar to all the input movies.\n" +
      "\t usage\t: similarI dataset repr movieID(s)\n" +
      "\t Similarity union\n" +
      "\t Computes the results similar to at least one of the input movies.\n" +
      "\t usage\t: similarU dataset repr movieID(s)\n\n" +
      "\n" +
      "*\t Movie Time Analysis: creates a csv about the evolution of rating average in time.\n" +
      "\t The generated csv can be passed to MADsPlot to produce the corresponding plot.\n" +
      "\t The analysis can be performed either in an interval or years or in a given year by month.\n" +
      "\t Analysis in an interval of years\n" +
      "\t usage\t: evolutionY dataset repr yearBegin yearEnd movieID(s)\n" +
      "\t note\t: to not provide the year of begin (end), replace it with a '-'.\n" +
      "\t In this case MADs will consider the first(last) year recorded into the dataset.\n" +
      "\n" +
      "\t Analysis in a given year by month\n" +
      "\t usage\t: evolutionM dataset repr year movieID(s)\n" +
      "\n" +
      "*\t User Helpfulness: computes the helpfulness of all the users in the given dataset.\n" +
      "\t usage\t: helpfulness dataset repr\n" +
      "\t note\t: operation provided only for Stanford dataset, because movieLens one lacks of the helpfulness score.\n" +
      "\n" +
      "*\t Reprint the main commands\n" +
      "\t usage\t: help\n\n" +
      "*\t Print utils\n" +
      "\t usage\t: utils\n\n" +
      "*\t Exit from MADs\n" +
      "\t usage\t: exit"
    val utils = "Utils:\n" +
      "*\t Print movie title\n" +
      "\t note\t: operation provided only for movieLens, because the other dataset doesn't provide movie titles\n" +
      "\t usage\t: title dataset movieID\n\n" +
      "*\t Set the minimum number of movies to compare two users\n" +
      "\t usage\t: set minMovies number\n\n" +
      "*\t Set the minimum number of users to compare two movies\n" +
      "\t usage\t: set minUsers number\n\n" +
      "*\t Set the number of neighbors to consider in recommendation prediction and similarity\n" +
      "\t usage\t: set neighbors number\n\n" +
      "*\t Set weight for prior estimate to compute Bayesian ranking\n" +
      "\t usage\t: set minVotes number\n\n" +
      "*\t Set the maximum number of displayed results\n" +
      "\t usage\t: set results number\n\n" +
      "*\t Set the similarity method to be used in recommendation prediction and similarity\n" +
      "\t note\t: operation provided only for DataFrames\n" +
      "\t usage\t: set simMethod method\n" +
      "\t method = \tCOSINE \t|\tPEARSON\n\n"
    println(help)
    var elapsedTime = 0.0
    var limitResult = 20
    try {
      while (true) {
        try {
          val tmpOp = StdIn.readLine("> ")
          val op = tmpOp.trim.split("\\s+")
          op(0) match {
            case "recommend" | "rank" | "similarI"| "similarU" | "evolutionM" | "evolutionY" | "helpfulness" =>
              if (op.length >= 3) {
                val (rumRDD, rumDf) = op(1) match {
                  case "L" => (rumLRDD, rumLDf)
                  case "S" => (rumSRDD, rumSDf)
                  case _ => throw new InvalidOp("Not allowed operation")
                }
                if (!(op(2).equals("RDD") || op(2).equals("DF"))) {
                  throw new InvalidOp("Not allowed operation")
                }
                var result = spark.emptyDataFrame
                op(0) match {
                  case "recommend" =>
                    if (op.length != 4)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Recommendation...")
                    val user = op(3)
                    op(2) match {
                      case "RDD" => elapsedTime = time {val r = mrc.movieRecommendation(rumRDD, user)
                        .toDF("movieId", "moviePrediction")
                        .orderBy($"moviePrediction".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                      case "DF" => elapsedTime = time {val r = mrc.movieRecommendation(rumDf, user)
                        .orderBy($"moviePrediction".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                    }
                  case "rank" =>
                    if (op.length != 3)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Ranking...")
                    op(2) match {
                      case "RDD" => elapsedTime = time{val r = mrk.trueBayesianEstimate(rumRDD)
                        .toDF("movieId", "rank")
                        .orderBy($"rank".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                      case "DF" => elapsedTime = time{val r = mrk.trueBayesianEstimate(rumDf)
                        .orderBy($"rank".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                    }
                  case "similarI" =>
                    if (op.length < 4)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Similarity...")
                    val movie = op(3)
                    val movies:Seq[String]= op.slice(4,op.length)
                    op(2) match {
                      case "RDD" => elapsedTime = time{val r = ms.similarMoviesIntersect(rumRDD, movie, movies:_*)
                        .toSeq.toDF("movieId", "similarity")
                        .orderBy($"similarity".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                      case "DF" => elapsedTime = time{val r = ms.similarMoviesIntersect(rumDf, movie,movies:_*)
                        .orderBy($"similarity".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                    }
                  case "similarU" =>
                    if (op.length < 4)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Similarity...")
                    val movie = op(3)
                    val movies:Seq[String]= op.slice(4,op.length)
                    op(2) match {
                      case "RDD" => elapsedTime = time{val r = ms.similarMoviesUnion(rumRDD, movie, movies:_*)
                        .toSeq.toDF("movieId", "similarity")
                        .orderBy($"similarity".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                      case "DF" => elapsedTime = time{val r = ms.similarMoviesUnion(rumDf, movie,movies:_*)
                        .orderBy($"similarity".desc, $"movieId")
                        .limit(limitResult)
                        r.show(limitResult)
                        result = r
                      }
                    }
                  case "evolutionM" =>
                    if (op.length < 5)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Evaluation...")
                    val year = if (op(3).forall(_.isDigit)) op(3) else null
                    if (year == null)
                      throw new InvalidOp("Valid year not provided")
                    val movie = op(4)
                    val movies:Seq[String]= op.slice(5,op.length)
                    try {
                      op(2) match {
                        case "RDD" => elapsedTime = time{result = MovieTimeAnalysis.timeDFR(spark, rumRDD, movie, movies:_*)(byMonth = true, yBegin = year)}
                        case "DF" => elapsedTime = time{result = MovieTimeAnalysis.timeDFD(rumDf, movie, movies:_*)(byMonth = true, yBegin = year)}
                      }
                      val fileName = "PM" + LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))+".csv"
                      Ds.storeDfPt1(result, path + fileName)
                      println("Results saved in " + fileName)
                    } catch {
                      case _:UnsupportedOperationException => println("Not existing year in the dataset")
                    }
                  case "evolutionY" =>
                    if (op.length < 6)
                      throw new InvalidOp("Not allowed operation")
                    println("Computing Movie Evaluation...")
                    val yb = if (op(3).forall(_.isDigit)) op(3) else null
                    val ye = if (op(4).forall(_.isDigit)) op(3) else null
                    val movie = op(5)
                    val movies:Seq[String]= op.slice(6,op.length)
                    op(2) match {
                      case "RDD" => elapsedTime = time {result = MovieTimeAnalysis.timeDFR(spark, rumRDD, movie, movies: _*)(byMonth = false, yBegin = yb, yEnd = ye)}
                      case "DF" => elapsedTime = time {result = MovieTimeAnalysis.timeDFD(rumDf, movie, movies: _*)(byMonth = false, yBegin = yb, yEnd = ye)}
                    }
                    val fileName = "PY" + LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))+".csv"
                    Ds.storeDfPt1(result, path + fileName)
                    println("Results saved in " + fileName)
                  case "helpfulness" =>
                    if (op.length != 3)
                      throw new InvalidOp("Not allowed operation")
                    (op(1), op(2)) match {
                      case ("S", "RDD") =>
                        println("Computing User Helpfulness...")
                        elapsedTime = time{val r = UserHelpfulness.userHelpfulness(rumRDD).toDF(
                          "userId", "userHelpfulness")
                          .orderBy($"userHelpfulness".desc, $"userId")
                          .limit(limitResult)
                          r.show(limitResult)
                        }
                      case ("S", "DF") =>
                        println("Computing User Helpfulness...")
                        elapsedTime = time{val r = UserHelpfulness.userHelpfulness(rumDf)
                          .orderBy($"userHelpfulness".desc, $"userId")
                          .limit(limitResult)
                          r.show(limitResult)
                        }
                      case _ => throw new InvalidOp("Not allowed operation")
                    }
                }
                /*// Remove comment below to keep track of elapsed time
                val writer = new FileWriter(timePath, true)
                writer.write(elapsedTime + "\n")
                writer.close()*/
                op(0) match {
                  case "recommend" | "rank" | "similarU"| "similarI" =>
                    if(op(1).equals("L"))
                      StdIn.readLine("Movie title correspondence available.\nPress y to print the matches, " +
                        "something else otherwise\n> ") match {
                        case "y"|"yes" => Utils.getDfTitles(result, titlesDf).show(limitResult)
                        case _ =>
                      }
                  case _ =>
                }
              } else {throw new InvalidOp("Not allowed operation")}
            case "set" =>
              if (op.length != 3)
                throw new InvalidOp("Not allowed operation")
              op(1) match {
                case "minMovies" =>
                  if (!op(2).forall(_.isDigit))
                    throw new InvalidOp("Not allowed operation")
                  mrc.minCommonMovies_=(op(2).toInt)
                case "minUsers" =>
                  if (!op(2).forall(_.isDigit))
                    throw new InvalidOp("Not allowed operation")
                  ms.minCommonUsers_=(op(2).toInt)
                case "neighbors" =>
                  if (!op(2).forall(_.isDigit))
                    throw new InvalidOp("Not allowed operation")
                  ms.neighbors_=(op(2).toInt)
                  mrc.neighbors_=(op(2).toInt)
                case "minVotes" =>
                  if (!op(2).forall(_.isDigit))
                    throw new InvalidOp("Not allowed operation")
                  mrk.minVotes_=(op(2).toInt)
                case "results" =>
                  if (!op(2).forall(_.isDigit))
                    throw new InvalidOp("Not allowed operation")
                  limitResult = op(2).toInt
                case "simMethod" =>
                  op(2) match {
                    case "COSINE" | "PEARSON" =>
                      ms.simMethod_=(op(2))
                      mrc.simMethod_=(op(2))
                    case _ => throw new InvalidOp("Not allowed operation")
                  }
                case _ => throw new InvalidOp("Not allowed operation")
              }
            case "title" => if (op.length == 3 && op(1).equals("L")) {println(Utils.getMovieName(titlesDf, op(2)))}
            case "help" => println(help)
            case "utils" => println(utils)
            case "" =>
            case "exit" => throw AllDone
            case _ => throw new InvalidOp("Not allowed operation")
          }
        } catch {
          case e:InvalidOp => println(e.getMessage)
        }
      }
    } catch {case AllDone => println("Thanks for using MADs")}
    spark.sparkContext.stop()
  }
}
