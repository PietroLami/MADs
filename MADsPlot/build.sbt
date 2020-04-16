name := "MADsPlotter"

version := "0.1"

scalaVersion := "2.11.10"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.vegas-viz" %% "vegas" % "0.3.9",
  "org.vegas-viz" %% "vegas-spark" % "0.3.9",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.683",
  "com.typesafe" % "config" % "1.4.0"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "MADsPlot.jar"
assemblyOutputPath in assembly := file("./MADsPlot.jar")