name := """fijbook-gamma"""

version := "1.0"

scalaVersion := "2.11.7"

lazy val spark = "2.0.2"


// Spark related dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-streaming" % spark,
  "org.apache.spark" %% "spark-graphx" % spark,
  "org.apache.spark" %% "spark-mllib" % spark,
  "com.databricks" %% "spark-csv" % "1.3.0",
  "mysql" % "mysql-connector-java" % "5.1.34"
)

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"


// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

