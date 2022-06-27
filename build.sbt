name := "DataSciCrime"

version := "0.1"

scalaVersion := "2.12.13"

val SparkVersion = "2.4.7"

libraryDependencies ++= Seq(
	"org.scalactic" %% "scalactic" % "3.1.1",
	"org.scalatest" %% "scalatest" % "3.1.1" % "test",
	"org.apache.spark" %% "spark-core" % SparkVersion,
	"org.apache.spark" %% "spark-sql" % SparkVersion,
	"mysql" % "mysql-connector-java" % "8.0.27",
	"org.plotly-scala" %% "plotly-render" % "0.8.1"
)

val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
	"io.circe" %% "circe-core",
	"io.circe" %% "circe-generic",
	"io.circe" %% "circe-parser"
).map(_ % circeVersion)




