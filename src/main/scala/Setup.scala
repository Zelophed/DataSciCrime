import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

object Setup {

	val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("Application"))
	sc.setLogLevel("WARN")
	val sql = new SQLContext(sc)

	var keyNameLUT: Map[String, String] = _
	var results: Array[(String, List[Combined])] = _
	var tCombinedFiltered: RDD[(String, List[Combined])] = _
	var tCombinedUnfiltered: RDD[(String, List[Combined])] = _


	def main(args: Array[String]): Unit = {

		init()

		//saveToDatabase()


		/*results
			.filterNot(entry => entry._1.contains("*"))
			.filter(entry => entry._2.map(_.year) == allYears)
			.map(entry => (entry._1, keyNameLUT(entry._1)))
			.sortBy(_._1)
			.foreach(println)
*/

	}

	def init(): Unit = {
		import sql.implicits._

		val dfT01 = sql
			.read
			.option("header", "true")
			.option("inferSchema", "true")
			.option("sep", ";")
			.option("encoding", "windows-1252")
			.csv("ZR-F-01-T01-Faelle_csv.csv")

		val t01 = dfT01.rdd

		val keyFixer = udf((key: String) => {
			if (key.length < 6) {
				key.toInt.formatted("%06d")
			} else if (key.contains("E+")) {
				key.replace(".00E+", "**")
			} else key
		})

		val dfT20 = sql
			.read
			.option("header", "true")
			.option("inferSchema", "true")
			.option("sep", ";")
			.option("encoding", "utf-8")
			.csv("ZR-TV-01-T20-TV-insg_csv.csv")
			.withColumn("Schluessel", keyFixer($"Schluessel"))

		val t20 = dfT20.rdd

		//dfT20.printSchema()
		//look up table from key to human name
		keyNameLUT = t01
			.groupBy(row => row.getString(0))
			.mapValues(rows => rows.maxBy(_.getInt(2)).getString(1))
			.collect()
			.toMap


		val t01pair = t01.map(row => (row.getString(0), row))
		val t20pair = t20.map(row => (row.getString(0), row))

		val keys = t01pair
			.join(t20pair)
			//.map(row => (row._1, row._2._1.getString(1), row._2._2.getString(1)))
			.map(row => row._1)
			.distinct()
			.collect()
			.sortBy(identity)

		//println("keys in both: " + keys.length)

		//		println("both keys")
		//		both
		//			.foreach(println)

		val t01mapped = t01.map(row => ((row.getString(0), row.getInt(2)), Case.fromRow(row)))

		val t20mapped = t20.map(row => ((row.getString(0), row.getInt(2)), Suspects.fromRow(row)))

		val tCombined = t01mapped
			.join(t20mapped)
			//.filter(entry => entry._1._2 >= 1993)
			.sortByKey()


		tCombined
			.take(20)
			.foreach(println)

		results = tCombined
			.groupBy(row => row._1._1)
			.mapValues(entries => entries.map(entry => Combined.fromSingles(entry._2._1, entry._2._2)).toList)
			.filter(entry => !entry._1.contains("*"))
			.filter(entry => entry._2.map(_.year) == allYears)
			.collect()

		//def allYears = (1987 to 2021).toList
		def allYears = (1993 to 2021).toList

		tCombinedUnfiltered = tCombined
			.groupBy(row => row._1._1)
			.mapValues(entries => entries.map(entry => Combined.fromSingles(entry._2._1, entry._2._2)).toList)

		tCombinedFiltered = tCombinedUnfiltered
			.filter(!_._1.contains("*"))
			.filter(entry => entry._2.map(_.year) == allYears)
	}

	def saveToDatabase(): Unit = {

		println("entries with all years: " + results.length)


		//---

		val jdbcUrl = "jdbc:mysql://192.168.178.98:5001/v3"
		//val jdbcUrl = "jdbc:mysql://127.0.0.1:5001/v2"
		val connProperties = new Properties()
		connProperties.put("user", "root")
		connProperties.put("password", "QiLaSlBTRqYweJiZibMD")
		val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
		Class.forName(MYSQL_DRIVER)


		results
			//.slice(0, results.length/4)
			.foreach(tuple => {
				sql
					.createDataFrame(tuple._2)
					.sort(col("year").asc)
					.write
					.mode(SaveMode.Overwrite)
					.jdbc(jdbcUrl, s"`${tuple._1}`", connProperties)
			})

	}

}
