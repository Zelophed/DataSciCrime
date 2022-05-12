import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

object Setup {

	def main(args: Array[String]): Unit = {

		val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("Application"))
		val sql = new SQLContext(sc)

		import sql.implicits._

		val dfT01 = sql
			.read
			.option("header", "true")
			.option("inferSchema", "true")
			.option("sep", ";")
			.option("encoding", "windows-1252")
			.csv("ZR-F-01-T01-Faelle_csv.csv")

		case class Case2(key: String, crime: String, year: Int, amount: Int, attempts: Int, clearanceRate: Float)
		case class Suspects2(key: String, crime: String, year: Int, amount: Int, sUnder6: Int, s6to8: Int, s8to10: Int, s10to12: Int, s12to14: Int, s14to16: Int, s16to18: Int, s18to21: Int, s21to23: Int, s23to25: Int, s25to30: Int, s30to40: Int, s40to50: Int, s50to60: Int, s60plus: Int)

		val t01 = dfT01.rdd

		val keyFixer = udf((key: String) => {
			if (key.contains("E+")) {
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
		val keyNameLUT = t01
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

		println("keys in both: " + keys.length)

		//		println("both keys")
		//		both
		//			.foreach(println)

		val t01mapped = t01.map(row => ((row.getString(0), row.getInt(2)), Case.fromRow(row)))

		val t20mapped = t20.map(row => ((row.getString(0), row.getInt(2)), Suspects.fromRow(row)))

		val tCombined = t01mapped
			.join(t20mapped)
			.sortByKey()


		tCombined
			.take(20)
			.foreach(println)

		val results = tCombined
			.groupBy(row => row._1._1)
			.mapValues(entries => entries.map(entry => Combined.fromSingles(entry._2._1, entry._2._2)).toList)
			.collect()


		def allYears = (1987 to 2021).toList

		println("entries with all years: " + results
			.filterNot(entry => entry._1.contains("*"))
			.count(entry => entry._2.map(_.year) == allYears))


		//---

		val jdbcUrl = "jdbc:mysql://192.168.178.98:5001/v2"
		//val jdbcUrl = "jdbc:mysql://127.0.0.1:5001/v2"
		val connProperties = new Properties()
		connProperties.put("user", "root")
		connProperties.put("password", "QiLaSlBTRqYweJiZibMD")
		val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
		Class.forName(MYSQL_DRIVER)


		results
			//.slice(0, results.length/4)
			.filterNot(entry => entry._1.contains("*"))
			.filter(entry => entry._2.map(_.year) == allYears)
			.foreach(tuple => {
				sql
					.createDataFrame(tuple._2)
					.sort(col("year").asc)
					.write
					.mode(SaveMode.Overwrite)
					.jdbc(jdbcUrl, s"`${tuple._1}`", connProperties)
			})


		results
			.filterNot(entry => entry._1.contains("*"))
			.filter(entry => entry._2.map(_.year) == allYears)
			.map(entry => (entry._1, keyNameLUT(entry._1)))
			.sortBy(_._1)
			.foreach(println)


	}

}
