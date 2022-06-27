import plotly._
import element._
import layout._
import Plotly._

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Random

object Plot {

	var plotValues: Map[String, List[(Int, Double)]] = _
	var mCombined: Map[String, List[Combined]] = _
	var keyNameLUT: Map[String, String] = _

	def main(args: Array[String]): Unit = {

		//import Setup._
		//init()
		//mCombined = tCombinedUnfiltered.collectAsMap().toMap
		//saveToJson()

		readFromJson()

		setupPlot()

		runPlot()


	}


	def write(filePath:String, contents:String) = {
		Files.write(Paths.get(filePath), contents.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE)
	}

	def read(filePath:String):String = {
		Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8).asScala.mkString
	}

	def saveToJson(): Unit = {

		import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

		write("keyNameLUT.json", keyNameLUT.asJson.noSpaces)

		write("mCombined.json", mCombined.asJson.noSpaces)


	}

	def readFromJson(): Unit = {

		import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

		keyNameLUT = decode[Map[String, String]](read("keyNameLUT.json")).getOrElse(throw new AssertionError())
		mCombined = decode[Map[String, List[Combined]]](read("mCombined.json")).getOrElse(throw new AssertionError())

	}

	def setupPlot(): Unit = {
		def yearRange13: Combined => Boolean = c => c.year >= 2005 && c.year <= 2017
		def yearRange93: Combined => Boolean = c => c.year >= 1988 && c.year <= 1997
		def yeaRangeAll: Combined => Boolean = c => c.year >= 1988 && c.year <= 2017

		def hasYears13: List[Combined] => Boolean = c => (2005 to 2017).forall(year => c.exists(_.year == year))
		def hasYears93: List[Combined] => Boolean = c => (1988 to 1997).forall(year => c.exists(_.year == year))
		def hasYearsAll: List[Combined] => Boolean = c => (1988 to 2017).forall(year => c.exists(_.year == year))

		val map = mCombined
			.filter(!_._1.contains("*"))
			.filter(t => hasYears13(t._2))
			.mapValues(_
				.filter(yearRange13)
				.map(combined => (combined.year, combined.suspects))
			)

		println("map size: " + map.size)

		plotValues = map
			.mapValues(list => {
				val max = list.maxBy(_._2)._2

				list
					.map(tuple => (
						tuple._1,
						tuple._2.toDouble / max
					))
			})

		println(map("100000"))


	}


	def runPlot(): Unit = {

		val random = new Random()

		def colRandom: () => Color.RGBA = () => Color.RGBA.apply(
			random.nextInt(255),
			random.nextInt(255),
			random.nextInt(255),
			0.05
		)
		def colBlack: () => Color.RGBA = () => Color.RGBA.apply(0, 0, 0, 0.1)

		val traces = plotValues
			.toList
			.sortBy(_._1)

			.map(tuple => Scatter()
				.withX(tuple._2.map(_._1))
				.withY(tuple._2.map(_._2))
				.withName(tuple._1)
				.withLine(Line()
					.withShape(LineShape.Spline)
					.withWidth(7)
					.withColor(colBlack())
				)
			)

		val averages: List[(Int, Double)] = plotValues
			.toList
			.flatMap(_._2)
			.groupBy(_._1)
			.mapValues(pairs => pairs.map(_._2).map(n => if (n.isNaN) 0 else n).sum / pairs.count(!_._2.isNaN))
			.toList
			.sortBy(_._1)

		val extendedTraces = traces ++ List(
			Scatter()
				.withX(averages.map(_._1))
				.withY(averages.map(_._2))
				.withName("averages")
				.withLine(Line()
					.withShape(LineShape.Spline)
					.withDash(Dash.Dot)
					.withWidth(2.5)
					.withColor(Color.RGBA(127, 0, 0, 1))
				)
		)


		val layout = Layout().withTitle("Curves")
		//plot.plot("plot", layout)

		val f = new File("plot.html")
		if (f.exists())
			f.delete()

		Plotly.plot("plot.html", extendedTraces, layout, addSuffixIfExists = false)


	}

}
