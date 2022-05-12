
case class Case(
				   key: String,
				   crime: String,
				   year: Int,
				   amount: Int,
				   attempts: Int,
				   clearanceRate: Double
			   )

case class Suspects(
					   key: String,
					   crime: String,
					   year: Int,
					   amount: Int,
					   sUnder6: Int,
					   s6to8: Int,
					   s8to10: Int,
					   s10to12: Int,
					   s12to14: Int,
					   s14to16: Int,
					   s16to18: Int,
					   s18to21: Int,
					   s21to23: Int,
					   s23to25: Int,
					   s25to30: Int,
					   s30to40: Int,
					   s40to50: Int,
					   s50to60: Int,
					   s60plus: Int
				   )

case class Combined(
					   year: Int,
					   cases: Int,
					   attempts: Int,
					   clearanceRate: Double,
					   suspects: Int,
					   sUnder6: Int,
					   s6to8: Int,
					   s8to10: Int,
					   s10to12: Int,
					   s12to14: Int,
					   s14to16: Int,
					   s16to18: Int,
					   s18to21: Int,
					   s21to23: Int,
					   s23to25: Int,
					   s25to30: Int,
					   s30to40: Int,
					   s40to50: Int,
					   s50to60: Int,
					   s60plus: Int
				   )

object Case {
	def fromRow(row: org.apache.spark.sql.Row): Case = Case(
		row.getString(0),
		row.getString(1),
		row.getInt(2),
		Types.parseInt(row.getString(3)),
		Types.parseInt(row.getString(5)),
		row.getDouble(9)
	)
}

object Suspects {
	def fromRow(row: org.apache.spark.sql.Row): Suspects = Suspects(
		row.getString(0),
		row.getString(1),
		row.getInt(2),
		Types.parseInt(row.getString(3)),
		Types.parseInt(row.getString(4)),
		Types.parseInt(row.getString(5)),
		Types.parseInt(row.getString(6)),
		Types.parseInt(row.getString(7)),
		Types.parseInt(row.getString(8)),
		Types.parseInt(row.getString(11)),
		Types.parseInt(row.getString(12)),
		Types.parseInt(row.getString(15)),
		Types.parseInt(row.getString(19)),
		Types.parseInt(row.getString(20)),
		Types.parseInt(row.getString(23)),
		Types.parseInt(row.getString(24)),
		Types.parseInt(row.getString(25)),
		Types.parseInt(row.getString(26)),
		Types.parseInt(row.getString(27))
	)
}

object Combined {

	def fromSingles(cases: Case, suspects: Suspects): Combined =
		Combined(
			cases.year,
			cases.amount,
			cases.attempts,
			cases.clearanceRate,
			suspects.amount,
			suspects.sUnder6,
			suspects.s6to8,
			suspects.s8to10,
			suspects.s10to12,
			suspects.s12to14,
			suspects.s14to16,
			suspects.s16to18,
			suspects.s18to21,
			suspects.s21to23,
			suspects.s23to25,
			suspects.s25to30,
			suspects.s30to40,
			suspects.s40to50,
			suspects.s50to60,
			suspects.s60plus
		)

}

object Types {
	def parseInt(string: String): Int = string.replaceAll(",", "").toInt
}
