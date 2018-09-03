package My_Utils

import java.text.SimpleDateFormat

object Log {

	private val sdf = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss.SSS")

	def printStages(): Unit = {
		println(
			s"""
				 |Stages of Basis KSB:
				 | 1) Table 1
				 | 2) Table 2
				 | 3) Table 3 (long one, z_main_docum involved)
				 | 4) Table 4
				 | 5) Table dt1_hive010617
				 | 6) Table kt1_hive010617
				 | 7) Table distclient_hive010617
				 | 8) Table innsec_hive010617
				 | 9) Table 5 (final)
			""".stripMargin)}

	def printStats(table_name: String): Unit = {
		println(s"Table $table_name started at ${sdf.format(System.currentTimeMillis())}")
	}


}
