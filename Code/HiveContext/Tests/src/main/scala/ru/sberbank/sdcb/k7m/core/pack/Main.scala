package ru.sberbank.sdcb.k7m.core.pack


import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.Tables._
import ru.sberbank.sdcb.k7m.core.pack.utils.{Check, TestsLogger}

object Main extends BaseMainClass{

	val spark: SparkSession = SparkSession.builder
		.appName("K7M_Tests")
		.enableHiveSupport()
		.getOrCreate

	override def run(params: Map[String, String], config: Config): Unit = {

		val `type`   = params("type")
		val levelStr = params("check_lvl")
		val level 	 = levelStr.toInt


		val offlineChecks = OfflineChecks(spark, config, `type`)

		if (level == 1) {
			val filepath = params("filePath")
			offlineChecks.createTestsTable(filepath)
			ClfCheck(spark, config).dropAndCreate()
			CluCheck(spark, config).dropAndCreate()
			ScoreCheck(spark, config).dropAndCreate()
			Error(spark, config).dropAndCreate()
		}

	  val exec_id = LoggerUtils.obtainExecId(spark, config.aux)

		lazy val checks: List[Check] = offlineChecks.genChecksListsForLvl(level)

		val processLogger = new ProcessLogger(spark, config, s"CHECK$levelStr")
		processLogger.logStartProcess()
		checks.foreach(check => Tester(spark, config).tryTestStatement(check, levelStr, exec_id))

		var checkName: String = null
		level match	{
			case 2 =>
				checkName = "all_2"
			case 3 =>
				checkName = "all_3"
			case _ =>
		}
		if (checkName != null) {
			val checks_final = offlineChecks.genFinalChecks(checkName)
			checks_final.foreach(check => Tester(spark, config).tryTestStatement(check, levelStr, exec_id))
		}
		processLogger.logEndProcess()
	}
}
