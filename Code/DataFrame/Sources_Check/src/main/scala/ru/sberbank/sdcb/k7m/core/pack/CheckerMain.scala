package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object CheckerMain extends BaseMainClass with Functions {

	val spark: SparkSession = SparkSession.builder
		.appName("K7M_Sources_Check")
		.enableHiveSupport()
		.getOrCreate

	override def run(params: Map[String, String], config: Config): Unit = {

		import spark.implicits._

		val source = params("type")
		val S2Tfilepath = params("filePath")
		val node = params("node").toLowerCase
		val errorLogTable = ErrorLogTable(spark, config.aux)

		LoggerUtils.createTables(spark, config.aux)

		val processLogger = new ProcessLogger(spark, config, "K7M_Sources_Check") // processName- наименование процесса,
		processLogger.logStartProcess()

		S2T_Uploader(spark, config.pa, S2Tfilepath).createS2Ttable()
		errorLogTable.dropNsave()

		lazy val S2T_table = node match {
			case "stg"     => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "zero_layer")
			case "ods_stg" => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "ods_stg")
			case "mmz_stg" => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "mmz_stg")
			case "rdm_feature"  => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "rdm_feature")
			case "rl_stg"  => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "rl_stg")
			case _         => throw NoSuchNodeException()
		}

		lazy val listTablesToBeCreated: List[String] = S2T_table.select("Table").collect().map(_(0).toString).toList

		listTablesToBeCreated.foreach(table => {
			collectMetaForTable(table, S2T_table).foreach(
				tm => {
					val Logger = new EtlLoggerExtended(spark, tm.sourceFullName, config)
					//Logger.logStart()

					println(tm.sourceFullName + " in progress \n")
					println(getSelectStatement(tm) + "\n")
					if (!tableExists(tm, spark)) {
						errorLogTable.logNoTableError(tm.sourceFullName)
					} else {
						val selectWorks = errorLogTable.trySelect1Statement(tm.sourceFullName)
						val explainWorks = errorLogTable.tryExplainStatement(tm.sourceFullName, getSelectStatement(tm))
						val fieldsPresent = errorLogTable.checkFieldTypes(tm.sourceFullName, tm.fieldsTypesMap())

            if (selectWorks && explainWorks && fieldsPresent) {
              Logger.logInserted()
            }
					}
				}
			)
		}
		)
		val checkMainDocArc2016= spark.sql("""select count(*) cnt, sum( c_sum_nt ) c_sum_nt from custom_cb_k7m_stg.eks_z_main_docum_arc_2016""")
			.collect().toSeq.foreach(c=> if (c(0).toString != "3740308867" || c(1).toString != "15635602130185266.85")
		                                  {errorLogTable.logUnknownError("eks_z_main_docum_arc_2016",s"Данные не соответствуют эталону: ${c(0)}, ${c(1)}")})

		val checkRecordsIn2016= spark.sql("""select count(*) cnt from custom_cb_k7m_stg.eks_z_records_in_20161231""")
			.collect().toSeq.foreach(c=> if (c(0).toString != "9843054")
		{errorLogTable.logUnknownError("eks_z_records_in_20161231",s"Данные не соответствуют эталону: ${c(0)}")})

		processLogger.logEndProcess()
		if (errorLogTable.errorsCount > 0) throw FatalSourcesException()

	}

}
