package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.tran_misc.Tables

object BasisTransactDelta extends BaseMainClass{

	val spark: SparkSession = SparkSession
		.builder()
		.appName("BasisTranDelta")
		.enableHiveSupport()
		.config("hive.exec.dynamic.partition", "true")
		.config("hive.exec.dynamic.partition.mode", "nonstrict")
		.config("hive.exec.max.dynamic.partitions", "10000")
		.config("hive.exec.max.dynamic.partitions.pernode", "10000")
		.getOrCreate()

	val STAGE_ID_KEY = "stageId"
	val SOURCE_KEY 	 = "source"

	override def run(params: Map[String, String], config: Config): Unit = {

    val StageId = params(STAGE_ID_KEY)
		val sourceBasis = params(SOURCE_KEY)

		val tables = sourceBasis match {
			case "basis_eks"  =>
				Tables(s"${config.pa}.basis_eks", s"${config.pa}.basisclasseks", s"pdeksin")
			case "basis_eks2" =>
				Tables(s"${config.pa}.basis_eks2", s"${config.pa}.basisclasseks2", s"pdeksin2")
		}
		val processLogger = new ProcessLogger(spark, config, sourceBasis) // processName- наименование процесса,
		processLogger.logStartProcess()

		lazy val delta = new BasisTrDeltaClass(spark, config, tables)
		lazy val hist  = new BasisTrHistClass(spark, config, tables)
		lazy val out   = new BasisTrOutClass(spark, config, tables)

		StageId match {
			case "Delta" 		  => delta.DoDelta()
			case "HistAndOut" => {
				hist.logStart(step = "TransactHistAndOut")
				hist.DoHist()
				out.DoOut()
				out.logEnd(step = "TransactHistAndOut")
			}
      case "Out"        => {
				out.logStart(step = "TransactOut")
				out.DoOut()
				out.logEnd(step = "TransactOut")
			}
		}

		processLogger.logEndProcess()
  }

}
