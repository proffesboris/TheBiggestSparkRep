package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends BaseMainClass with Functions {

  val spark: SparkSession = SparkSession.builder
    .appName("K7M_Zero_layer")
    .enableHiveSupport()
    .getOrCreate

  import spark.implicits._

  override def run(params: Map[String, String], config: Config): Unit = {

    val source = params("type")
    val node = params("node").toLowerCase

    val saveSchema = config.stg
    val savePath = config.stgPath

    val tableCreationString = params("tables").toLowerCase() // tables separated by comma without space or "all"
    val schemaCreationString = params("schemas").toLowerCase() // schemas separated by comma without space or "all"

    val schemasCreationList = schemaCreationString.split(",").toList
    val tablesCreationList = tableCreationString.split(",").toList

    val S2T_table = node match {
      case "stg"     => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "zero_layer")
      case "ods_stg" => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "ods_stg")
      case "mmz_stg" => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "mmz_stg")
      case "rl_stg"  => S2T_Stage(spark, config.pa, source).finalDataFrame.where($"Node" === "rl_stg")
      case _         => throw NoSuchNodeException()
    }

    val S2T_tableFiltered: DataFrame = if (schemaCreationString == "all") {
      S2T_table
    } else {
      S2T_table.where($"schema".isin(schemasCreationList: _*))
    }

    val S2T_tableFilteredCached = S2T_tableFiltered.cache()

    val listTablesToBeCreated: List[String] = if (tableCreationString == "all") {
      S2T_tableFilteredCached.select("Table").collect().map(_ (0).toString).toList
    }
    else {
      tablesCreationList
    }


    val ZeroLayerLogger = new EtlLoggerExtended(spark, "",  config)

    ZeroLayerLogger.logStartProcess()

    listTablesToBeCreated.foreach(table => {
      collectMetaForTable(table, S2T_tableFilteredCached).foreach(
        tm => {
          val TableLogger = new EtlLoggerExtended(spark, tm.saveFullName(saveSchema), config)

          if (!tableIsInS2T(tm, S2T_tableFilteredCached)) TableLogger.errorNoTableInS2T()

          else if (!tableExists(tm, spark)) TableLogger.errorNoTableInHive()

          else if (tableEmpty(tm, spark)) {
            TableLogger.errorSourceTableEmpty()
            dropAndSaveTable(tm, spark, saveSchema, savePath)
            TableLogger.logInserted()
            throw new NoDataException(tm.name)
          } else {
            print(tm + " "+ saveSchema + "" +savePath)

            dropAndSaveTable(tm, spark, saveSchema, savePath)
            TableLogger.logInserted()
          }
        }
      )
    })

    ZeroLayerLogger.logEndProcess()
  }

}
