package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {

  val STAGE_ID_KEY = "StageId"
  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val DevSchema = config.aux

    val MartSchema = config.pa

//    val AuxPShema  = "t_team_k7m_aux_p"


    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Score")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists
    val processLogger = new ProcessLogger(spark, config, "Score") // processName- наименование процесса,
    processLogger.logStartProcess()

    val StageId = obtainStageId(params)
    if ((StageId =="all") || (StageId=="1"))
    {val ScoreKeys = new ScoreKeysClass(spark,config).DoScoreKeys()}
    if ((StageId =="all") || (StageId=="2"))
    {val Score = new ScoreClass(spark,config).DoScore()}

    processLogger.logEndProcess()

  }


  def obtainStageId(params: Map[String, String]): String = {
    if (params.contains(STAGE_ID_KEY)) params(STAGE_ID_KEY) else "all"
  }
}



