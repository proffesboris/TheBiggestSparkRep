package ru.sberbank.sdcb.k7m.core.pack;

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.util.Date
import java.text.SimpleDateFormat

object BkpMain extends BaseMainClass{

  val SCHEMA_KEY = "schema"
  val paCode = "pa"
  val auxCode = "aux"
  val allCode = "all"

  override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
      .appName("BKP")
      .enableHiveSupport()
      .getOrCreate()

    val dt = new SimpleDateFormat("yyMMdd").format( new Date())

    val schema = obtainSchema(params)

    if (schema==paCode || schema==allCode) {
      backupSchema(spark.sqlContext.tableNames(s"${config.pa}").toSeq, spark,config,dt,paCode)
    }

    if (schema==auxCode || schema==allCode) {
      backupSchema(spark.sqlContext.tableNames(s"${config.aux}").toSeq, spark,config,dt,auxCode)
    }
  }

  def backupSchema(tabNames: Seq[String], spark: SparkSession, config: Config, dt: String, code: String)
  {
    tabNames.foreach(c => if (!spark.sqlContext.tableNames(s"${config.bkp}").contains(s"${code}_${dt}_$c")) {
      val schema =  code match {
        case `auxCode` => config.aux
        case `paCode` => config.pa}

      try{spark.table(s"${schema}.$c")
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", s"${config.bkpPath}${code}_${dt}_$c")
      .saveAsTable(s"${config.bkp}.${code}_${dt}_$c")}
    catch {case _:Throwable => println(s"${schema}.$c")}
    })
  }

  def obtainSchema(params: Map[String, String]): String = {
    if (params.contains(SCHEMA_KEY)) params(SCHEMA_KEY) else allCode
  }
}
