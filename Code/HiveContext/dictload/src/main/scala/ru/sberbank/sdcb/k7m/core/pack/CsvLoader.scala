package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession
import sys.process._

class CsvLoader(val spark: SparkSession, val filePath: String, val config: Config, override val processName: String) extends EtlLogger with EtlJob {

  var dashboardName = ""

  val TEMP_PREFIX = "temp_"

  def load(desc: DictionaryDescription): Unit ={
    dashboardName = desc.tableName
    logStart()
    val tempFilePath = copyFile(desc)
    dropAndCreate(desc)
    loadData(desc, tempFilePath)
    logInserted()
    logEnd()
  }

  private def copyFile(desc: DictionaryDescription): String = {
    val fullPath: String = filePath + desc.fileName
    val tempFilePath: String = filePath + TEMP_PREFIX + desc.fileName
    s"""hdfs dfs -cp -f $fullPath $tempFilePath""".!
    tempFilePath
  }

  private def dropAndCreate(desc: DictionaryDescription): Unit = {
    spark.sql(desc.dropTableQuery)
    spark.sql(desc.createTableQuery)
  }

  private def loadData(desc: DictionaryDescription, tempFilePath: String): Unit ={
    spark.sql(s"""load data inpath '$tempFilePath' overwrite into table ${desc.tableName}""")
  }

}
