package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class RDOC(override val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val processName = "RDOC"
  var dashboardName = "RDOC"

  def run(): Unit = {
    logStart()

    dashboardName = s"${config.pa}.RDOC"

    val rdoc =
      if (spark.catalog.tableExists(config.stg, "RL_CONCLUSION") && !spark.table(s"${config.stg}.RL_CONCLUSION").rdd.isEmpty()) {

        spark.table(s"${config.stg}.RL_CONCLUSION")
          .select(explode(col("legalEntities")) as "legalEntities")
          .select(
            col("legalEntities.id")
            , col("legalEntities.result")
            , explode(col("legalEntities.legalEntity.missingDoc")) as "missingDoc")
          .createOrReplaceTempView("rlc")

        spark.sql(
          s"""SELECT u7m_id, missingDoc
             |FROM ${config.pa}.CLU
             |INNER JOIN rlc on
             |    CLU.CRM_ID = rlc.id""".stripMargin)
          .selectExpr(
            s"$execId as EXEC_ID"
            , "cast(null as bigint) as OKR_ID"
            , "cast(row_number() over (order by u7m_id) as string) as RDOC_ID"
            , "'CLU' as OBJ"
            , "u7m_id as 7M_ID"
            , "missingDoc.name as DOC_NAME"
            , "missingDoc.number as DOC_NUM"
            , "missingDoc.docDate as DOC_DATE"
            //,"missingDoc.docSource.name as PARENT_DOC_NAME"
            , "cast(null as string) as PARENT_DOC_NAME"
            //,"missingDoc.docSource.number as PARENT_DOC_NUM"
            , "cast(null as string) as PARENT_DOC_NUM"
            //,"missingDoc.docSource.docDate as PARENT_DOC_DATE"
            , "cast(null as string) as PARENT_DOC_DATE"
            , "cast(null as string) as REASON_REQUEST_TXT"
            , "missingDoc.doc_type as ID_DOC_ECM"
            , "cast(null as string) as Ext_F28"
            , "cast(null as string) as Ext_F29"
            , "cast(null as string) as Ext_F30"
          )

      } else {
        val schema = StructType(List(StructField("EXEC_ID", IntegerType, false), StructField("OKR_ID", LongType, true), StructField("RDOC_ID", StringType, true), StructField("OBJ", StringType, false), StructField("7M_ID", StringType, true), StructField("DOC_NAME", StringType, true), StructField("DOC_NUM", StringType, true), StructField("DOC_DATE", StringType, true), StructField("PARENT_DOC_NAME", StringType, true), StructField("PARENT_DOC_NUM", StringType, true), StructField("PARENT_DOC_DATE", StringType, true), StructField("REASON_REQUEST_TXT", StringType, true), StructField("ID_DOC_ECM", StringType, true), StructField("Ext_F28", StringType, true), StructField("Ext_F29", StringType, true), StructField("Ext_F30", StringType, true)))
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }

    rdoc
      .write
      .format("parquet")
      .option("path", s"${config.paPath}RDOC")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${config.pa}.RDOC")

    logInserted()

    logEnd()
  }

}