package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class EioEgrul(override val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val processName = "EIO_egrul"
  override val dashboardName = s"${config.aux}.EIO_egrul"

  val zip = udf((arr0: Seq[String], arr1: Seq[String], arr2: Seq[String], arr3: Seq[String]) => {
    arr0.indices.map(i => (arr0(i), arr1(i), arr2(i), arr3(i)))
  })

  def run(): Unit = {
    logStart()

    spark
      .table(s"${config.stg}.fns_egrul")
      .select(
        col("inn")
        , col("SvedDolzhnFL.SvDolzhn.NaimDolzhn") as "Position"
        , col("SvedDolzhnFL.SvFL.Familiya") as "S_NAME"
        , col("SvedDolzhnFL.SvFL.Imya") as "F_NAME"
        , col("SvedDolzhnFL.SvFL.Otchestvo") as "L_NAME"
      )
      .withColumn("zip",
        when(
          col("Position").isNotNull,
          zip(col("Position"), col("S_NAME"), col("F_NAME"), col("L_NAME"))
        ))
      .withColumn("zip", explode_outer(col("zip")))
      .select(
        col("inn")
        , col("zip._1") as "Position"
        , col("zip._2") as "S_NAME"
        , col("zip._3") as "F_NAME"
        , col("zip._4") as "L_NAME"
      )
      .write
      .format("parquet")
      .option("path", s"${config.auxPath}EIO_egrul")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${config.aux}.EIO_egrul")

    logInserted()

    logEnd()
  }

}