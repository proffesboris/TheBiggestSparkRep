package ru.sberbank.sdcb.k7m.core.pack

import org.apache.hadoop.fs._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.Utils._
import ru.sberbank.sdcb.k7m.java.{Utils => JavaUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class BO(override val spark: SparkSession, val config: Config, date: String) extends EtlJob with EtlIntegrLogger with EtlLogger {

  override val processName = "VDOKROutBO"
  var dashboardName: String = "VDOKROutBO"

  override val logSchemaName: String = config.aux
  val targPath = s"${config.paPath}/export/vdokrout"
  val boPack = s"$targPath/7m_1_${date}_bo"

  val conf = spark.sparkContext.hadoopConfiguration
  implicit val fs = FileSystem.get(conf)
  fs.delete(new Path(boPack), true)

  //---------------------------------------------------------

  case class Data(name: String, df: DataFrame)

  def csvOptions = Map(
    "sep" -> ";",
    "header" -> "false",
    "escape" -> "\"",
    "dateFormat" -> "yyyy-MM-dd",
    "timestampFormat" -> "yyyy-MM-dd")

  def run() {

    logIntegrStart()

    dashboardName = s"${config.aux}.vdokrout_bo"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_bo")

    spark.sql(s"""create table ${config.aux}.vdokrout_bo as
                 |select
                 |    c.CRM_ID, -- для деления на пакеты по клиентам
                 |    a.BO_ID,
                 |    a.U7M_ID,
                 |    a.CLUST_ID
                 |  from
                 |    ${config.pa}.BO_KEYS a
                 |    inner join ${config.aux}.vdokrout_score b on a.U7M_ID = b.U7M_ID
                 |    inner join ${config.aux}.vdokrout_clu c on a.U7M_ID = c.U7M_ID
                 |    where not exists (
                 |      select bo_id,u7m_id
                 |      from ${config.aux}.bo_incomplete bi
                 |      where bi.bo_id=a.bo_id
                 |          and bi.u7m_id = a.u7m_id)
                 | """.stripMargin)

    val bo = Data("BO", spark.table(s"${config.aux}.vdokrout_bo"))

    logIntegrInserted()
    logInserted()

    dashboardName = s"${config.aux}.vdokrout_dt"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_dt")

    spark.sql(s"""create table ${config.aux}.vdokrout_dt as
              select
                      a.CRM_ID, -- для деления на пакеты по клиентам
                     'BO' as obj,
                     a.BO_ID as obj_id,
                     a.KEY,
                     a.VALUE_C,
                     a.VALUE_N,
                     a.VALUE_D
               from
                    (
                     select
                        distinct
                          ai.BO_ID,
                          ai.KEY,
                          ai.VALUE_C,
                          ai.VALUE_N,
                          ai.VALUE_D,
                          bi.CRM_ID,
                          ai.U7M_ID,
                          ai.BO_ID
                      from
                         ${config.pa}.BO ai
                         inner join ${config.aux}.vdokrout_clu bi
                       on ai.U7M_ID = bi.U7M_ID
                      where  ai.KEY not in ('BO_Q_SRC','BO_OfferExpDate', 'EADRegimeM', 'CCF_C_OFFLINE')
                      ) a
                     inner join  ${config.aux}.vdokrout_bo vb
                     on a.U7M_ID = vb.U7M_ID
                        and a.bo_ID = vb.bo_ID
                 """.stripMargin)

    logIntegrInserted()
    logInserted()

    val dt = Data("DT", spark.table(s"${config.aux}.vdokrout_dt"))

    Seq(bo, dt).foreach { case Data(name, df) =>

      dashboardName = s"$boPack/$name.csv"

      val count = df.count()
      val header = df.drop("CRM_ID").getHeader()//20180502 Ключко П. Убрать из заголовка

      df
        .replace("[\\n,\\r,\\;]", " ")
        .formatNumbers()
        .write
        .mode(SaveMode.Append)
        .options(csvOptions)
        .partitionBy("CRM_ID")
        .csv(s"$boPack/")

      Await.result(Future.sequence(
        fs.listStatus(new Path(boPack)).toSeq.map {
        case fileStatus if fileStatus.getPath.getName.startsWith("CRM_ID=") => executeBlockingIO {

          val crmId = fileStatus.getPath.getName.split("=")(1)

          JavaUtils.copyMerge(
            fs, fileStatus.getPath,
            fs, new Path(s"$boPack/$crmId/$name.csv"),
            true, conf,
            null, header)
        }
        case _ => Future.successful(false)

      }), Duration.Inf)

      logIntegrSaved(count)
    }

    Await.result(Future.sequence(
      fs.listStatus(new Path(boPack)).toSeq.map {
        case fileStatus if (fileStatus.isDirectory && !fileStatus.getPath.getName.startsWith("_")) => executeBlockingIO {
          ZipUtils.zipDir(fileStatus.getPath)
          true
        }
        case fileStatus => executeBlockingIO {
          fs.delete(fileStatus.getPath, true)
        }
      }), Duration.Inf)

    ZipUtils.zipDir(new Path(boPack), false)

    logIntegrEnd()
  }

}
