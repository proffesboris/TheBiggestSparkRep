package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FOKHeaderClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_fok_headerIN = s"${Stg0Schema}.fok_docs_header"
  val Node2t_team_k7m_aux_d_k7m_fok_headerIN = s"${Stg0Schema}.crm_s_org_ext_x"
  val Nodet_team_k7m_aux_d_k7m_fok_headerOUT = s"${DevSchema}.k7m_fok_header"
  val dashboardPath = s"${config.auxPath}k7m_fok_header"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_fok_headerOUT //витрина
  override def processName: String = "EL"

  def DoFOKHeader() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_fok_headerOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
    h.id,
    h.divid,
    h.docs_status,
    h.version,
    h.create_date,
    h.modify_date,
    h.title,
    h.year,
    h.period,
    h.formid,
    h.user_name,
    h.dealid,
    h.template_datefr,
    e.SBRF_INN
  from $Node1t_team_k7m_aux_d_k7m_fok_headerIN as h
  join $Node2t_team_k7m_aux_d_k7m_fok_headerIN as e
    on h.divid = e.row_id
 where h.formid=${SparkMainClass.formId}""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_fok_headerOUT")

    logInserted()
    logEnd()
  }

}
