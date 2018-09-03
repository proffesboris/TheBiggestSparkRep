package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ELZMainDocumClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_z_main_documIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Nodet_team_k7m_aux_d_k7m_z_main_documOUT = s"${DevSchema}.k7m_z_main_docum"
  val dashboardPath = s"${config.auxPath}k7m_z_main_docum"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_z_main_documOUT //витрина
  override def processName: String = "EL"

  def DoELZMainDocum(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_z_main_documOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      id,
      unix_timestamp(c_date_prov)*1000 c_date_prov,
      inn_st prov_kt_inn,
      inn_sec prov_dt_inn,
      C_SUM,C_SUM_PO,
      C_SUM_NT,
      c_acc_kt,
      c_nazn,
      c_num_dt,
      c_num_kt
  from $Node1t_team_k7m_aux_d_k7m_z_main_documIN
 where c_date_prov>=cast(add_months(date'${dateString}',-5*3)as timestamp)
 --and c_kl_kt_0 =1
   and ktdt=0 --Клиенты базиса в Кредите
   and inn_st<>inn_sec --плательщик не равен получателю""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_z_main_documOUT")

    logInserted()
    logEnd()
  }
}
