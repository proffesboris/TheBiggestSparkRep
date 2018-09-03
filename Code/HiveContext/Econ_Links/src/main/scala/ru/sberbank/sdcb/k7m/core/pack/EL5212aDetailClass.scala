package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5212aDetailClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL5212a_detailIN = s"${DevSchema}.k7m_link_for_5212a"
  val Node2t_team_k7m_aux_d_k7m_EL5212a_detailIN = s"${DevSchema}.k7m_z_main_docum"
  val Nodet_team_k7m_aux_d_k7m_EL5212a_detailOUT = s"${DevSchema}.k7m_EL5212a_detail"
  val dashboardPath = s"${config.auxPath}k7m_EL5212a_detail"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL5212a_detailOUT //витрина
  override def processName: String = "EL"

  def DoEL5212aDetail(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL5212a_detailOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      a.cred_id,
      a.issue_dt,
      b.id prov_id,
      b.c_acc_kt ,
      a.zalog_class_id ,
      b.prov_kt_inn,
      b.prov_dt_inn,--Важно!  Берем исправленный ИНН алгоритм:
        --c_kl_dt_2_inn заполняется в 98% случаев если c_kl_dt_0 =1.если, например, C_KL_DT#0=2, то мы должны брать ИНН из поля c_kl_dt_2_inn,
      --а если C_KL_dT#0=1, то необходимо по ссылке     c_acc_dt найти счет ac_fin coalesce(c_client_r,c_client_v), далее найти клиента по счету и получит ИНН из z_clients.
      ---Часто проставляют ИНН Сбербанка если C_KL_dT#0=1. Хотя счет клиентский
        a.cred_class_id ,
      a.cred_inn,
      a.zalog_inn,
      A.payment_amt  fact_SUM,
      C_SUM_PO AS PROV_SUM,
      b.c_nazn
  from $Node1t_team_k7m_aux_d_k7m_EL5212a_detailIN a
  join $Node2t_team_k7m_aux_d_k7m_EL5212a_detailIN b ---Вернуть core_internal_eks.z_main_docum
    on a.acc_id = b.c_acc_kt
 where b.c_date_prov>=unix_timestamp(a.issue_dt)*1000
   and b.c_date_prov<= unix_timestamp(cast(date'${dateString}'as timestamp))*1000
   --and b.c_kl_kt_0 =1
   and b.prov_kt_inn<>b.prov_dt_inn --плательщик не равен получателю
  """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL5212a_detailOUT")

    logInserted()
    logEnd()
  }

}
