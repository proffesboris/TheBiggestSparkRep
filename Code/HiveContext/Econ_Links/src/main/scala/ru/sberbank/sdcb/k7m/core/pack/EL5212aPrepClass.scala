package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5212aPrepClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL5212a_prepIN = s"${DevSchema}.k7m_link_for_5212a"
  val Node2t_team_k7m_aux_d_k7m_EL5212a_prepIN = s"${DevSchema}.k7m_z_main_docum"
  val Nodet_team_k7m_aux_d_k7m_EL5212a_prepOUT = s"${DevSchema}.k7m_EL5212a_prep"
  val dashboardPath = s"${config.auxPath}k7m_EL5212a_prep"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL5212a_prepOUT //витрина
  override def processName: String = "EL"

  def DoEL5212aPrep(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL5212a_prepOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      x.cred_id,
      x.issue_dt,
      x.zalog_class_id ,
      x.cred_class_id ,
      x.to_inn  ,
      x.from_inn ,
      x.crit_code,
      x.fact_SUM,
      x.PROV_SUM,
      x.PROV_SUM/x.fact_SUM as quantity
  from (
    select
          a.cred_id,
          a.issue_dt,
          a.zalog_class_id ,
          case when a.zalog_inn=b.prov_dt_inn then '5.2.1.2' else  '5.2.1.4' end crit_code,
          b.prov_dt_inn to_inn,--Важно!  c_kl_dt_2_inn заполняется в 98% случаев если c_kl_dt_0 =1.если, например, C_KL_DT#0=2, то мы должны брать ИНН из поля c_kl_dt_2_inn,
          --а если C_KL_dT#0=1, то необходимо по ссылке     c_acc_dt найти счет ac_fin coalesce(c_client_r,c_client_v), далее найти клиента по счету и получит ИНН из z_clients.
          a.cred_class_id ,
          a.cred_inn from_inn,
          CAST(A.payment_amt AS double) fact_SUM,
          sum(CAST( C_SUM_PO AS double)) AS PROV_SUM
     from $Node1t_team_k7m_aux_d_k7m_EL5212a_prepIN a
     join $Node2t_team_k7m_aux_d_k7m_EL5212a_prepIN b
       on a.acc_id = b.c_acc_kt
    where b.c_date_prov>=unix_timestamp(a.issue_dt)*1000
      and b.c_date_prov <= unix_timestamp(cast(date'${dateString}'as timestamp) )*1000
      --and b.c_kl_kt_0 =1
      and b.prov_kt_inn<>b.prov_dt_inn --плательщик не равен получателю
      and b.prov_kt_inn<>${SparkMainClass.excludeProvKtInn}
      and b.prov_dt_inn<>${SparkMainClass.excludeProvDtInn}
      and substr(b.c_num_dt,1,5) not between ${SparkMainClass.NumDtLow} and ${SparkMainClass.NumDtHigh}
    group by
          a.cred_id,
          a.issue_dt,
          a.zalog_class_id ,
          case when a.zalog_inn=b.prov_dt_inn then '5.2.1.2' else  '5.2.1.4' end,
          b.prov_dt_inn  ,
          a.cred_class_id ,
          a.cred_inn ,
          CAST(A.payment_amt AS double)
       ) x""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL5212a_prepOUT")

    logInserted()
    logEnd()
  }
}
