package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class IntIpEgripClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_int_ip_organization_egrip_sblIN = s"${config.stg}.int_ip_organization_egrip"
  val Nodet_team_k7m_aux_d_int_ip_organization_egrip_sblOUT = s"${config.aux}.int_ip_organization_egrip_sbl"


  override val dashboardName: String = Nodet_team_k7m_aux_d_int_ip_organization_egrip_sblOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}int_ip_organization_egrip_sbl"

  def DoIntIpEgrip(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_int_ip_organization_egrip_sblOUT).setLevel(Level.WARN)

    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select distinct a.ip_inn 
    from( select
                 row_number() over (partition by ip_org_id order by effectivefrom desc) as rn,
                 ip_inn,
                 ip_active_flg
            from $Node1t_team_k7m_aux_d_int_ip_organization_egrip_sblIN
           where (effectivefrom < '${dateString}')
             and (effectiveto > '${dateString}')
              ) a
                where a.rn = 1 and a.ip_active_flg = true"""//необязательно, но для совместимости = true
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_int_ip_organization_egrip_sblOUT")
    

    logInserted()
    logEnd()
  }


}
