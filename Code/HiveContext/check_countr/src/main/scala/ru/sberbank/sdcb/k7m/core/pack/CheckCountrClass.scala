package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CheckCountrClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_check_countrIN = s"${DevSchema}.countr_stg1"
  val Node2t_team_k7m_aux_d_check_countrIN = s"${DevSchema}.countr_stg5"
  val Node3t_team_k7m_aux_d_check_countrIN = s"${DevSchema}.basis_client"
   val Nodet_team_k7m_aux_d_check_countrOUT = s"${DevSchema}.check_countr"
  val dashboardPath = s"${config.auxPath}check_countr"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_check_countrOUT //витрина
  override def processName: String = "check_countr"

  def DoCheckCountr() {

    Logger.getLogger(Nodet_team_k7m_aux_d_check_countrOUT).setLevel(Level.WARN)

     val createHiveTableStage1 = spark.sql(
      s"""select crm_id, flag_countr from $Node1t_team_k7m_aux_d_check_countrIN
              union all
          select
           b.org_crm_id as crm_id,
           case when (t.reg_country = 'RUS' or t.reg_country is null) then 0 else 1 end as flag_countr
          from $Node2t_team_k7m_aux_d_check_countrIN t
          join $Node3t_team_k7m_aux_d_check_countrIN b on  b.org_inn_crm_num = t.inn_to
          where t.rn =1
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_check_countrOUT")

    logInserted()

  }
}


