package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogSwitchClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa



  val Node1t_team_k7m_aux_d_k7m_zalog_switchIN = s"${Stg0Schema}.eks_z_property_grp"
  val Node2t_team_k7m_aux_d_k7m_zalog_switchIN = s"${Stg0Schema}.eks_z_properties"
  val Nodet_team_k7m_aux_d_k7m_zalog_switchOUT = s"${DevSchema}.k7m_zalog_switch"
  val dashboardPath = s"${config.auxPath}k7m_zalog_switch"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_switchOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogSwitch(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_switchOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  p.collection_id,
  max(p.c_bool) c_bool --1-баланс, иначе марк
    from
  $Node1t_team_k7m_aux_d_k7m_zalog_switchIN pg
    INNER JOIN $Node2t_team_k7m_aux_d_k7m_zalog_switchIN  p
    ON  p.c_group_prop = pg.id
  AND pg.c_code = ${SparkMainClass.cCode}
  where cast(date'${dateString}'as timestamp) BETWEEN p.C_DATE_BEG AND NVL( p.C_DATE_END, cast(date'4000-01-01' as timestamp) )
  group by p.collection_id
         """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_switchOUT")

    logInserted()
    logEnd()
  }
}
