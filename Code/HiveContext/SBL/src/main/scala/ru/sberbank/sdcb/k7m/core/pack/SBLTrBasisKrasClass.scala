package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBLTrBasisKrasClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_trbasis_krasIN = s"${config.pa}.pdeksin"//s"t_corp_risks.yia_kras_010217"
  val Nodet_team_k7m_aux_d_trbasis_krasOUT = s"${config.aux}.trbasis_kras"


  override val dashboardName: String = Nodet_team_k7m_aux_d_trbasis_krasOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}trbasis_kras"

  def DoSBLTrBasisKras() {

    Logger.getLogger(Nodet_team_k7m_aux_d_trbasis_krasOUT).setLevel(Level.WARN)

    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select f.*,
      case when ((acc_from_name = '') or (acc_from_name is null)) then 0
           when (acc_from_name5 > 0) and (filt_fl <> 1) then 0
           else 1 end as filt
  from (select fl.* ,
               cast(rlike(acc_from_name5,'427[0-9]{2}|40817|40807|40820|40802|407[0-9]{2}|302[0-9]{2}|303[0-9]{2}|423[0-9]{2}|474[0-9]{2}|440[0-9]{2}') as int) as filt_fl
        from (select t.*,
                    substring(acc_from_name, 0, 5) as acc_from_name5
                from (select
                             v.*,
                             regexp_extract(c_nazn, '[1-9]{1}[0-9]{4}[0-9A-Z]{1}[0-9]{14}[0-9]*', 0) as acc_from_name
                        from $Node1t_team_k7m_aux_d_trbasis_krasIN v
                      ) t
              )fl
        ) f"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_trbasis_krasOUT")

    logInserted()
    logEnd()
  }

}
