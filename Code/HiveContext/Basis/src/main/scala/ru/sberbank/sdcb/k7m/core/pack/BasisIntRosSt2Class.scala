package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntRosSt2Class (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_integr_rosstat2IN = s"${DevSchema}.basis_integr_rosstat"
  val Nodet_team_k7m_aux_d_basis_integr_rosstat2OUT = s"${DevSchema}.basis_integr_rosstat2"
  val dashboardPath = s"${config.auxPath}basis_integr_rosstat2"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_integr_rosstat2OUT //витрина
  override def processName: String = "Basis"

  def DoBasisIntRosSt2()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_integr_rosstat2OUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                  select       t1.org_id_egrul,
                               t1.org_id_rt,
                               t1.ul_inn,
                               t1.ul_ogrn,
                               t1.ul_short_nm,
                               t1.ul_full_nm,
                               t1.ul_okopf_cd,
                               t1.ul_reg_first_dt,
                               t1.ul_reg_ogrn_dt,
                               t1.effectivefrom,
                               t1.effectiveto,
                               t1.load_dt,
                               t1.is_active_in_period
                  from (select t.org_id_egrul,
                               t.org_id_rt,
                               t.ul_inn,
                               t.ul_ogrn,
                               t.ul_short_nm,
                               t.ul_full_nm,
                               t.ul_okopf_cd,
                               t.ul_reg_first_dt,
                               t.ul_reg_ogrn_dt,
                               t.effectivefrom,
                               t.effectiveto,
                               t.load_dt,
                               t.is_active_in_period,
                               row_number() over(partition by t.ul_inn order by t.org_id_rt, t.ul_reg_ogrn_dt desc) as rn1
                          from $Node1t_team_k7m_aux_d_basis_integr_rosstat2IN t) t1
                 where t1.rn1 = 1
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_integr_rosstat2OUT")

    logInserted()
    logEnd()
  }

}
