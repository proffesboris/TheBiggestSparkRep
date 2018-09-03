package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntPrvClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_integr_pravoIN = s"${Stg0Schema}.prv_organization"
  val Node2t_team_k7m_aux_d_basis_integr_pravoIN = s"${Stg0Schema}.prv_ref_opf"
  val Node3t_team_k7m_aux_d_basis_integr_pravoIN = s"${DevSchema}.basis_client_crm"
  val Nodet_team_k7m_aux_d_basis_integr_pravoOUT = s"${DevSchema}.basis_integr_pravo"
  val dashboardPath = s"${config.auxPath}basis_integr_pravo"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_integr_pravoOUT //витрина
  override def processName: String = "Basis"

  def DoBasisIntPrv()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_integr_pravoOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                   select t1.org_inn,
                          t1.org_kpp,
                          t1.org_ogrn,
                          t1.org_nm,
                          t1.opf_id,
                          cast(t1.org_reg_dt as string) as org_reg_dt,
                          r.opf_nm
                     from (select t.org_inn, --ИНН организации
                                  t.org_kpp, --КПП организации
                                  t.org_ogrn, -- ОГРН организации
                                  t.org_nm, -- Наименование организации
                                  t.opf_id, -- FK на cb_akm_pravo.ref_opf. Ссылка на ОПФ
                                  cast(t.org_reg_dt as date) as org_reg_dt, --Дата регистрации организации
                                  row_number() over(partition by t.org_inn order by t.effectiveto desc) as rn
                             from $Node1t_team_k7m_aux_d_basis_integr_pravoIN t
                            where year(cast(effectiveto as date)) = 2999) t1
                     join (select r1.file_dt,
                                  r1.load_dt,
                                  r1.opf_id,
                                  r1.opf_nm,
                                  r1.effectivefrom
                             from (select o.file_dt,
                                          o.load_dt,
                                          o.opf_id,
                                          o.opf_nm,
                                          o.effectivefrom,
                                row_number() over(partition by o.opf_id order by o.effectivefrom desc) as rn
                           from $Node2t_team_k7m_aux_d_basis_integr_pravoIN o) r1
                            where r1.rn = 1) r
                       on r.opf_id = t1.opf_id
                    where t1.rn = 1
                      and t1.org_inn in
                          (select org_inn from $Node3t_team_k7m_aux_d_basis_integr_pravoIN)
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_integr_pravoOUT")

    logInserted()
    logEnd()
  }

}
