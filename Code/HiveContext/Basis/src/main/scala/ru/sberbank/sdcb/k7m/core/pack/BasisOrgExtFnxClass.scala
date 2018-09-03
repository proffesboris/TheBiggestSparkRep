package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisOrgExtFnxClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //------------------CRM-----------------------------
  val Node1t_team_k7m_aux_d_basis_org_ext2_fnxIN = s"${Stg0Schema}.crm_s_org_ext2_fnx"
  val Node2t_team_k7m_aux_d_basis_org_ext2_fnxIN = s"${DevSchema}.basis_org_ext"
   val Nodet_team_k7m_aux_d_basis_org_ext2_fnxOUT = s"${DevSchema}.basis_org_ext2_fnx"
  val dashboardPath = s"${config.auxPath}basis_org_ext2_fnx"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_org_ext2_fnxOUT //витрина
  override def processName: String = "Basis"

  def DoBasisOrgExtFnx()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_org_ext2_fnxOUT).setLevel(Level.WARN)

    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
          select                     t1.row_id                                       as org_fnx_id,     --уникальный id в s_org_ext2_fnx
                                     t1.par_row_id                                   as par_row_id_fnx, --FK на s_org_ext.row_id
                                     t1.attrib_03                                    as org_segment,    --Крупнейшие, Крупные, Средние , Малые, Микро
                                     t1.attrib_04                                    as org_ispartner,  --Тип сотрудничества. Варианты: Клиент, Не клиент
                                     t1.attrib_06                                    as org_subsegment, --Возможные значения: Гос.органы, Фин.институт
                                     t1.attrib_07                                    as org_priority   --Возможные значения: A, B, C, D. Заполняется посредством механизма импорта данных по приоритезации
                             from $Node1t_team_k7m_aux_d_basis_org_ext2_fnxIN t1
                   inner join $Node2t_team_k7m_aux_d_basis_org_ext2_fnxIN o on t1.par_row_id = o.org_id
      """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_org_ext2_fnxOUT")

    logInserted()
    logEnd()
  }
}
