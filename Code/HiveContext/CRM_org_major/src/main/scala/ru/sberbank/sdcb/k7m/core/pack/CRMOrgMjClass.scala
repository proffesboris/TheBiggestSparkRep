package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CRMOrgMjClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  
  val Node1t_team_k7m_aux_d_k7m_crm_org_mjIN = s"${DevSchema}.k7m_crm_org"
  val Node2t_team_k7m_aux_d_k7m_crm_org_mjIN = s"${Stg0Schema}.crm_s_org_ext2_fnx"
  val Nodet_team_k7m_aux_d_k7m_crm_org_mjOUT = s"${DevSchema}.k7m_crm_org_mj"
  val dashboardPath = s"${config.auxPath}k7m_crm_org_mj"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_crm_org_mjOUT //витрина
  override def processName: String = "CRM_ORG_MAJOR"

  def DoOrgMj() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_crm_org_mjOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""  select id, inn, full_name,type_cd,attrib_04, rn_inn from
      (
        select
          c.*,f.attrib_04,
    row_number() over(
      partition by
        inn
        order by
    case
    --when type_cd = 'Холдинг' then 1
    when type_cd = 'Юр. лицо' then 2
    else 3 end,

    case
      when attrib_04 = 'Активный клиент' then 1
    when attrib_04 = 'Спящий клиент' then 2
    else 3 end		-- сначала записи в активном статусе
    )  rn_inn
    from (select
      distinct id,inn,full_name,type_cd
      from $Node1t_team_k7m_aux_d_k7m_crm_org_mjIN c
      where 1=1
    and length(inn)=10
    and inn = regexp_replace(inn,'[^0-9]+','')
    and inn <> ${SparkCRMmajor.excludeInn}
    and type_cd not in (${SparkCRMmajor.typeCd})
    ) c
    left join $Node2t_team_k7m_aux_d_k7m_crm_org_mjIN f on c.id = f.par_row_id) t"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_crm_org_mjOUT")

    logInserted()
    logEnd()
  }


}
