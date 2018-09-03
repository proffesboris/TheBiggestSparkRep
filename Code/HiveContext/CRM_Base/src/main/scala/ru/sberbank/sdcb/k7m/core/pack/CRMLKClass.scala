package ru.sberbank.sdcb.k7m.core.pack

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CRMLKClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_CRMLKIN = s"${Stg0Schema}.crm_cx_gsz"
  val Node2t_team_k7m_aux_d_K7M_CRMLKIN = s"${Stg0Schema}.crm_CX_PARTY_GSZ_X"
  val Node3t_team_k7m_aux_d_K7M_CRMLKIN = s"${DevSchema}.K7M_crm_org"
  val Node4t_team_k7m_aux_d_K7M_CRMLKIN = s"${Stg0Schema}.crm_CX_GSZMEM_INTER"
  val Node5t_team_k7m_aux_d_K7M_CRMLKIN = s"${Stg0Schema}.crm_CX_GSZ_CRITER"
  val Nodet_team_k7m_aux_d_K7M_CRMLKOUT = s"${DevSchema}.K7M_CRMLK"
  val dashboardPath = s"${config.auxPath}K7M_CRMLK"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRMLKOUT //витрина
  override def processName: String = "CRM_BASE"

  def DoCRMLK() {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CRMLKOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select row_number() over (order by ID_FROM, ID_TO) loc_id,
     r.CRIT_ID,
     r.ID_FROM,
     r.name_FROM,
     r.inn_FROM,
     r.kio_FROM,
     r.ID_TO,
     r.name_TO,
     r.inn_TO,
     r.kio_TO,
     r.gsz_id,
     r.gsz_name
from (select distinct
         t.CRIT_ID,
         t.ID_FROM,
         t.name_FROM,
         t.inn_FROM,
         t.kio_FROM,
         t.ID_TO,
         t.name_TO,
         t.inn_TO,
         t.kio_TO,
         t.gsz_id,
         t.gsz_name
          from
         ( select	 
                  I3.CRIT_ID
                 , org2.id        as ID_FROM
                 , org2.full_name as name_FROM
                 , org2.inn       as inn_FROM
                 , org2.kio       as kio_FROM
                 , org.id         as ID_TO
                 , org.full_name  as name_TO
                 , org.inn        as inn_TO
                 , org.kio        as kio_TO
                 , gsz.row_id     as gsz_id
                 , gsz.name       as gsz_name
             from $Node1t_team_k7m_aux_d_K7M_CRMLKIN gsz
             join $Node2t_team_k7m_aux_d_K7M_CRMLKIN gsz_x on (gsz_x.gsz_id = gsz.row_id)
             join $Node3t_team_k7m_aux_d_K7M_CRMLKIN org on (org.id = gsz_x.row_id)
             join (
                  select
                         ROW_ID
                        , PARTY_ID
                        , LINK_ID
                    from $Node4t_team_k7m_aux_d_K7M_CRMLKIN
                  ) i
               on (i.PARTY_ID = org.id)
             join (
                  select
                         ROW_ID
                        , PARTY_ID
                        , LINK_ID
                  from $Node4t_team_k7m_aux_d_K7M_CRMLKIN
                ) i2
               on (i2.LINK_ID = i.LINK_ID)
             join $Node3t_team_k7m_aux_d_K7M_CRMLKIN org2 on (org2.id = i2.PARTY_ID)
             join $Node5t_team_k7m_aux_d_K7M_CRMLKIN i3 on (i3.LINK_ID = i2.LINK_ID)
            where  (i2.PARTY_ID <> org.id)
          ) t) r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRMLKOUT")

    logInserted()
    logEnd()
  }


}
