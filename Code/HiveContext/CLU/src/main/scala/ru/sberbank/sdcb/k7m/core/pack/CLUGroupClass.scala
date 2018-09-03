package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CLUGroupClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Node1t_team_k7m_aux_d_stg_GROUP_GSZIN = s"${config.stg}.crm_cx_gsz"
  val Node2t_team_k7m_aux_d_stg_GROUP_GSZIN = s"${config.stg}.crm_CX_PARTY_GSZ_X"
  val Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT = s"${config.aux}.k7m_GROUP_GSZ"


  override val dashboardName: String = Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT //витрина
  override def processName: String = "CLU"

  val dashboardPath = s"${config.auxPath}k7m_GROUP_GSZ"

  def DoCLUGroup() {

    Logger.getLogger(Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT).setLevel(Level.WARN)



    val Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep = Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT.concat("_Prep")
    val Nodet_team_k7m_aux_d_stg_GROUP_GSZHier = Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT.concat("_Hier")
    val Nodet_team_k7m_aux_d_stg_GROUP_GSZBad = Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT.concat("_Bad")

    val createHiveTableStage1 = spark.sql(
      s"""SELECT
      gsz_x.row_id         as org_crm_id
    , gsz.row_id           as id
    , gsz.parent_gsz_id    as par_id
    , gsz.top_gsz_id       as top_id
    , gsz.name             as GSZ_name
  FROM  $Node2t_team_k7m_aux_d_stg_GROUP_GSZIN gsz_x
  JOIN  $Node1t_team_k7m_aux_d_stg_GROUP_GSZIN gsz  on (gsz_x.gsz_id = gsz.row_id)
 WHERE nvl(gsz.status,'') not in ('Закрыта')"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep")



    val createHiveTableStage2 = spark.sql(
      s"""
select
c1.org_crm_id,
c1.id,
c1.top_id,
coalesce(c4.id,c3.id,c2.id,c1.id) as major_id,
c1.GSZ_name as GSZ_name,
 c2.id          c2_id,
c2.GSZ_name as GSZ_name_2,
 c3.id          c3_id,
c3.GSZ_name as GSZ_name_3,
 c4.id          c4_id,
c4.GSZ_name as GSZ_name_4

FROM  $Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep c1
left JOIN  $Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep c2 on c1.par_id = c2.id
left JOIN  $Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep c3 on c2.par_id = c3.id
left JOIN  $Nodet_team_k7m_aux_d_stg_GROUP_GSZPrep c4 on c3.par_id = c4.id
"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Hier")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_stg_GROUP_GSZHier")

    val createHiveTableStage3 = spark.sql(
      s"""select distinct h.*
    from $Nodet_team_k7m_aux_d_stg_GROUP_GSZHier h
   where nvl(top_id,'') = nvl(major_id,'')
"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_stg_GROUP_GSZOUT")

    logInserted()

    val createHiveTableStage4Bad = spark.sql(
      s"""select distinct h.*
    from $Nodet_team_k7m_aux_d_stg_GROUP_GSZHier h
   where nvl(top_id,'') <> nvl(major_id,'')"""
    ).persist()//для дальнейшего использования

    val cntBadRecs: Long = createHiveTableStage4Bad.count()//кол-во зацикленных записей

    if (cntBadRecs > 0) {
      createHiveTableStage4Bad.write
        .format("parquet")
        .mode(SaveMode.Append)//можно Overwrite, тогда таблица очистится перед записью
        .option("path", s"${dashboardPath}_Bad")
        .saveAsTable(s"$Nodet_team_k7m_aux_d_stg_GROUP_GSZBad")

       logInserted(step = s"Bad records to $Nodet_team_k7m_aux_d_stg_GROUP_GSZBad", count = cntBadRecs)

    }
    else {
      createHiveTableStage4Bad.unpersist() //не понадобилось-пустой датафрейм свободен
    }

  }


}
