package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CRMFilialClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_t_crm_filialIN = s"${Stg0Schema}.crm_s_org_ext"
  val Node2t_team_k7m_aux_d_t_crm_filialIN = s"${Stg0Schema}.crm_s_org_ext_x"
  val Nodet_team_k7m_aux_d_t_crm_filialOUT = s"${DevSchema}.t_crm_filial"
  val dashboardPath = s"${config.auxPath}t_crm_filial"


  override val dashboardName: String = Nodet_team_k7m_aux_d_t_crm_filialOUT //витрина
  override def processName: String = "CRM_BASE"

  def DoCRMFilial() {

    Logger.getLogger(Nodet_team_k7m_aux_d_t_crm_filialOUT).setLevel(Level.WARN)

    val Nodet_team_k7m_aux_d_t_crm_filial_Cust = Nodet_team_k7m_aux_d_t_crm_filialOUT.concat("_Cust")
    val Nodet_team_k7m_aux_d_t_crm_filial_Hier5 = Nodet_team_k7m_aux_d_t_crm_filialOUT.concat("_Hier5")
    val Nodet_team_k7m_aux_d_t_crm_filial_Prep = Nodet_team_k7m_aux_d_t_crm_filialOUT.concat("_Prep")
    val Nodet_team_k7m_aux_d_t_crm_filial_Bad = Nodet_team_k7m_aux_d_t_crm_filialOUT.concat("_Bad")

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""SELECT   org.ROW_ID        as id
      , org_x.sbrf_inn    as inn
      , org.PAR_OU_ID     as par_id
      , org.OU_TYPE_CD    as type
  FROM $Node1t_team_k7m_aux_d_t_crm_filialIN org
  join $Node2t_team_k7m_aux_d_t_crm_filialIN  org_x      on org.ROW_ID = org_x.PAR_ROW_ID
  where  org.INT_org_FLG='N'
  and  lower(org.OU_TYPE_CD)<> ${SparkMainClass.OrgOuTypeCd}
  and  lower(org.cust_stat_cd)<> ${SparkMainClass.OrgCustStatCd}"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_Cust"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_t_crm_filial_Cust")

    val createHiveTableStage2 = spark.sql(
      s"""select
        c1.id,
        c1.inn,
        c2.id          c2_id,
        c3.id          c3_id,
        c4.id          c4_id,
        c5.id          c5_id,
        coalesce(c5.id,c4.id,c3.id,c2.id) as major_crm_id
  from $Nodet_team_k7m_aux_d_t_crm_filial_Cust c1
  join $Nodet_team_k7m_aux_d_t_crm_filial_Cust c2      on c1.par_id = c2.id and c1.inn = c2.inn
  left join $Nodet_team_k7m_aux_d_t_crm_filial_Cust c3 on c2.par_id = c3.id and c2.inn = c3.inn
  left join $Nodet_team_k7m_aux_d_t_crm_filial_Cust c4 on c3.par_id = c4.id and c3.inn = c4.inn
  left join $Nodet_team_k7m_aux_d_t_crm_filial_Cust c5 on c4.par_id = c5.id and c4.inn = c5.inn
  where c1.inn<>''"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_Hier5"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_t_crm_filial_Hier5")

    val createHiveTableStage3 = spark.sql(
      s"""select t2.*, count(*) over (partition by inn, concat(least(id, major_crm_id),' ; ', greatest(id,major_crm_id))) cnt
  from (select distinct t.*
          from
          (
            select id, inn,  major_crm_id from $Nodet_team_k7m_aux_d_t_crm_filial_Hier5
          union all
            select c2_id as id, inn,  major_crm_id from $Nodet_team_k7m_aux_d_t_crm_filial_Hier5 where c3_id is not null
          union all
            select c3_id as id, inn,  major_crm_id from $Nodet_team_k7m_aux_d_t_crm_filial_Hier5 where c4_id is not null
          union all
            select c4_id as id, inn,  major_crm_id from $Nodet_team_k7m_aux_d_t_crm_filial_Hier5 where c5_id is not null
          ) t
   where nvl(id,'') <> nvl(major_crm_id,'')
       ) t2"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_Prep"))
      //.partitionBy("cnt") --возможно неожиданное поведение в будущих реализациях SPARK
      .saveAsTable(s"$Nodet_team_k7m_aux_d_t_crm_filial_Prep")

    val createHiveTableStage4 = spark.sql(
      s"""select distinct id, inn, major_crm_id
  from $Nodet_team_k7m_aux_d_t_crm_filial_Prep
 where cnt = 1"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_t_crm_filialOUT")

    logInserted()


    //logDeleted(s"Cleared old $Nodet_team_k7m_aux_d_t_crm_filial_Bad")

    val createHiveTableStage5Bad = spark.sql(
      s"""select distinct id, inn, major_crm_id, cast (current_date as string) dt
  from $Nodet_team_k7m_aux_d_t_crm_filial_Prep
 where cnt > 1"""
    ).persist()//для дальнейшего использования

    val cntBadRecs: Long = createHiveTableStage5Bad.count()//кол-во зацикленных записей

    if (cntBadRecs > 0) {
      createHiveTableStage5Bad.write
        .format("parquet")
        .mode(SaveMode.Append)//можно Overwrite, тогда таблица очистится перед записью
        .option("path", dashboardPath.concat("_Bad"))
        .saveAsTable(s"$Nodet_team_k7m_aux_d_t_crm_filial_Bad")

      logInserted(step = s"Bad records to $Nodet_team_k7m_aux_d_t_crm_filial_Bad", count = cntBadRecs)

    }
    else {
      createHiveTableStage5Bad.unpersist()//не понадобилось-пустой датафрейм свободен
    }
    logEnd()
  }



}
