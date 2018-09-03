package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL15Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_zalogiIN = s"${config.stg}.eks_z_zalog_body"
  val Node2t_team_k7m_aux_d_crit_zalogiIN = s"${config.stg}.eks_z_zalog"
  val Node3t_team_k7m_aux_d_crit_zalogiIN = s"${config.stg}.eks_z_client"
  val Node4t_team_k7m_aux_d_crit_zalogiIN = s"${config.aux}.basis_client"
  val Nodet_team_k7m_aux_d_crit_zalogiOUT = s"${config.aux}.crit_zalogi"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_zalogiOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_zalogi"

  def DoSBL15CritZalogi(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_zalogiOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_zalogiBodyEq = Nodet_team_k7m_aux_d_crit_zalogiOUT.concat("_BodyEq")
    val Nodet_team_k7m_aux_d_crit_zalogiClientsEq = Nodet_team_k7m_aux_d_crit_zalogiOUT.concat("_ClientsEq")
    val Nodet_team_k7m_aux_d_crit_zalogiPrep = Nodet_team_k7m_aux_d_crit_zalogiOUT.concat("_Prep")

    val createHiveTableStage1 = spark.sql(
      s"""  select distinct
            a.collection_id as left_zalog_body_collection_id,
            b.collection_id as right_zalog_body_collection_id,
            lower(trim(a.c_storage)) as c_storage
        from (select * from $Node1t_team_k7m_aux_d_crit_zalogiIN
               where lower(trim(c_storage)) not in (${SparkMain.excludeStorage})) a
        join (select * from $Node1t_team_k7m_aux_d_crit_zalogiIN
               where c_storage is not null) b
          on lower(trim(a.c_storage)) = lower(trim(b.c_storage))
       where a.id <> b.id"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_BodyEq").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_zalogiBodyEq")

    val createHiveTableStage2 = spark.sql(
      s"""  select distinct
                 left1.c_user_zalog as left_client,
                 right1.c_user_zalog as right_client,
                 z.c_storage
            from $Nodet_team_k7m_aux_d_crit_zalogiBodyEq z
       left join $Node2t_team_k7m_aux_d_crit_zalogiIN left1
              on z.left_zalog_body_collection_id = left1.C_VALUES
       left join $Node2t_team_k7m_aux_d_crit_zalogiIN right1
              on z.right_zalog_body_collection_id = right1.C_VALUES """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_ClientsEq").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_zalogiClientsEq")


    val createHiveTableStage3 = spark.sql(
      s"""    select
              left1.c_inn as left_inn,
              right1.c_inn as right_inn,
              z.c_storage
         from (select * from $Nodet_team_k7m_aux_d_crit_zalogiClientsEq
                where c_storage is not null
                  and c_storage not in (${SparkMain.excludeStoragedsf})) z
    left join (select * from $Node3t_team_k7m_aux_d_crit_zalogiIN
                where c_inn is not null
                  and c_inn not in (${SparkMain.excludeInn})
              ) left1
           on z.left_client = left1.id
    left join (select * from $Node3t_team_k7m_aux_d_crit_zalogiIN
                where c_inn is not null
                  and c_inn not in (${SparkMain.excludeInn})
              ) right1
           on z.right_client = right1.id
    left semi join $Node4t_team_k7m_aux_d_crit_zalogiIN f
           on case when left1.c_inn = f.org_inn_crm_num then 1
                   when right1.c_inn = f.org_inn_crm_num then 1
                   else 0 end = 1

    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_zalogiPrep")



    val createHiveTableStage4 = spark.sql(
      s""" select
             a.left_inn as inn1,
             a.right_inn as inn2,
             '${dateString}' as dt,
             min(b.quantity) as quantity
         from $Nodet_team_k7m_aux_d_crit_zalogiPrep a
    left join (select c_storage, count(c_storage) as quantity
                  from $Nodet_team_k7m_aux_d_crit_zalogiPrep
                 where (left_inn is not null) and (right_inn is not null) and (left_inn <> right_inn)
                 group by c_storage
              ) b
           on a.c_storage = b.c_storage
        where a.left_inn is not null
          and a.right_inn is not null
          and a.left_inn <> a.right_inn
          and left_inn <> '${SparkMain.innSber}'
          and right_inn <> '${SparkMain.innSber}'
       group by a.left_inn, a.right_inn"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_zalogiOUT")

    logInserted()
    logEnd()
  }


}
