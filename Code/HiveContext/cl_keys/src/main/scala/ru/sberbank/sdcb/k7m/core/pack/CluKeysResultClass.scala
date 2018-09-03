package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.lang.System


class CluKeysResultClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_resultIN = s"${DevSchema}.clu_keys_new"
    val Node2t_team_k7m_aux_d_clu_keys_resultIN = s"${DevSchema}.clu_keys"
    val Nodet_team_k7m_aux_d_clu_keys_resultOUT = s"${DevSchema}.clu_keys_result"
    val dashboardPath = s"${config.auxPath}clu_keys_result"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_resultOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysResult() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_resultOUT).setLevel(Level.WARN)

      logStart()


      val Nodet_team_k7m_aux_d_clu_keys_resultClose_1 = s"${DevSchema}.clu_keys_new_close_1"
      val dashboardPathClose_1 = s"${config.auxPath}clu_keys_new_close_1"
      val Nodet_team_k7m_aux_d_clu_keys_resultClose_2 = s"${DevSchema}.clu_keys_new_close_2"
      val dashboardPathClose_2 = s"${config.auxPath}clu_keys_new_close_2"

      val createHiveTableStage1 = spark.sql(
    s"""select  k.u7m_id,
        k.crm_id,
        k.inn,
        2 priority,
        n.u7m_id closed_and_merged_to_u7m_id ,
        n.inn as actual_inn,
        k.u7m_from_crm_flag
 from $Node1t_team_k7m_aux_d_clu_keys_resultIN n
   join $Node2t_team_k7m_aux_d_clu_keys_resultIN k
   on n.crm_id = k.crm_id
where k.closed_and_merged_to_u7m_id is null
    and (n.inn <> k.inn
        or n.inn is null and k.inn is not null
        or n.u7m_id <>k.u7m_id and  n.inn is not null and k.inn is  null
        )
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPathClose_1)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_resultClose_1")

      val createHiveTableStage2 = spark.sql(
    s"""select  k.u7m_id,
        k.crm_id,
        k.inn,
        3 priority,
        n.u7m_id closed_and_merged_to_u7m_id,
        n.crm_id actual_crm_id,
        k.u7m_from_crm_flag
 from $Node1t_team_k7m_aux_d_clu_keys_resultIN n
   join $Node2t_team_k7m_aux_d_clu_keys_resultIN k
   on n.inn = k.inn
where k.closed_and_merged_to_u7m_id is null
    and ( n.crm_id<>k.crm_id
        or n.crm_id is null and k.crm_id is not null
        or n.u7m_id <>k.u7m_id and  n.crm_id is not null and k.crm_id is  null
        )
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPathClose_2)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_resultClose_2")

      val createHiveTableStage3 = spark.sql(
    s"""select
        u7m_id,
        crm_id,
        inn,
        closed_and_merged_to_u7m_id,
        coalesce(create_exec_id,$execId)  create_exec_id,
        status,
        u7m_from_crm_flag
 from (
    select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        create_exec_id,
        status,
        row_number() over (partition by u7m_id, closed_and_merged_to_u7m_id order by priority ) rn,
        u7m_from_crm_flag
    from (
     select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        create_exec_id,
        'ACTIVE' status,
        u7m_from_crm_flag
     from $Node1t_team_k7m_aux_d_clu_keys_resultIN
     union all
     select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        cast(null as string) as create_exec_id,
        'INACTIVE' status,
        u7m_from_crm_flag
     from  $Nodet_team_k7m_aux_d_clu_keys_resultClose_1
     union all
     select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        cast(null as string) as create_exec_id,
        'INACTIVE' status,
        u7m_from_crm_flag
     from  $Nodet_team_k7m_aux_d_clu_keys_resultClose_2
     union all
     select
        b.u7m_id,
        b.crm_id,
        b.inn,
        99 priority,
        b.closed_and_merged_to_u7m_id,
        b.create_exec_id,
        'INACTIVE' status,
        b.u7m_from_crm_flag
     from  $Node2t_team_k7m_aux_d_clu_keys_resultIN b
        left join $Nodet_team_k7m_aux_d_clu_keys_resultClose_1 c1
        on c1.u7m_id = b.u7m_id
        left join $Nodet_team_k7m_aux_d_clu_keys_resultClose_2 c2
        on c2.u7m_id = b.u7m_id
     where
        c1.u7m_id is null
        and c2.u7m_id is null
    ) act
    ) reslt
    where rn=1
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_resultOUT")

      logInserted()

      //---------------------------------------------------------------------
      //--Постусловия на ключи:
      //-- Если один  6-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
      //---------------------------------------------------------------------

      //1.
      val checkStage1 = spark.sql(s"""
    select
 u7m_id,closed_and_merged_to_u7m_id,
 cast(count(*) as string) cnt
 from $Nodet_team_k7m_aux_d_clu_keys_resultOUT
group by u7m_id,closed_and_merged_to_u7m_id
having count(*)>1""").collect().toSeq

      val log1 = checkStage1.foreach(c=>{
        log("clu_keys","U7M_ID. "+c.getString(0)+" " + c.getString(1)+ ": " +c.getString(2), CustomLogStatus.ERROR)
          })

      //2.
      val checkStage2 = spark.sql(s"""
   select
 inn,
 cast(count(*) as string) cnt
 from  $Nodet_team_k7m_aux_d_clu_keys_resultOUT
where closed_and_merged_to_u7m_id is  null
    and inn is not null
group by inn
having count(*)>1""").collect().toSeq

      val log2 = checkStage2.foreach(c=> {
        log("clu_keys", "INN. " + c.getString(0) + ": " + c.getString(1), CustomLogStatus.ERROR)
      })

      //3.
      val checkStage3 = spark.sql(s"""
  select
 crm_id,
 cast(count(*) as string) cnt
 from  $Nodet_team_k7m_aux_d_clu_keys_resultOUT
where closed_and_merged_to_u7m_id is  null
    and crm_id is not null
group by crm_id
having count(*)>1""").collect().toSeq

      val log3 = checkStage3.foreach(c=>{
        log("clu_keys","CRM_ID. "+c.getString(0)+": " +c.getString(1), CustomLogStatus.ERROR)
          })

      //4.
      val nullsCnt = spark.sql(s"""
  select cast(count(*) as string) cnt
from $Nodet_team_k7m_aux_d_clu_keys_resultOUT
where u7m_id is null or inn is null and crm_id is null """).first().getString(0)

      if (nullsCnt != "0") {
        log("clu_keys","NULLS: "+nullsCnt, CustomLogStatus.ERROR)
        }

      //5.
      val checkStage5Cnt = spark.sql(s"""
select cast(count(*) as string) cnt
  from $DevSchema.clu_keys_raw s
  left join $Nodet_team_k7m_aux_d_clu_keys_resultOUT k
    on s.crm_id = k.crm_id
        and k.closed_and_merged_to_u7m_id is null
        and k.status = 'ACTIVE'
 where k.u7m_id is null
   and s.crm_id is not null""").first().getString(0)

      if (checkStage5Cnt != "0"){
        log("clu_keys","NULLS Active CRM_ID: "+checkStage5Cnt, CustomLogStatus.ERROR)
        }

      //6.
      val checkStage6Cnt = spark.sql(s"""
select cast(count(*) as string) cnt
  from $DevSchema.clu_keys_raw s
  left join $Nodet_team_k7m_aux_d_clu_keys_resultOUT k
    on s.inn = k.inn
       and k.closed_and_merged_to_u7m_id is   null
       and k.status = 'ACTIVE'
 where k.u7m_id is null
   and s.inn is not null""").first().getString(0)

      if (checkStage6Cnt != "0"){
        log("clu_keys","NULLS Active INN: "+checkStage6Cnt, CustomLogStatus.ERROR)
        }

      val flagEx: Boolean = !checkStage1.isEmpty || !checkStage2.isEmpty || !checkStage3.isEmpty || !(nullsCnt=="0") || !(checkStage5Cnt=="0")|| !(checkStage6Cnt=="0")

      if (flagEx) throw new IllegalArgumentException
      (s"${if (!checkStage1.isEmpty) "Дубли u7m_id."} ${if (!checkStage2.isEmpty) "Дубли inn."} ${if (!checkStage3.isEmpty) "Дубли crm_id."} ${if (!(nullsCnt=="0")) s"Пустые значения: $nullsCnt ."} ${if (!(checkStage5Cnt=="0")) s"INN есть, u7m_id пустой: $checkStage5Cnt ."} ${if (!(checkStage6Cnt=="0")) s"crm_id есть, u7m_id пустой: $checkStage6Cnt ."}")

      val createBkp = spark.sql(s"""select * from $Node2t_team_k7m_aux_d_clu_keys_resultIN""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${config.auxPath}clu_keys_backup")
        .saveAsTable(s"${Node2t_team_k7m_aux_d_clu_keys_resultIN}_backup")

      val renameKeys = spark.sql(s"""select * from $Nodet_team_k7m_aux_d_clu_keys_resultOUT""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${config.auxPath}clu_keys")
        .saveAsTable(s"${Node2t_team_k7m_aux_d_clu_keys_resultIN}")


      logEnd()
    }
}

