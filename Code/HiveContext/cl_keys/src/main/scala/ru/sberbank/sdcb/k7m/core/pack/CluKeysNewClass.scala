package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluKeysNewClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_newIN = s"${DevSchema}.clu_keys_active"
    val Node2t_team_k7m_aux_d_clu_keys_newIN = s"${DevSchema}.clu_keys"
    val Node3t_team_k7m_aux_d_clu_keys_newIN = s"${DevSchema}.k7m_crm_org_major"
    val Node4t_team_k7m_aux_d_clu_keys_newIN = s"${DevSchema}.basis_client"
    val Nodet_team_k7m_aux_d_clu_keys_newOUT = s"${DevSchema}.clu_keys_new"
    val dashboardPath = s"${config.auxPath}clu_keys_new"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_newOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysNew() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_newOUT).setLevel(Level.WARN)

      logStart()

      val Nodet_team_k7m_aux_d_clu_keys_newMain = s"${DevSchema}.clu_keys"
      val dashboardPathMain = s"${config.auxPath}clu_keys"

      val createHiveTableStage0 = spark.sql(
        s"""select
      cast(null as string) as u7m_id,
      cast(null as string) as crm_id,
      cast(null as string) as inn,
      cast(null as string) as closed_and_merged_to_u7m_id,
      cast(null as string) as create_exec_id,
      cast(null as string) as status,
      cast(null as string) u7m_from_crm_flag
  from $Node1t_team_k7m_aux_d_clu_keys_newIN
 where 1=0
           """).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathMain)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_newMain")

      val checkKeysCnt = spark.table(Nodet_team_k7m_aux_d_clu_keys_newMain).count()
      if (checkKeysCnt == 0) {
      val createHiveTableStage0_5 = spark.sql(
        s"""select
     id as u7m_id,
     id as crm_id,
     inn,
     cast(null as string) as closed_and_merged_to_u7m_id,
     'INI' as create_exec_id,
     'INACTIVE' as  status,
     'Y' as u7m_from_crm_flag
from (
    select
        id,
        inn,
        row_number() over (partition by inn order by s ) rn
    from (
        --Приоритет базису при определении crm_id для ИНН
        select
            id as id,
            inn,
            2 s
        from $Node3t_team_k7m_aux_d_clu_keys_newIN
        union all
        select
            org_crm_id as id,
            org_inn_crm_num as inn,
            1 s
        from $Node4t_team_k7m_aux_d_clu_keys_newIN
        ) x
        ) y
    where y.rn = 1""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPathMain)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_newMain")}

      //---------------------------------------------------------------------
      //--Постусловия на ключи:
      //-- Если один  6-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
      //---------------------------------------------------------------------

      //1.
      val checkStage1 = spark.sql(s"""
    select
 u7m_id,closed_and_merged_to_u7m_id,
 cast(count(*) as string) cnt
 from $Nodet_team_k7m_aux_d_clu_keys_newMain
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
 from  $Nodet_team_k7m_aux_d_clu_keys_newMain
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
 from  $Nodet_team_k7m_aux_d_clu_keys_newMain
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
from $Nodet_team_k7m_aux_d_clu_keys_newMain
where u7m_id is null or inn is null and crm_id is null """).first().getString(0)

      if (nullsCnt != "0") {
        log("clu_keys","NULLS: "+nullsCnt, CustomLogStatus.ERROR)
        }

      val flagEx: Boolean = !checkStage1.isEmpty || !checkStage2.isEmpty || !checkStage3.isEmpty || !(nullsCnt=="0")


      if (flagEx) throw new IllegalArgumentException
         (s"${if (!checkStage1.isEmpty) "Дубли u7m_id."} ${if (!checkStage2.isEmpty) "Дубли inn."} ${if (!checkStage3.isEmpty) "Дубли crm_id."} ${if (!(nullsCnt=="0")) "Пустые значения."}")

      val startId = spark.sql(s"""select cast(coalesce(max(cast(u7m_id as bigint)),0) as string) s from $Nodet_team_k7m_aux_d_clu_keys_newMain""").first().getString(0)

      val createHiveTableStage1 = spark.sql(
    s"""select
    COALESCE(x.u7m_id,x.crm_id,cast((nvl($startId,0)+RN) as string)) u7m_id,
    x.crm_id,
    x.inn,
    1 priority,
    cast (null as string)  closed_and_merged_to_u7m_id,
    create_exec_id,
    k1_u7m_id,
    k1_inn,
    k1_crm_ID,
    k2_u7m_id,
    k2_inn,
    k2_crm_ID,
    case
       when x.u7m_id is not null then x.u7m_from_crm_flag
       when x.crm_id is not null then 'Y'
       else 'N'
    end    u7m_from_crm_flag,
    sel_type
 FROM (
 SELECT
    --ВНИМАНИЕ ПРИ ИЗМЕНЕНИИ CASE нужно его дублировать 3 раза!
    ROW_NUMBER() OVER (PARTITION BY   CASE
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.U7M_ID
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.U7M_ID
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.U7M_ID
        ELSE CAST(NULL AS string) END ORDER BY A.CRM_ID ) RN,
   CASE
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.U7M_ID
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.U7M_ID
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.U7M_ID
        ELSE CAST(NULL AS string) END
        U7M_ID,
        CASE
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.create_exec_id
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.create_exec_id
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.create_exec_id
        ELSE CAST(NULL AS string) END create_exec_id,
        a.crm_id,
        a.INN ,
        k1.u7m_id k1_u7m_id,
        k1.inn  k1_inn,
        k1.crm_ID  k1_crm_ID,
        k2.u7m_id k2_u7m_id,
        k2.inn  k2_inn,
        k2.crm_ID  k2_crm_ID,
        CASE
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS STRING)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS STRING)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.u7m_from_crm_flag
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.u7m_from_crm_flag
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.u7m_from_crm_flag
        ELSE CAST(NULL AS STRING) END u7m_from_crm_flag,
        CASE
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then '1'
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then '2'
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  '3'
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  '4'
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  '5'
        ELSE CAST(NULL AS string) END sel_type
 from
    $Node1t_team_k7m_aux_d_clu_keys_newIN a
    left join $Node2t_team_k7m_aux_d_clu_keys_newIN k1
    on k1.crm_id = a.crm_id
        AND k1.closed_and_merged_to_u7m_id is null
    left join $Node2t_team_k7m_aux_d_clu_keys_newIN k2
    on k2.inn = a.inn
        AND k2.closed_and_merged_to_u7m_id is null
       ) X

    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_newOUT")

      logInserted()
      logEnd()
    }
}

