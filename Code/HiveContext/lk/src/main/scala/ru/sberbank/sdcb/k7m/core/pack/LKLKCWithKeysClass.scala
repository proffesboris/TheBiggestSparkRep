package ru.sberbank.sdcb.k7m.core.pack


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class LKLKCWithKeysClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {
  import LkMainClass._

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.lk_lkc_raw"
  val Node2t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.clu_keys"
  val Node3t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.clf_keys"
  val Nodet_team_k7m_aux_d_lk_lkc_rawOUT = s"${DevSchema}.lk_lkc_with_keys"
  val dashboardPath = s"${config.auxPath}lk_lkc_with_keys"


  override val dashboardName: String = Nodet_team_k7m_aux_d_lk_lkc_rawOUT //витрина
  override def processName: String = "lk_lkc"

  def DoLKLKCWithKeys() {

    Logger.getLogger(Nodet_team_k7m_aux_d_lk_lkc_rawOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(s"""
   select
          rw.crit_id,
          rw.flag_new ,
          rw.link_prob,
          rw.quantity,
          rw.link_id,
          rw.T_FROM,
          rw.ID_FROM,
          rw.INN_FROM,
          rw.post_nm_from,
          rw.T_TO,
          rw.ID_TO,
          rw.INN_TO ,
          k1i.u7m_id u7m_id_from_by_inn,
          k1c.u7m_id u7m_id_from_by_crm,
          k1f.f7m_id f7m_id_from,
          k2i.u7m_id u7m_id_to_by_inn,
          k2c.u7m_id u7m_id_to_by_crm
     from $Node1t_team_k7m_aux_d_lk_lkc_rawIN rw
left join $Node2t_team_k7m_aux_d_lk_lkc_rawIN k1i
       on k1i.inn = rw.inn_from
      and k1i.status='ACTIVE'
left join $Node2t_team_k7m_aux_d_lk_lkc_rawIN k1c
       on k1c.crm_id = rw.ID_FROM
      and k1c.status='ACTIVE'
left join $Node2t_team_k7m_aux_d_lk_lkc_rawIN k2i
       on k2i.inn = rw.inn_to
      and k2i.status='ACTIVE'
left join $Node2t_team_k7m_aux_d_lk_lkc_rawIN k2c
       on k2c.crm_id = rw.ID_to
      and k2c.status='ACTIVE'
left join $Node3t_team_k7m_aux_d_lk_lkc_rawIN k1f
       on k1f.link_id = rw.link_id""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_lk_lkc_rawOUT")

    logInserted()

    //---------------------------------------------------------------------
    //--  Выполнить проверки:
    //  -- Записать расхождения в журнал CUSTOM_LOG с типом ERR
    //---------------------------------------------------------------------

    //1) Не найдены суррогатные ключи:
      //сторона FROM
    val checkStage1 = spark.sql(s"""
select
    t_from,
    crit_id,
    cast(sum(
        case   when
                not (u7m_id_from_by_inn is not null
                or  coalesce(length(inn_from),0) not in (10,12)
                or  inn_from like '00%')
                and
                    not (
                    u7m_id_from_by_crm is not null
                    or id_from is not null
                    )
                 and t_from ='CLU'
              or
                f7m_id_from is null
                and t_from ='CLF'
            then 1
            else 0
    end) as string) empty ,
    cast(count(*) as string) total
from ${config.aux}.lk_lkc_with_keys
where id_from is not null or coalesce(length(inn_from)) in (10,12)
group by        t_from, crit_id
having empty>0""").collect().toSeq

    val log1 = checkStage1.foreach(c=>
    log("LK-LKC","Не найдены суррогатные ключи со стороны FROM. "+c.getString(0)+", "+c.getString(1)+" пустые "+c.getString(2)+", всего "+c.getString(3), CustomLogStatus.ERROR))

    //сторона TO
    val checkStage2 = spark.sql(s"""
select
      t_to,
      crit_id,
      cast(sum(case   when
                  not (u7m_id_to_by_inn is not null
                  or  coalesce(length(inn_to),0) not in (10,12)
                  or  inn_to like '00%')
                  and
                      not (
                      u7m_id_to_by_crm is not null
                      or id_to is not null
                      )
                   and t_to ='CLU'
                or
                  f7m_id_from is null
                  and t_to ='CLF'
              then 1
              else 0
      end) as string) empty ,
      cast(count(*) as string) total
    from ${config.aux}.lk_lkc_with_keys
    where id_to is not null or coalesce(length(inn_to)) in (10,12)
group by        t_to, crit_id
having empty>0""").collect().toSeq

    val log2 = checkStage2.foreach(c=>
      log("LK-LKC","Не найдены суррогатные ключи со стороны TO. "+c.getString(0)+", "+c.getString(1)+" пустые "+c.getString(2)+", всего "+c.getString(3), CustomLogStatus.ERROR))

    // Дубли (не должно быть)
    val checkStage3 = spark.sql(s"""
    select link_id, cast(count(*) as string) cnt
    from ${config.aux}.lk_lkc_with_keys
    group by        link_id
    having count(*)>1""").collect().toSeq

    val log3 = checkStage3.foreach(c=>
      log("LK-LKC","Дубли. "+c.getString(0)+": "+c.getString(1), CustomLogStatus.ERROR))

    //Конфликт в ключах(Не должно быть):
      //сторона FROM
    val checkStage4 = spark.sql(s"""
    select crit_id, cast(count(*) as string) cnt
    from ${config.aux}.lk_lkc_with_keys
    where
    u7m_id_from_by_inn <> u7m_id_from_by_crm
    group by         crit_id""").collect().toSeq

    val log4 = checkStage4.foreach(c=>
      log("LK-LKC","Конфликт в ключах со стороны FROM. "+c.getString(0)+": "+c.getString(1), CustomLogStatus.ERROR))

    //сторона FROM
    val checkStage5 = spark.sql(s"""
    select crit_id, cast(count(*) as string) cnt
    from ${config.aux}.lk_lkc_with_keys
    where
    u7m_id_to_by_inn <> u7m_id_to_by_crm
    group by         crit_id""").collect().toSeq

    val log5 = checkStage5.foreach(c=>
      log("LK-LKC","Конфликт в ключах со стороны TO. "+c.getString(0)+": "+c.getString(1), CustomLogStatus.ERROR))
    //------------КОНЕЦ ПРОВЕРОК--------------

    logEnd()
  }
}
