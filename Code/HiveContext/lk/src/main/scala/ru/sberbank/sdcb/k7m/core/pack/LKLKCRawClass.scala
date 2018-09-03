package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class LKLKCRawClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {
  import LkMainClass._

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.k7m_EL"
  val Node2t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.k7m_SBL"
  val Node3t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.exsgen"
  val Node4t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.k7m_eks_eiogen"
  val Node5t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.k7m_km"
  val Node6t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.k7m_vko"
  val Node7t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.MRT3_GSL_FINAL"
  val Node8t_team_k7m_aux_d_lk_lkc_rawIN = s"${DevSchema}.basis_client"
  val Node9t_team_k7m_aux_d_lk_lkc_rawIN = s"${Stg0Schema}.rdm_link_criteria_mast"
  val Nodet_team_k7m_aux_d_lk_lkc_rawOUT = s"${DevSchema}.lk_lkc_raw"
  val dashboardPath = s"${config.auxPath}lk_lkc_raw"


  override val dashboardName: String = Nodet_team_k7m_aux_d_lk_lkc_rawOUT //витрина
  override def processName: String = "lk_lkc_raw"

  def DoLKLKCRaw() {

    Logger.getLogger(Nodet_team_k7m_aux_d_lk_lkc_rawOUT).setLevel(Level.WARN)

    logStart()

    //Удалим таблицы. Для надежного перехода на фильтрацию ЕИО на этапе EKS_ID
    spark.sql(s"drop table if exists ${config.pa}.lk")
    spark.sql(s"drop table if exists ${config.pa}.lkc")

    val Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered = s"${Nodet_team_k7m_aux_d_lk_lkc_rawOUT}_unfiltered"
    val dashboardPathUnfiltered = s"${dashboardPath}_unfiltered"

    //Отдельный этап. Выполняется ПЕРЕД ключами и служит основой для ключей.
    //Эк.связи. Пока только CLU
    val createHiveTableStage1 = spark.sql(s"""
     select
        e.crit_code as crit_id,
        cast(0 as int) as   flag_new,
        cast(e.probability as double) as link_prob,
        cast(e.quantity as double) as quantity,
        CONCAT('EL_',cast(e.loc_id as string)) link_id,
        'CLU' as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM,
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from $Node1t_team_k7m_aux_d_lk_lkc_rawIN  e
    """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPathUnfiltered)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")

    //СБ-связи. Только CLU
    val createHiveTableStage2 = spark.sql(s"""
     select
        e.crit_code as crit_id,
        cast(0 as int) as   flag_new,
        cast(e.probability as double) as link_prob,
        cast(e.quantity as double) quantity,
        CONCAT('SB_',cast(e.loc_id as string)) link_id,
        'CLU' as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM,
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from $Node2t_team_k7m_aux_d_lk_lkc_rawIN  e
    """).write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")

    // exsgen CLU
    val createHiveTableStage3 = spark.sql(s"""

     select distinct
        e.crit_id,
        cast(0 as int) as   flag_new,
        cast(100 as double) as link_prob,
        cast(1 as double) as quantity,
        CONCAT('EX_',cast(e.loc_id as string)) link_id,
        e.from_obj as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM,
        e.to_obj as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from $Node3t_team_k7m_aux_d_lk_lkc_rawIN  e
    """).write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")

    //k7m_eks_eiogen CLF (from)
    val createHiveTableStage4 = spark.sql(s"""
     select
        e.crit as crit_id,
        cast(0 as int) as   flag_new,
        cast(e.link_prob*100 as double)  as link_prob,
        cast(1 as double) as quantity,
        CONCAT('EK_',cast(e.loc_id as string)) link_id,
        'CLF' as T_FROM,
        cast(null as string) as ID_FROM,
        e.inn_benef as INN_FROM,
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.inn_org as INN_TO,
        e.position as post_nm_from
    from $Node4t_team_k7m_aux_d_lk_lkc_rawIN  e
    """).write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")


    val createHiveTableStage6 = spark.sql(s"""
  select
         'KM' as crit_id,
         cast(1 as int) as   flag_new,
         cast(100  as double)   as link_prob,
         cast(1 as double) quantity,
         CONCAT('VK_',cast(v.loc_id as string)) link_id,
         'CLF' as T_FROM,
         v.crm_id as ID_FROM,
         cast(null as string) as INN_FROM,
         'CLU' as T_TO,
         v.org_crm_id as ID_TO,
         v.org_inn_crm_num  as INN_TO,
        cast(null as string) as post_nm_from
    from $Node6t_team_k7m_aux_d_lk_lkc_rawIN v
   inner join $Node8t_team_k7m_aux_d_lk_lkc_rawIN b
      on v.org_crm_id = b.org_crm_id
    """).write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")

    //    UL
    val createHiveTableStage7 = spark.sql(s"""

      select
        u.criterion  as crit_id,
        case when   u.criterion='ben_crm' then          1         else 0 end as   flag_new ,
        cast(u.confidence as double) as link_prob,
        cast(u.quantity as double) quantity,
        CONCAT('UL_',cast(u.loc_id as string)) link_id,
        case when u.criterion in ('5.1.5','5.1.5g','5.1.6tp','5.1.7','EIOGen','ben_crm' )
               then 'CLF'
               else 'CLU'
        end  as T_FROM,
        u.crm_id1 as ID_FROM,
        u.inn1 as INN_FROM,
        'CLU' as T_TO,
        u.crm_id2 as ID_TO,
        u.inn2 as INN_TO,
        cast(null as string) as post_nm_from
     from $Node7t_team_k7m_aux_d_lk_lkc_rawIN u
""").write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered")


    val createHiveTableStageFinal1 = spark.sql(s"""
     select
        f.crit_id,
        f.flag_new,
        f.link_prob,
        f.quantity,
        f.link_id,
        f.T_FROM,
        f.ID_FROM,
        f.INN_FROM,
        f.post_nm_from,
        f.T_TO,
        f.ID_TO,
        f.INN_TO
     from $Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered f
     left join $Node9t_team_k7m_aux_d_lk_lkc_rawIN d on lower(f.crit_id) = d.code
     where ((f.quantity <= nvl(d.max_val,$defLimitUp)) and (f.quantity >= nvl( d.min_val,$defLimitDown)) or f.quantity is null)
      and (length(f.INN_FROM) = 10 or length(f.INN_FROM) = 12 or f.crit_id = 'ben_crm')
      and (length(f.INN_TO) = 10 or length(f.INN_TO) = 12 or f.INN_FROM is null and f.T_FROM = 'CLF')
      and (f.INN_FROM not like $excludeINN or f.crit_id = 'ben_crm')
      and (f.INN_FROM <> $SBRF_INN or f.crit_id = 'ben_crm')
      and f.INN_TO not like $excludeINN
      and f.INN_TO <> $SBRF_INN
      and f.crit_id not in ('EIOGEN_EKS','EIOGen')
    """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_lk_lkc_rawOUT")

    val createHiveTableStageFinal2 = spark.sql(s"""
     select
        s.crit_id,
        s.flag_new,
        s.link_prob,
        s.quantity,
        s.link_id,
        s.T_FROM,
        s.ID_FROM,
        case when length(s.INN_FROM) = 10 or length(s.INN_FROM) = 12 or s.INN_FROM is null
        then s.INN_FROM
        else cast(null as string)
        end INN_FROM,
        s.post_nm_from,
        s.T_TO,
        s.ID_TO,
        s.INN_TO
    from (
        select distinct INN
        from (
            select
                INN_TO as INN
            from $Nodet_team_k7m_aux_d_lk_lkc_rawOUT
            where T_TO = 'CLU'
                and INN_TO is not null
             union all
             select org_inn_crm_num as INN
             from $Node8t_team_k7m_aux_d_lk_lkc_rawIN
             ) i
        ) x
    join $Nodet_team_k7m_aux_d_lk_lkc_rawUnfiltered s
    on x.inn = s.INN_TO
    where  (length(s.INN_TO) = 10 or length(s.INN_TO) = 12 or s.INN_FROM is null and s.T_FROM = 'CLF')
      and s.INN_TO not like $excludeINN
      and s.INN_TO <> $SBRF_INN
      and s.crit_id in ('EIOGEN_EKS','EIOGen')
    """).persist()

    logInserted(count=createHiveTableStageFinal2.count())

    createHiveTableStageFinal2.write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_lk_lkc_rawOUT")

    logInserted()
    logEnd()
  }
}

