package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetSuretyshipsStatNcClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_suretyships_stat_ncIN = s"${DevSchema}.set_bo_agg2"
    val Node2t_team_k7m_aux_d_set_suretyships_stat_ncIN = s"${DevSchema}.set_z_sur"
    val Nodet_team_k7m_aux_d_set_suretyships_stat_ncOUT = s"${DevSchema}.set_suretyships_stat_nc"
    val dashboardPath = s"${config.auxPath}set_suretyships_stat_nc"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_suretyships_stat_ncOUT //витрина
    override def processName: String = "SET"

    def DoSetSuretyshipsStatNc() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_suretyships_stat_ncOUT).setLevel(Level.WARN)

      logStart()


      val readHiveTable1 = spark.sql(s"""
select
    cast(t1.client_id as bigint) as  client_id,
    t1.client_name,
    t1.inn,
    cast(cast (t1.pr_cred_id as double ) as bigint) as  pr_cred_id ,
    t1.ndog,
    t1.ddog,
    t1.date_close,
    t1.cred_flag,
    t1.nkl_flag,
    t1.vkl_flag,
    t1.over_flag,
    t1.instrument,
    t1.branch,
    t1.branch_ru,
    case when t1.num_cr_desc = 1 then 'Последний' else 'Не последний' end as last_deal_flag,
    t1.dur_in_days,
    t1.product_status,
    case when  t1.dur_in_days <= 366 then '1. менее 1 года'
          when 366<  t1.dur_in_days and  t1.dur_in_days <= 366*1.5 then '2. от 1 года до 1.5-х'
          when 366*1.5<  t1.dur_in_days and  t1.dur_in_days <= 366*2 then '3. от 1.5 года до 2-х'
          when 366*2<  t1.dur_in_days and  t1.dur_in_days <= 366*3 then '4. от 2 лет до 3-х'
    end as DUR,
    max(case when t2.type_sur is not null then 1 else 0 end) as any_surety,
    max(case when t2.type_sur = 'ПФЛ' then 1 else 0 end) as fis_surety,
    max(case when t2.type_sur = 'ПЮЛ' then 1 else 0 end) as jur_surety,
    max(case when t2.zal_flag is not null and t2.zal_flag<> 0 then 1 else 0 end) as collateral
  from  $Node1t_team_k7m_aux_d_set_suretyships_stat_ncIN as t1
  left join $Node2t_team_k7m_aux_d_set_suretyships_stat_ncIN as t2
    on t1.pr_cred_id  = t2.pr_cred_id
where
    (t1.cred_flag=1 or t1.nkl_flag=1 or t1.vkl_flag=1 or t1.over_flag=1)
group by
    t1.client_id,
    t1.client_name,
    t1.inn,
    t1.pr_cred_id,
    t1.ndog,
    t1.ddog,
    t1.date_close,
    t1.cred_flag,
    t1.nkl_flag,
    t1.vkl_flag,
    t1.over_flag,
    t1.instrument,
    t1.branch,
    t1.branch_ru,
    case when t1.num_cr_desc = 1 then 'Последний' else 'Не последний' end,
    t1.dur_in_days,
    t1.product_status""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_suretyships_stat_ncOUT")

      logInserted()
      logEnd()
    }
}

