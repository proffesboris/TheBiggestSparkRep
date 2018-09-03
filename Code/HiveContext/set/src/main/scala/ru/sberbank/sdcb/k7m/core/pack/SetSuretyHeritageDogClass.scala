package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetSuretyHeritageDogClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_surety_heritage_dogIN = s"${DevSchema}.set_basis_client_instr_rep"
    val Node2t_team_k7m_aux_d_set_surety_heritage_dogIN = s"${DevSchema}.set_suretyships_stat_nc"
    val Nodet_team_k7m_aux_d_set_surety_heritage_dogOUT = s"${DevSchema}.set_surety_heritage_dog_"
    val dashboardPath = s"${config.auxPath}set_surety_heritage_dog_"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_surety_heritage_dogOUT //витрина
    override def processName: String = "SET"

    def DoSetSuretyHeritageDog(dt: String) {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_surety_heritage_dogOUT).setLevel(Level.WARN)

      logStart()
      //поиск договора для наследования
      val readHiveTable1 = spark.sql(s"""
--поиск договора для наследования
select
    b.u7m_id,
    b.inn,
    b.org_name as org_short_crm_name,
    b.bsegment org_segment_name,
    b.instrument,
    case when coalesce(s.pr_cred_id) is not null then 1 else 0 end as heritance_flag,
    case
        when (s.any_surety = 0 and s.collateral = 0) then "without_surety"
        when (s.any_surety = 1 and s.collateral = 1) then "surety_and_coll"
        when (s.any_surety = 1 and s.collateral = 0) then "only_surety"
        when (sc.any_surety = 0 and sc.collateral = 1) then "only_collateral"
        when (w.pr_cred_id <> s.pr_cred_id and w.pr_cred_id <> sc.pr_cred_id and w.pr_cred_id is not NULL)   then "Old_or_More_then_3_year"
        when (oc.cnt_over > 0  and oc.cnt_credit < 1) then "only_over"
        when (oc.cnt_over > 0  and oc.cnt_credit > 0) then "over and credit"
        when (oc.cnt_over < 1  and oc.cnt_credit > 0) then "only credit"
    end as sur_dog_type,
    s.pr_cred_id,
    sc.pr_cred_id as  sc_pr_cred_id,
    w.pr_cred_id as w_pr_cred_id,
    s.ndog,
    s.ddog
from $Node1t_team_k7m_aux_d_set_surety_heritage_dogIN b
    left join (
    select a.* from (
        select *
            ,row_number() over (
                partition by inn,instrument
                order by ddog desc,
                    pr_cred_id desc) as rn
        from $Node2t_team_k7m_aux_d_set_surety_heritage_dogIN
        where dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию')
                  or
                  (product_status = 'Закрыт' and months_between(DATE'${dt}',to_date(date_close)) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
            and not (any_surety = 0 and collateral = 1)
        ) a
        where rn = 1
            and pr_cred_id is not null
            and ndog is not null
            and ddog is not null
) s
on
   b.inn = s.inn
   and b.instrument = s.instrument
left join (
    select a2.* from (
        select *
            ,row_number() over (partition by inn,instrument order by ddog desc, pr_cred_id desc) as rn
        from $Node2t_team_k7m_aux_d_set_surety_heritage_dogIN
        where  dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию')
                  or
                  (product_status = 'Закрыт' and months_between(DATE'${dt}',to_date(date_close)) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) a2
    where rn = 1
        and pr_cred_id is not null
        and ndog is not null
        and ddog is not null
    ) sc
on
    b.inn = sc.inn
    and b.instrument = sc.instrument
left join (
    select a3.*
    from (
        select *
            ,row_number() over (partition by inn,instrument order by ddog desc, pr_cred_id desc) as rn
        from $Node2t_team_k7m_aux_d_set_surety_heritage_dogIN
        where  instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) a3
        where rn = 1
            and pr_cred_id is not null
            and ndog is not null
            and ddog is not null
) w on b.inn = w.inn and b.instrument = w.instrument
left join (
    select a4.* from (
        select
            inn,
            sum(case when instrument = 'ОВЕР' then 1 else 0 end)  as cnt_over,
            sum(case when instrument = 'КД/НКЛ/ВКЛ' then 1 else 0 end) as cnt_credit
        from $Node2t_team_k7m_aux_d_set_surety_heritage_dogIN
        where instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        group by inn
        ) a4
) oc on b.inn = oc.inn """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_surety_heritage_dogOUT")


      logInserted()
      logEnd()
    }
}

