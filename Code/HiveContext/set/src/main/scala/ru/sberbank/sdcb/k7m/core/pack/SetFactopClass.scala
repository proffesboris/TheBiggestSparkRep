package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Date
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetFactopClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_factopIN = s"${DevSchema}.set_cred"
    val Node2t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_pr_cred"
    val Node3t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_vid_oper_dog"
    val Node4t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.rdm_set_prov_type_mast"
    val Node5t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_fact_oper"
    val Node6t_team_k7m_aux_d_set_factopIN = s"${DevSchema}.k7m_cur_courses"
    val Node7t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_kind_credits"
    val Node8t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_types_cred"
    val Node9t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_obj_kred"
    val Node10t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_Z_branch"
    val Node11t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_object_cred"
    val Node13t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_properties"
    val Node14t_team_k7m_aux_d_set_factopIN = s"${Stg0Schema}.eks_z_prod_property"
    val Nodet_team_k7m_aux_d_set_factopOUT = s"${DevSchema}.set_factop"
    val dashboardPath = s"${config.auxPath}set_factop"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_factopOUT //витрина
    override def processName: String = "SET"

    def DoSetFactop() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_factopOUT).setLevel(Level.WARN)

      logStart()

      val Nodet_team_k7m_aux_d_set_factop1 = Nodet_team_k7m_aux_d_set_factopOUT.concat("_1")
      val dashboardPath1 = s"${config.auxPath}set_factop_1"
      val Nodet_team_k7m_aux_d_set_factop2 = Nodet_team_k7m_aux_d_set_factopOUT.concat("_2")
      val dashboardPath2 = s"${config.auxPath}set_factop_2"

      val createHiveTable1 = spark.sql(s"""
select
       fo.id,
       lcr.pr_cred_id as pr_cred_id,
       fo.c_date,
       fo.c_summa ,
       fo.c_valuta,
       fo.c_reg_currency_sum,
       lcr.summa_currency as to_currency,
       lcr.C_FT_CREDIT,
       vd.c_sys_name,
       pt.vid,
       pt.ob
  from $Node1t_team_k7m_aux_d_set_factopIN as lcr
  join $Node2t_team_k7m_aux_d_set_factopIN  as cr
    on lcr.pr_cred_id = cr.c_high_level_cr
  join internal_eks_ibs.z_fact_oper  fo --Вернуть 0 слой
    on fo.collection_id = cr.c_list_pay
  join $Node3t_team_k7m_aux_d_set_factopIN vd
      on vd.id = fo.c_oper
   left join (
        SELECT
            nm,
            MAX(ob) AS ob,
            MAX(vid) as vid
        FROM $Node4t_team_k7m_aux_d_set_factopIN
        group by nm
        )  as pt
    on pt.nm = lower(vd.c_sys_name)
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath1)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_factop1")

      val createHiveTable2 = spark.sql(s"""
select
     fo.id,
     lcr.pr_cred_id as pr_cred_id,
     fo.c_date,
     fo.c_summa ,
     fo.c_valuta,
     fo.c_reg_currency_sum,
     lcr.summa_currency as to_currency,
     lcr.C_FT_CREDIT,
     vd.c_sys_name,
     pt.vid,
     pt.ob,
     pt.nm,
     fo.c_oper
from
   $Node1t_team_k7m_aux_d_set_factopIN  as lcr
   join $Node5t_team_k7m_aux_d_set_factopIN   fo --Вернуть 0 слой
     on fo.collection_id = lcr.c_list_pay
   join $Node3t_team_k7m_aux_d_set_factopIN vd
      on vd.id = fo.c_oper
   left join
        (
        SELECT
            nm,
            MAX(ob) AS ob,
            MAX(vid) as vid
        FROM $Node4t_team_k7m_aux_d_set_factopIN
        group by nm
        )  as pt
    on pt.nm = lower(vd.c_sys_name)
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath2)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_factop2")

      val createHiveTable3 = spark.sql(s"""
select
        id,
       c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob
from     (
    select
        id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob
    from $Nodet_team_k7m_aux_d_set_factop1
    where c_valuta=c_ft_credit
    union all
    select
        id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob
    from $Nodet_team_k7m_aux_d_set_factop2
    where c_valuta=c_ft_credit
    union all
    select
        f.id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        (f.c_summa *crs1.course)/crs2.course c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob
    from
        $Nodet_team_k7m_aux_d_set_factop1 f
        join $Node6t_team_k7m_aux_d_set_factopIN crs1 -- L4_5 Считается для эк.связей
            on crs1.id = f.c_valuta
        join $Node6t_team_k7m_aux_d_set_factopIN crs2 -- L4_5 Считается для эк.связей
            on crs2.id = f.to_currency
    where f.c_date  between crs1.from_dt and crs1.to_dt
       and f.c_date  between crs2.from_dt and crs2.to_dt
       and f.c_valuta <> f.c_ft_credit
    union all
    select
        f.id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        (f.c_summa *crs1.course)/crs2.course c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob
    from
        $Nodet_team_k7m_aux_d_set_factop1 f
        join $Node6t_team_k7m_aux_d_set_factopIN crs1 -- L4_5 Считается для эк.связей
            on crs1.id = f.c_valuta
        join $Node6t_team_k7m_aux_d_set_factopIN crs2 -- L4_5 Считается для эк.связей
            on crs2.id = f.to_currency
    where f.c_date  between crs1.from_dt and crs1.to_dt
       and f.c_date  between crs2.from_dt and crs2.to_dt
       and f.c_valuta <> f.c_ft_credit
) x
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_factopOUT")

      logInserted()
      logEnd()
    }
}

