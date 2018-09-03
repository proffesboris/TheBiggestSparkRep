package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Date
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetCredFilteredClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_cred_filteredIN = s"${DevSchema}.set_cred"
    val Node3t_team_k7m_aux_d_set_cred_filteredIN = s"${Stg0Schema}.rdm_set_regimes_mast"
    val Node4t_team_k7m_aux_d_set_cred_filteredIN = s"${Stg0Schema}.rdm_set_vid_cred_mast"
    val Nodet_team_k7m_aux_d_set_cred_filteredOUT = s"${DevSchema}.set_cred_filtered"
    val dashboardPath = s"${config.auxPath}set_cred_filtered"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_cred_filteredOUT //витрина
    override def processName: String = "SET"

    def DoSetCredFiltered() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_cred_filteredOUT).setLevel(Level.WARN)

      logStart()

      val dashboardPath0 = s"${config.auxPath}rdm_deal_target_prepared"

      val readHiveTable0 = spark.sql(s"""
      select
        max(target) as target,
        lower(regexp_replace(B.target_cred,' |\\r|\\n|\\"','')) target_cred
      from ${Stg0Schema}.rdm_deal_target_mast b
      group by lower(regexp_replace(B.target_cred,' |\\r|\\n|\\"',''))
              """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath0)
        .saveAsTable(s"${DevSchema}.rdm_deal_target_prepared")



      val readHiveTable1 = spark.sql(s"""
select
    distinct
    a.client_id,
a.inn,
a.kpp,
a.client_name,
a.crm_segment,
a.pr_cred_id,
a.pr_cred_class_id,
a.pr_cred_collection_id,
a.ndog,
a.ddog,
a.dur_in_days,
a.dur_in_month,
a.c_date_ending,
a.date_close,
a.c_register_date_reg,
a.age,
a.summa_base,
a.summa_ru,
a.c_ft_credit,
a.summa_currency,
a.limit_amt,
a.limit_currency,
a.regime_prod,
a.cred_flag,
a.nkl_flag,
a.vkl_flag,
a.over_flag,
a.instrument,
a.c_name,
a.c_short_name,
a.avp_in_days,
a.avp_in_month,
a.target_cred,
a.target_cred2,
a.cr_type26,
a.c_list_pay,
a.product_status,
a.branch,
a.branch_ru,
a.cc_id,
a.cl_id,
a.st_id,
a.dep_id,
a.kc_id,
a.tc_id,
a.ok_id,
a.b_id,
a.zoc_id,
a.zok_id,
a.prop1_id,
a.prop2_id,
a.prod_prop_id,
    B.target b_target,
    C.use as c_USE,
    D.target  d_target
from $Node1t_team_k7m_aux_d_set_cred_filteredIN as A
left join ${DevSchema}.rdm_deal_target_prepared as B on
    lower(regexp_replace(A.target_cred2,' |\\r|\\n|\\"','')) = B.target_cred
left join $Node3t_team_k7m_aux_d_set_cred_filteredIN  as C on
    lower(regexp_replace(A.regime_prod,' |\\r|\\n|\\"','')) = lower(regexp_replace(C.regime_prod,' |\\r|\\n|\\"',''))
left join $Node4t_team_k7m_aux_d_set_cred_filteredIN  as D on
    lower(regexp_replace(A.CR_TYPE26,' |\\r|\\n|\\"','')) = lower(regexp_replace(D.C_name,' |\\r|\\n|\\"',''))
where 1=1
    --DELETED
    --and (B.target <> 'исключить' or a.over_flag =1 or B.target is null)
    --and C.use <> 'исключить'
   -- and (D.target <> 'исключить' or D.target is null)
    --- DELETED
    --INSERTED 12.04.2018 TEMIN-LV START
    and (B.target = 'оставить' or a.over_flag =1)
    and C.use = 'оставить'
    and (D.target = 'оставить' or D.target is null)
    --INSERTED 12.04.2018 TEMIN-LV END
    and a.product_status not in ( 'Отменен', 'Утверждено мировое соглашение')
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_cred_filteredOUT")

      logInserted()
      logEnd()
    }
}

