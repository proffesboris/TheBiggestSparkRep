package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetBasisWithSuretySetClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_basis_with_surety_setIN = s"${DevSchema}.set_basis_heritance_plus_over"
    val Node2t_team_k7m_aux_d_set_basis_with_surety_setIN = s"${DevSchema}.sep_surety_7M_for_non_client_sur_init"
    val Node3t_team_k7m_aux_d_set_basis_with_surety_setIN = s"${DevSchema}.set_suretyships_stat_nc"
    val  Nodet_team_k7m_aux_d_set_basis_with_surety_setOUT = s"${DevSchema}.set_basis_with_surety_set"
    val dashboardPath = s"${config.auxPath}set_basis_with_surety_set"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_basis_with_surety_setOUT //витрина
    override def processName: String = "SET"

    def DoSetBasisWithSuretySet(dt: String) {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_basis_with_surety_setOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
select
     t1.u7m_id,
     t1.org_short_crm_name,
     t1.org_segment_name
    ,t1.instrument
    ,t1.heritance_flag
    ,t1.sur_dog_type
    ,t1.pr_cred_id
    ,t1.ndog
    ,t1.ddog
    ,t1.cnt_heritage_sur,
    case when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.sur_obj
          when t2.u7m_id is not null then 'CLU'
          else null
     end as sur_obj,
     t1.sur_f7m_id,
     t1.loc_id,
     case when t1.surety_status = 'Ok' and heritance_flag = 1 then 'SUCL'
          when t2.u7m_id is not null then 'SUNCL'
          else null
     end as sur_class,
     case when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.sur_inn
          when t2.u7m_id is not null then t2.sur_inn
          else null
     end as sur_inn,
     case
        when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.surety_status
        when t2.u7m_id is not null then 'Ok'
        when t1.instrument ='ОВЕР' then t1.surety_status
        when t1.surety_status <> 'Ok' then t1.surety_status
        else 'FIND'
     end as surety_status,
     case when t3.inn is not null then 1 else 0 end as Clients_7m
from
    $Node1t_team_k7m_aux_d_set_basis_with_surety_setIN t1
    left join $Node2t_team_k7m_aux_d_set_basis_with_surety_setIN t2 -- =surety_7M_for_non_client_final !
    on
        t1.u7m_id =t2.u7m_id
        and t1.instrument = 'КД/НКЛ/ВКЛ'
        and t1.surety_status is null
    left join (
        select distinct inn
        from $Node3t_team_k7m_aux_d_set_basis_with_surety_setIN
        where  dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию')
                  or
                  (product_status = 'Закрыт' and months_between(date'${dt}',to_date(date_close)) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) t3
     on t3.inn = t1.inn""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_basis_with_surety_setOUT")


      logInserted()
      logEnd()
    }
}

