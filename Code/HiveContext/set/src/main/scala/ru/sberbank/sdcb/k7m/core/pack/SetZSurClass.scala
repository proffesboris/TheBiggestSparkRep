package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetZSurClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_set_z_surIN = s"${DevSchema}.set_bo_agg2"
    val Node2t_team_k7m_aux_set_z_surIN = s"${Stg0Schema}.eks_z_part_to_loan"
    val Node3t_team_k7m_aux_set_z_surIN = s"${Stg0Schema}.eks_Z_zalog"
    val Node4t_team_k7m_aux_set_z_surIN = s"${Stg0Schema}.eks_z_ac_fin"
    val Node5t_team_k7m_aux_set_z_surIN = s"${Stg0Schema}.eks_z_client"
    val Nodet_team_k7m_aux_set_z_surOUT = s"${DevSchema}.set_z_sur"
    val dashboardPath = s"${config.auxPath}set_z_sur"


    override val dashboardName: String = Nodet_team_k7m_aux_set_z_surOUT //витрина
    override def processName: String = "SET"

    def DoSetZSur() {

      Logger.getLogger(Nodet_team_k7m_aux_set_z_surOUT).setLevel(Level.WARN)

      logStart()


      val readHiveTable1 = spark.sql(s"""
select 
    distinct 
        t1.pr_cred_id,
        t1.inn, 
        t1.ndog,
        t1.ddog,
        case 
            when acc.c_main_v_id like '91414%' and guarantor.class_id = 'CL_PRIV' then 'ПФЛ'
            when acc.c_main_v_id like '91414%' and guarantor.class_id = 'CL_ORG'  then 'ПЮЛ'
        end as type_sur, 
        case 
            when zal.id is not null 
                and acc.c_main_v_id not like '91414%' 
                then 1 
                else 0 
                end as zal_flag,
        guarantor.id as sur_client_id, 
        guarantor.c_inn 
    from $Node1t_team_k7m_aux_set_z_surIN as t1
    join $Node2t_team_k7m_aux_set_z_surIN pl
      on pl.c_product= t1.pr_cred_id
    join $Node3t_team_k7m_aux_set_z_surIN zal
      on zal.c_part_to_loan = pl.collection_id
    join $Node4t_team_k7m_aux_set_z_surIN as acc
      on zal.c_acc_zalog = acc.id
    join $Node5t_team_k7m_aux_set_z_surIN as guarantor
      on zal.c_user_zalog = guarantor.id
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_set_z_surOUT")

      logInserted()
      logEnd()
    }
}

