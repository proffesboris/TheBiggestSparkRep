package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetBasisWithHeritanceClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_basis_with_heritanceIN = s"${DevSchema}.set_surety_heritage_dog_"
    val Node2t_team_k7m_aux_d_set_basis_with_heritanceIN = s"${DevSchema}.set_surety_heritage_dog_sur_cnt"
    val Nodet_team_k7m_aux_d_set_basis_with_heritanceOUT = s"${DevSchema}.set_basis_with_heritance"
    val dashboardPath = s"${config.auxPath}set_basis_with_heritance"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_basis_with_heritanceOUT //витрина
    override def processName: String = "SET"

    def DoSetBasisWithHeritance() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_basis_with_heritanceOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
--добавим поручителей базису
select
    b.*,
    hs.sur_obj,
    hs.sur_f7m_id,
    hs.sur_inn,
    hs.loc_id,
    coalesce(hs_cnt.cnt_sur,0) as cnt_heritage_sur
from
    $Node1t_team_k7m_aux_d_set_basis_with_heritanceIN b
    left join $Node2t_team_k7m_aux_d_set_basis_with_heritanceIN hs
    on b.pr_cred_id = hs.pr_cred_id
        and b.instrument = hs.instrument and
        hs.too_much_sur_flag = 0
    left join (
        select
            distinct
                pr_cred_id,
                instrument,
                cnt_sur
        from $Node2t_team_k7m_aux_d_set_basis_with_heritanceIN) hs_cnt
    on
        b.pr_cred_id = hs_cnt.pr_cred_id
        and b.instrument = hs_cnt.instrument  """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_basis_with_heritanceOUT")


      logInserted()
      logEnd()
    }
}

