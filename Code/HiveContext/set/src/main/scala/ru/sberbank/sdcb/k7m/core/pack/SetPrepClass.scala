package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetPrepClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_prepIN = s"${DevSchema}.set_basis_with_surety_set"
    val Node2t_team_k7m_aux_d_set_prepIN = s"${MartSchema}.clu"
    val Nodet_team_k7m_aux_d_set_prepOUT = s"${DevSchema}.set_prep"
    val dashboardPath = s"${config.auxPath}set_prep"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_prepOUT //витрина
    override def processName: String = "SET"

    def DoSetPrep() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_prepOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
select
    s.u7m_id,
    s.instrument,
    s.sur_class,
    s.sur_obj,
    s.sur_u7m_id,
    s.surety_status,
    row_number () over (partition by s.u7m_id,s.instrument, s.sur_obj, s.sur_u7m_id order by s.sur_class,s.surety_status ) rn --!!!!!!! order by s.sur_classs.surety_status добавлено разработчиком!!!!!!!!
from (
select
    s.u7m_id,
    s.instrument,
    s.sur_class,
    s.sur_obj,
    case
        when s.sur_obj = 'CLF'then s.sur_f7m_id
        else c.u7m_id
    end sur_u7m_id,
    s.surety_status
from
    $Node1t_team_k7m_aux_d_set_prepIN s
    left join
    $Node2t_team_k7m_aux_d_set_prepIN c
    on c.inn = s.sur_inn
) s""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_prepOUT")


      logInserted()
      logEnd()
    }
}

