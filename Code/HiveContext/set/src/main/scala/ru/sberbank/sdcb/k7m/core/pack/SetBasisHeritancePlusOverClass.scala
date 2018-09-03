package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetBasisHeritancePlusOverClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa
    val gMaxWarrantorsCount = 5 //TODO Определять значение из ETL_TASK_PARAM



    val Node1t_team_k7m_aux_d_set_basis_heritance_plus_overIN = s"${DevSchema}.set_basis_with_heritance"
    val Nodet_team_k7m_aux_d_set_basis_heritance_plus_overOUT = s"${DevSchema}.set_basis_heritance_plus_over"
    val dashboardPath = s"${config.auxPath}set_basis_heritance_plus_over"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_basis_heritance_plus_overOUT //витрина
    override def processName: String = "SET"

    def DoSetBasisHeritancePlusOver() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_basis_heritance_plus_overOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
--добавим поручителей базису
select  *
    ,case when heritance_flag = 1 and cnt_heritage_sur <= $gMaxWarrantorsCount then 'Ok'
          when heritance_flag = 1 and cnt_heritage_sur > $gMaxWarrantorsCount  then 'Block'
          when heritance_flag = 0 and instrument = 'ОВЕР' then 'Ok'
          else null
     end as surety_status
from $Node1t_team_k7m_aux_d_set_basis_heritance_plus_overIN""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_basis_heritance_plus_overOUT")


      logInserted()
      logEnd()
    }
}

