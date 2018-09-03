package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetPrepRiskClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_prep_riskIN = s"${DevSchema}.set_prep"
    val Node2t_team_k7m_aux_d_set_prep_riskIN = s"${DevSchema}.proxyrs_int_final"
    val Node3t_team_k7m_aux_d_set_prep_riskIN = s"${MartSchema}.PDStandalone"
    val Nodet_team_k7m_aux_d_set_prep_riskOUT = s"${DevSchema}.set_prep_risk"
    val dashboardPath = s"${config.auxPath}set_prep_risk"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_prep_riskOUT //витрина
    override def processName: String = "SET"

    def DoSetPrepRisk() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_prep_riskOUT).setLevel(Level.WARN)

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
        case
            when coalesce(rs.risk_segment,'X') in  ('Кредитование по пилот. моделям','Корпор. компания-резидент (РФ)')    then 'Y'
            else 'N'
        end sur_corporate_flag,
     case when pd.pd_standalone<0.00032 then 1   --SCALE:Для шкалы использовать таблицу t_team_k7m_aux_p.dict_BO_PD_by_SCALE. вместо значения для сравнения (например 0.00032, 0.00044 ...) использовать поле upper_bound. Вместо результирующего значения (например 1,2...) использовать поле rating
          when pd.pd_standalone<0.00044 then 2
          when pd.pd_standalone<0.0006 then 3
          when pd.pd_standalone<0.00083 then 4
          when pd.pd_standalone<0.00114 then 5
          when pd.pd_standalone<0.00156 then 6
          when pd.pd_standalone<0.00215 then 7
          when pd.pd_standalone<0.00297 then 8
          when pd.pd_standalone<0.00409 then 9
          when pd.pd_standalone<0.00563 then 10
          when pd.pd_standalone<0.00775 then 11
          when pd.pd_standalone<0.01067 then 12
          when pd.pd_standalone<0.0147 then 13
          when pd.pd_standalone<0.02024 then 14
          when pd.pd_standalone<0.02788 then 15
          when pd.pd_standalone<0.03839 then 16
          when pd.pd_standalone<0.05287 then 17
          when pd.pd_standalone<0.0728 then 18
          when pd.pd_standalone<0.10026 then 19
          when pd.pd_standalone<0.13807 then 20
          when pd.pd_standalone<0.19014 then 21
          when pd.pd_standalone<0.26185 then 22
          when pd.pd_standalone<0.36059  then 23
          when pd.pd_standalone<0.49659 then 24
          when pd.pd_standalone<1 then 25
          when pd.pd_standalone>=1 then 26
         end as sur_rating
from $Node1t_team_k7m_aux_d_set_prep_riskIN s
    left join  $Node2t_team_k7m_aux_d_set_prep_riskIN rs
    on s.sur_u7m_id = rs.u7m_id
      and s.sur_class ='CLU'
    left join $Node3t_team_k7m_aux_d_set_prep_riskIN pd
    on s.sur_class ='CLU'
     and s.sur_u7m_id = pd.u7m_id
where s.rn = 1""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_prep_riskOUT")


      logInserted()
      logEnd()
    }
}

