package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL52125214Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5212_5214IN = s"${DevSchema}.k7m_EL5212a_prep"
  val Nodet_team_k7m_aux_d_k7m_EL_5212_5214OUT = s"${DevSchema}.k7m_EL_5212_5214"
  val dashboardPath = s"${config.auxPath}k7m_EL_5212_5214"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5212_5214OUT //витрина
  override def processName: String = "EL"

  def DoEL52125214(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5212_5214OUT).setLevel(Level.WARN)

    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""SELECT
      cast (date'${dateString}' as timestamp) report_dt,
      from_cid,
      from_inn,
      to_cid,
      to_inn,
      concat(crit_code,
      case
      when   quantity>=${SparkMainClass.qnt5212_5214}
      and  (unix_timestamp(cast(date'${dateString}'as timestamp)   )*1000 -unix_timestamp(issue_dt)*1000 )>= 3*3600*24*30*1000
      or quantity>=1
      then ''
      else
      case
      when from_cid ='CL_PRIV' then '_gfl'
      else '_gul'
      end
      end        ) crit_code,
      100 probability,
      quantity,
      issue_dt source,
      fact_SUM base_amt,
      PROV_SUM absolute_value_amt
  FROM (
      select
        zalog_class_id from_cid,
      from_inn,
      cred_class_id to_cid,
      to_inn,
      issue_dt,
      quantity,
      PROV_SUM,
      fact_SUM,
      cred_id,
      crit_code,
      ROW_NUMBER() OVER (PARTITION BY crit_code,to_inn, from_inn ORDER BY quantity DESC) RN
        from $Node1t_team_k7m_aux_d_k7m_EL_5212_5214IN
  ) x
  WHERE RN=1   """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5212_5214OUT")

    logInserted()
    logEnd()
  }
}
