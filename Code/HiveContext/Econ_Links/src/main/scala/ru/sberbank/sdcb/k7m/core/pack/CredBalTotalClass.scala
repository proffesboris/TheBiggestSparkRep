package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CredBalTotalClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_cred_bal_totalIN = s"${DevSchema}.k7m_cred_bal"
  val Nodet_team_k7m_aux_d_k7m_cred_bal_totalOUT = s"${DevSchema}.k7m_cred_bal_total"
  val dashboardPath = s"${config.auxPath}k7m_cred_bal_total"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_cred_bal_totalOUT //витрина
  override def processName: String = "CRED_LIAB"

    def DoCredBal() {
      Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cred_bal_totalOUT).setLevel(Level.WARN)

      logStart()


      val createHiveTableStage1 = spark.sql(
        s""" select
      cred_id,
      sum(abs(C_BALANCE_LCL)) C_BALANCE_LCL    ,
      sum(abs(C_BALANCE)) C_BALANCE,
      sum(abs(OD_LCL)) OD_LCL    ,
      sum(abs(OD)) OD,
      sum(abs(LIM_LCL)) LIM_LCL    ,
      sum(abs(LIM)) LIM
    from  (
    select
        cred_id,
        acc_id,
        max(C_BALANCE_LCL) C_BALANCE_LCL,
        max(C_BALANCE) C_BALANCE,
        max(
            case
                when C_NAME_ACCOUNT ='2049586' then C_BALANCE_LCL
                else cast(0 as decimal(27,2))
                end
        ) OD_LCL,
        max(
            case
                when C_NAME_ACCOUNT ='2049586' then C_BALANCE
                else cast(0 as decimal(27,2))
                end
        ) OD,
        max(
            case
                when C_NAME_ACCOUNT ='2049588' then C_BALANCE_LCL
                else cast(0 as decimal(27,2))
                end
        ) LIM_LCL,
        max(
            case
                when C_NAME_ACCOUNT ='2049588' then C_BALANCE
                else cast(0 as decimal(27,2))
                end
        ) LIM
    from $Node1t_team_k7m_aux_d_k7m_cred_bal_totalIN c
    group by
        cred_id,
        acc_id
    ) x
  group by cred_id""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cred_bal_totalOUT")

      logInserted()
      logEnd()
    }
}
