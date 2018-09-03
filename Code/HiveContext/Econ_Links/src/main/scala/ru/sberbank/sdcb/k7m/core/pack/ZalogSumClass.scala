package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogSumClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_z_zalog_sumIN = s"${Stg0Schema}.eks_z_zalog"
  val Node2t_team_k7m_aux_d_k7m_z_zalog_sumIN = s"${DevSchema}.k7m_zalog_switch"
  val Node3t_team_k7m_aux_d_k7m_z_zalog_sumIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node4t_team_k7m_aux_d_k7m_z_zalog_sumIN = s"${DevSchema}.k7m_acc_rest"
  val Node5t_team_k7m_aux_d_k7m_z_zalog_sumIN = s"${Stg0Schema}.eks_Z_ZALOG_MARKET_SUM"
  val Nodet_team_k7m_aux_d_k7m_z_zalog_sumOUT = s"${DevSchema}.k7m_z_zalog_sum"
  val dashboardPath = s"${config.auxPath}k7m_z_zalog_sum"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_z_zalog_sumOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogSum(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_z_zalog_sumOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  z.id,
  z.collection_id,
  z.c_acc_zalog,
  z.c_user_zalog,
  ac.c_main_v_id zalog_acc,
  z.C_PART_TO_LOAN,
  max(case when
  coalesce(s.c_bool,'x') = '1' then abs(r.C_BALANCE_LCL)
  else coalesce(ms.c_summ ,abs(r.C_BALANCE_LCL))
  end) zalog_sum,
  max(r.C_BALANCE_LCL) C_BALANCE_LCL ,
  max(ms.c_summ ) c_summ  ,
  max(s.c_bool) c_bool
    --    z.c_market_sum,
  --c_properties
    from $Node1t_team_k7m_aux_d_k7m_z_zalog_sumIN z
    left join $Node2t_team_k7m_aux_d_k7m_z_zalog_sumIN s
    on s.collection_id = z.c_properties
  join  $Node3t_team_k7m_aux_d_k7m_z_zalog_sumIN ac
    on ac.id = z.c_acc_zalog
  join $Node4t_team_k7m_aux_d_k7m_z_zalog_sumIN r
    on ac.c_arc_move = r.c_arc_move
  left join $Node5t_team_k7m_aux_d_k7m_z_zalog_sumIN ms
    on ms.collection_id =z.c_market_sum
  where (ms.collection_id is null
    or  cast(date'${dateString}'as timestamp) BETWEEN ms.C_DATE_BEG AND coalesce( ms.C_DATE_END, cast(date'4000-01-01' as timestamp)))
  and r.c_balance_lcl<>0
  group by
    z.id,
  z.collection_id,
  z.c_acc_zalog,
  ac.c_main_v_id,
  z.c_user_zalog,
  z.C_PART_TO_LOAN
  having max(case when
  coalesce(s.c_bool,'x') = '1' then abs(r.C_BALANCE_LCL)
  else coalesce(ms.c_summ ,abs(r.C_BALANCE_LCL))
  end) <>0
          """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_z_zalog_sumOUT")

    logInserted()
    logEnd()
  }
}
