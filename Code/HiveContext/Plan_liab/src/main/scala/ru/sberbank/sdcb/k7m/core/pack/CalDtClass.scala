package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CalDtClass (val spark: SparkSession, val config: Config, date: String) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_cal_dtIN = s"${DevSchema}.k7m_prd_dt"
  val Node2t_team_k7m_aux_d_k7m_cal_dtIN = s"${DevSchema}.k7m_lim_dt"
  val Node3t_team_k7m_aux_d_k7m_cal_dtIN = s"${Stg0Schema}.EKS_z_recont"
  val Node4t_team_k7m_aux_d_k7m_cal_dtIN = s"${Stg0Schema}.eks_z_ft_money"
   val Nodet_team_k7m_aux_d_k7m_cal_dtOUT = s"${DevSchema}.k7m_cal_dt"
  val dashboardPath = s"${config.auxPath}k7m_cal_dt"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_cal_dtOUT //витрина
  override def processName: String = "Plan_liab"

  def DoCalDt()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cal_dtOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
         select  p.id,
                 p.c_ft_limit,
                 cast(l.max_limit as decimal(38,2)) as max_limit,
                 cast(round(l.max_limit*nvl(ft.C_COURSE/ft.C_UNIT,1),2) as decimal(38,2)) as max_limit_nat
                from $Node1t_team_k7m_aux_d_k7m_cal_dtIN p
                join (select l.id, max(l.c_summa) max_limit from $Node2t_team_k7m_aux_d_k7m_cal_dtIN l group by l.id) l on l.id = p.id
           left join (
                      select c.id, c.c_cur_short, r.collection_id, r.C_COURSE, r.C_UNIT, r.C_DATE_RECOUNT C_DATE_BEG
                        ,cast(lead(date_sub(r.C_DATE_RECOUNT,1),1,cast('9999-12-31' as timestamp)) over (partition by r.collection_id order by r.C_DATE_RECOUNT ) as timestamp) as c_date_end
                        from $Node3t_team_k7m_aux_d_k7m_cal_dtIN r
                        join $Node4t_team_k7m_aux_d_k7m_cal_dtIN c on c.c_jour_recont = r.collection_id
                      ) ft
                  on ft.id = p.c_ft_limit
                 and cast('$date' as timestamp) between ft.C_DATE_BEG and ft.C_DATE_END
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cal_dtOUT")

    logInserted()
    logEnd()
  }
}

