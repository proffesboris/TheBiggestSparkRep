package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EKSCurCoursesClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_cur_coursesIN = s"${Stg0Schema}.eks_z_ft_money"
  val Node2t_team_k7m_aux_d_k7m_cur_coursesIN = s"${Stg0Schema}.eks_z_recont"
  val Nodet_team_k7m_aux_d_k7m_cur_coursesOUT = s"${DevSchema}.k7m_cur_courses"
  val dashboardPath = s"${config.auxPath}k7m_cur_courses"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_cur_coursesOUT //витрина
  override def processName: String = "EL"

  def DoEKSCurCourses() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cur_coursesOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      id,
      c_cur_short,
      unix_timestamp(c_date_recount)*1000 c_date_recount,
      unix_timestamp(next_date_recount)*1000 till_c_date_recount,
      cast(concat(substr(cast(c_date_recount as string),1,10),' 00:00:00') as timestamp)   from_dt,
      cast(concat(substr(cast(date_sub(cast(next_date_recount as date),1) as string),1,10),' 23:59:59') as timestamp)  to_dt,
      course
  from (
      select
             m.id,
             m.c_cur_short,
             r.c_date_recount,
             lead(cast(date_sub(c_date_recount,1) as timestamp),1,cast(date'4000-01-01'as timestamp)) over (partition by m.id order by c_date_recount) next_date_recount,
             cast(r.c_course as double)/cast(r.c_unit as double) course
        from $Node1t_team_k7m_aux_d_k7m_cur_coursesIN m
        join $Node2t_team_k7m_aux_d_k7m_cur_coursesIN r
          on m.c_jour_recont  = r.collection_id ) x"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cur_coursesOUT")

    logInserted()
    logEnd()
  }
}
