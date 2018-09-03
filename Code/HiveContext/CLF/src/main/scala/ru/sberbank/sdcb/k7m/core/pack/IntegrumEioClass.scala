package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class IntegrumEioClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  val SchemaAuxP = "t_team_k7m_aux_p"

  val Node1t_team_k7m_aux_d_K7M_INTEGRUM_EIOIN = s"${Stg0Schema}.int_executive"
  val Node2t_team_k7m_aux_d_K7M_INTEGRUM_EIOIN = s"${Stg0Schema}.int_board_of_directors"
   val Nodet_team_k7m_aux_d_K7M_INTEGRUM_EIOOUT = s"${DevSchema}.K7M_INTEGRUM_EIO"
  val dashboardPath = s"${config.auxPath}K7M_INTEGRUM_EIO"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_INTEGRUM_EIOOUT //витрина
  override def processName: String = "K7M_INTEGRUM_EIO"

  def DoIntegrumEio() {

    Logger.getLogger("org").setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select
         row_number() over (order by ul_inn)  loc_id --Добавить уникальный loc_id = UL_0123456789
         ,ul_inn
         , fio,
         max(birth_dt) birth_dt,
         'EIO_INTEGRUM'  as crit
         from
         (
             select distinct lower(trim(e_ul_inn)) ul_inn, lower(trim(e_fio)) fio ,lower(trim(e_birth_dt)) birth_dt
             from
             (
                 select distinct t1.e_ul_inn, t1.e_fio, t1.e_birth_dt
                 from
                 (
                     select  e_ul_inn, e_fio, e_birth_dt
                     from $Node1t_team_k7m_aux_d_K7M_INTEGRUM_EIOIN
                     where lower(e_post_nm) like '%генеральн%' and  lower(e_post_nm)  like '%директор%'

            union all

            select bd_ul_inn, bd_fio, bd_birth_dt
                     from $Node2t_team_k7m_aux_d_K7M_INTEGRUM_EIOIN
                     where bd_post_nm ='Единоличный исполнительный орган'
                 ) t1
             ) t
         ) t
         group by ul_inn, fio
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_INTEGRUM_EIOOUT")

    logInserted()
    logEnd()
  }
}

