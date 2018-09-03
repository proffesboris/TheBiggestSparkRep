package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5211Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5211IN = s"${DevSchema}.k7m_zalog_dispensed"
  val Node2t_team_k7m_aux_d_k7m_EL_5211IN = s"${Stg0Schema}.eks_Z_CLIENT"
  val Node3t_team_k7m_aux_d_k7m_EL_5211IN = s"${DevSchema}.basis_client"
  val Node4t_team_k7m_aux_d_k7m_EL_5211IN = s"${DevSchema}.k7m_rep1600_final"
  val Nodet_team_k7m_aux_d_k7m_EL_5211OUT = s"${DevSchema}.k7m_EL_5211"
  val dashboardPath = s"${config.auxPath}k7m_EL_5211"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5211OUT //витрина
  override def processName: String = "EL"

  def DoEL5211(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5211OUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  cast (date'${dateString}' as timestamp) report_dt,
  zalog_class_id from_cid,
  zalog_inn  from_inn,
  cred_class_id to_cid,
  cred_inn to_inn,
  concat('5.2.1.1',(case when ta1600 is not  null and dispensed_sum/ta1600<${SparkMainClass.qnt5211} then '_g' else '' end)) crit_code,
  case when ta1600 is null then 10 else 100 end probability,
  dispensed_sum/ta1600 quantity,
  source,
  ta1600 base_amt,
  dispensed_sum absolute_value_amt
    from (
      select
        cc.class_id cred_class_id,
  cc.c_inn cred_inn,
  cz.class_id zalog_class_id,
  cz.c_inn zalog_inn,
  max(f.ta1600) ta1600,
  max(f.source) source,
  sum(d.final_dispensed_sum) dispensed_sum
    from $Node1t_team_k7m_aux_d_k7m_EL_5211IN   d
    join
  $Node2t_team_k7m_aux_d_k7m_EL_5211IN    cc
    on cc.id=d.cred_client_id
  join 	 (
    select distinct org_inn_crm_num org_inn from $Node3t_team_k7m_aux_d_k7m_EL_5211IN
      where org_inn_crm_num is not null) b
  on b.org_inn =  cc.c_inn
  join
  $Node2t_team_k7m_aux_d_k7m_EL_5211IN    cz
    on cz.id=d.zalog_client_id
  left join
    (select
      ul_inn inn,ta1600,source
      from
      $Node4t_team_k7m_aux_d_k7m_EL_5211IN
      where dt>=add_months(date'${dateString}',-5*3) and source ='F'
         or dt>=add_months(date'${dateString}',-24*2) and source ='I'
  )    f
  on cz.c_inn  = f.inn
  group by
    cc.class_id,
  cc.c_inn,
  cz.class_id,
  cz.c_inn
  ) x
  where zalog_inn <> cred_inn
    and zalog_class_id <>'${SparkMainClass.excludeClassId}'
         """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5211OUT")

    logInserted()
    logEnd()
  }

}
