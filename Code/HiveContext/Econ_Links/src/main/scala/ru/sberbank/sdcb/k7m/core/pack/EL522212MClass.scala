package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL522212MClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa



  val Node1t_team_k7m_aux_d_k7m_EL_5222_12mIN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_k7m_EL_5222_12mIN = s"${DevSchema}.k7m_OFR_final"
  val Node3t_team_k7m_aux_d_k7m_EL_5222_12mIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Nodet_team_k7m_aux_d_k7m_EL_5222_12mOUT = s"${DevSchema}.k7m_EL_5222_12m"
  val dashboardPath = s"${config.auxPath}k7m_EL_5222_12m"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5222_12mOUT //витрина
  override def processName: String = "EL"

  def DoEL522212M(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5222_12mOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  case
  when x.ktdt=0 --Клиенты базиса в Кредите
    then 'YULD' else 'YULK'
  end typ_contr,
  x.ktdt,
  x.org_inn ,
  x.s12m_amt
  from (
    select
      --  case
  --    when bprov.ktdt=0 --Клиенты базиса в Кредите
    --then 'YULD' else 'YULK'
  --end c_tp,
  bprov.ktdt,
  b.org_inn ,
  suM(cast(bprov.c_sum_nt as double)) s12m_amt
    from     (
      select distinct org_inn_crm_num org_inn from $Node1t_team_k7m_aux_d_k7m_EL_5222_12mIN
        where org_inn_crm_num is not null) b
  join  $Node2t_team_k7m_aux_d_k7m_EL_5222_12mIN ofr
    on b.org_inn =  ofr.ul_inn
  join     $Node3t_team_k7m_aux_d_k7m_EL_5222_12mIN bprov
    on bprov.inn_st=b.org_inn
  where c_date_prov  between  cast( add_months(date'${dateString}',-12) as string) and '${dateString}'
  and bprov.predicted_value in (${SparkMainClass.includeYpred5222})
  and cast(bprov.c_sum_nt as double)>0
  group by
    bprov.ktdt,
  b.org_inn) x""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_12mOUT")

    logInserted()
    logEnd()
  }
}
