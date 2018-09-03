package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5222NormYULKClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN = s"${DevSchema}.k7m_OFR_final"
  val Node3t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT = s"${DevSchema}.k7m_EL_5222_Norm_YULK"
  val dashboardPath = s"${config.auxPath}k7m_EL_5222_Norm_YULK"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT //витрина
  override def processName: String = "EL"

  def DoEL5222NormYULK() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT).setLevel(Level.WARN)

    logStart()


    val Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKPREP = Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT.concat("_prep")

    val createHiveTableStage1 = spark.sql(
      s"""select
  org_inn ,
  abs_amt/base_amt normk,
  case when abs_amt/base_amt between ${SparkMainClass.amtFrom5222} and ${SparkMainClass.amtTo5222} then abs_amt else 0 end  abs_amt, ---2018-04-06 Коррекция Никиты. Если меньше  0.1 (CUTOFF_LOW) - это значит клиент через нас и не работает, если >3(CUTOFF_HIGH) это какая-то ненужная фигня наподобие УК.
  case when base_amt<${SparkMainClass.amtDownLimit5222} then 0 else base_amt end base_amt, ---2018-04-06 Коррекция Никиты. Дезавуируем отчетность менее млн. как ничтожную
  source
  from (
    select
      b.org_inn ,
  max(ofr.source) source,
  max(cast(ofr.total_expenses as double))  base_amt ,
  suM(cast(bprov.c_sum_nt as double)) abs_amt
    from     (
      select distinct org_inn_crm_num org_inn from $Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN
        where org_inn_crm_num is not null) b
  join  $Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN ofr
    on b.org_inn =  ofr.ul_inn
  join    $Node3t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN  bprov  --t_team_cynomys.yia_transact_out_2019_uf
  on bprov.inn_st=b.org_inn
  where bprov.ktdt=1 --Клиенты базиса в Дебете
    and   c_date_prov  between  ofr.from_dt and ofr.to_dt
  and bprov.predicted_value in (${SparkMainClass.includeYpred5222})
  group by b.org_inn ) x
  where base_amt>0 and abs_amt>0 and abs_amt<=base_amt""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT")

    val createHiveTableStage2 = spark.sql(
      s"""
  select
  org_inn ,
  normk,
  abs_amt,
  base_amt,
  source
  from $Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT""")
       .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_prep"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKPREP")

    val createHiveTableStage3 = spark.sql(
      s"""select
  b.org_inn ,
  cast(null as double) normk,
  cast(null as double) abs_amt,
  case when ofr.total_expenses<${SparkMainClass.amtDownLimit5222} then 0 else ofr.total_expenses end base_amt, ---2018-04-06 Коррекция Никиты. Дезавуируем отчетность менее млн. как ничтожную
  ofr.source
  from     (
    select distinct org_inn_crm_num org_inn from $Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN
      where org_inn_crm_num is not null) b
  join  $Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULKIN ofr
    on b.org_inn =  ofr.ul_inn
  left join $Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKPREP y
    on ofr.ul_inn = y.org_inn
  where ofr.ul_inn is null"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULKOUT")

    logInserted()
    logEnd()
  }
}
