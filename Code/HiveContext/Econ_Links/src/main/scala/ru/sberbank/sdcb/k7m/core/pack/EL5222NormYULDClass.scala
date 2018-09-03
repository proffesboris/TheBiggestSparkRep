package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5222NormYULDClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN = s"${DevSchema}.k7m_OFR_final"
  val Node3t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT = s"${DevSchema}.k7m_EL_5222_Norm_YULD"
  val dashboardPath = s"${config.auxPath}k7m_EL_5222_Norm_YULD"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT //витрина
  override def processName: String = "EL"

  def DoEL5222NormYULD() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT).setLevel(Level.WARN)

    logStart()


    val Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDPREP = Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT.concat("_prep")

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
  max(cast(ofr.revenue as double))  base_amt ,
  suM(cast(bprov.c_sum_nt as double)) abs_amt
    from     (
      select distinct org_inn_crm_num org_inn from $Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN
        where org_inn_crm_num is not null) b
  join  $Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN ofr
    on b.org_inn =  ofr.ul_inn
  join     $Node3t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN  bprov
    on bprov.inn_st=b.org_inn
  where bprov.ktdt=0 --Клиенты базиса в Кредите
    and   c_date_prov  between  ofr.from_dt and ofr.to_dt
  and bprov.predicted_value in (${SparkMainClass.includeYpred5222})
  group by b.org_inn ) x
  where base_amt>0 and abs_amt>0 and abs_amt<=base_amt""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT")

    val createHiveTableStage2 = spark.sql(
      s"""
  select
  org_inn ,
  normk,
  abs_amt,
  base_amt,
  source
  from $Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_prep"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDPREP")

    val createHiveTableStage3 = spark.sql(
      s"""select
  b.org_inn ,
  cast(null as double) normk,
  cast(null as double) abs_amt,
  case when ofr.revenue<${SparkMainClass.amtDownLimit5222} then 0 else ofr.revenue end base_amt, -- 2018-04-06 Коррекция Никиты. Дезавуируем отчетность менее млн. как ничтожную
  ofr.source
  from     (
    select distinct org_inn_crm_num org_inn from $Node1t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN
      where org_inn_crm_num is not null) b
  join  $Node2t_team_k7m_aux_d_k7m_EL_5222_Norm_YULDIN ofr
    on b.org_inn =  ofr.ul_inn
  left join $Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDPREP y
    on ofr.ul_inn = y.org_inn
  where ofr.ul_inn is null""")
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_Norm_YULDOUT")

    logInserted()
    logEnd()
  }
}
