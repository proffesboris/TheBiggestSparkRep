package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5222PrepClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5222_prepIN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_k7m_EL_5222_prepIN = s"${DevSchema}.k7m_EL_5222_12m"
  val Node3t_team_k7m_aux_d_k7m_EL_5222_prepIN = s"${DevSchema}.k7m_EL_5222_Norm_YULK"
  val Node4t_team_k7m_aux_d_k7m_EL_5222_prepIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Node5t_team_k7m_aux_d_k7m_EL_5222_prepIN = s"${DevSchema}.k7m_EL_5222_Norm_YULD"
  val Nodet_team_k7m_aux_d_k7m_EL_5222_prepOUT = s"${DevSchema}.k7m_EL_5222_prep"
  val dashboardPath = s"${config.auxPath}k7m_EL_5222_prep"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5222_prepOUT //витрина
  override def processName: String = "EL"

  def DoEL5222Prep(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5222_prepOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  'YULK' typ_contr,
  bprov.inn_sec from_inn,
  b.org_inn to_inn,
  max(b.norm_base)  base_amt,
  max(b.norm_case)  base_case,
  suM(cast(bprov.c_sum_nt as double)) abs_amt
    from     (
      select
        distinct
        i.org_inn_crm_num org_inn,
  case
    when coalesce(k.base_amt,0) < 1 and m.s12m_amt> ${SparkMainClass.amtDownLimit5222}   then    m.s12m_amt  -- 2018-04-06 Коррекция Никиты.  Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= ${SparkMainClass.amtDownLimit5222} then 0 -- 2018-04-06 Коррекция Никиты. Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then k.base_amt
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between ${SparkMainClass.lowLimitRate5222} and ${SparkMainClass.highLimitRate5222} then m.s12m_amt*(k.base_amt/ k.abs_amt)  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <${SparkMainClass.lowLimit2Rate5222} or m.s12m_amt/k.abs_amt >  ${SparkMainClass.highLimit2Rate5222}) then k.base_amt  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>${SparkMainClass.highLimitRate5222} then  ${SparkMainClass.highLimitRate5222}* k.base_amt -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<${SparkMainClass.lowLimitRate5222} and m.s12m_amt/k.abs_amt > 0 then ${SparkMainClass.lowLimitRate5222} * k.base_amt -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < ${SparkMainClass.microLimitRate5222} then k.base_amt
    else   m.s12m_amt
  end    norm_base,
  case
    when coalesce(k.base_amt,0) < 1 and m.s12m_amt> ${SparkMainClass.amtDownLimit5222}   then    'case1'  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= ${SparkMainClass.amtDownLimit5222} then 'case2' -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then 'case3'
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between ${SparkMainClass.lowLimitRate5222} and ${SparkMainClass.highLimitRate5222} then 'case4'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <${SparkMainClass.lowLimit2Rate5222} or m.s12m_amt/k.abs_amt >  ${SparkMainClass.highLimit2Rate5222}) then 'case5'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>${SparkMainClass.highLimitRate5222} then  'case6' -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<${SparkMainClass.lowLimitRate5222} and m.s12m_amt/k.abs_amt > 0 then 'case7' -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < ${SparkMainClass.microLimitRate5222} then 'case8'
  else   'error'
  end    norm_case
    from $Node1t_team_k7m_aux_d_k7m_EL_5222_prepIN  i
    join $Node2t_team_k7m_aux_d_k7m_EL_5222_prepIN m
    on i.org_inn_crm_num = m.org_inn
  left join $Node3t_team_k7m_aux_d_k7m_EL_5222_prepIN k
    on i.org_inn_crm_num = k.org_inn
  where i.org_inn_crm_num is not null
  and m.typ_contr ='YULK'
  ) b
  join    $Node4t_team_k7m_aux_d_k7m_EL_5222_prepIN bprov
    on bprov.inn_st=b.org_inn
  where bprov.ktdt=1 --Клиенты базиса в Дебете
    and   c_date_prov  between  cast( add_months(date'${dateString}',-12) as string) and '${dateString}'
  and bprov.inn_sec!=bprov.inn_st
  and bprov.predicted_value in (${SparkMainClass.includeYpred5222})
  group by bprov.inn_sec ,
  b.org_inn """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_prepOUT")

    logInserted()

    val createHiveTableStage2 = spark.sql(s""" select
  'YULD' typ_contr,
  bprov.inn_sec from_inn,
  b.org_inn to_inn,
  max(b.norm_base)  base_amt,
  max(b.norm_case)  base_case,
  suM(cast(bprov.c_sum_nt as double)) abs_amt
    from     (
      select
        distinct
        i.org_inn_crm_num org_inn,
  case
    when coalesce(k.base_amt,0) < 1 and m.s12m_amt> ${SparkMainClass.amtDownLimit5222}   then    m.s12m_amt  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= ${SparkMainClass.amtDownLimit5222} then 0 -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then k.base_amt
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between ${SparkMainClass.lowLimitRate5222} and ${SparkMainClass.highLimitRate5222} then m.s12m_amt*(k.base_amt/ k.abs_amt)  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <${SparkMainClass.lowLimit2Rate5222} or m.s12m_amt/k.abs_amt >  ${SparkMainClass.highLimit2Rate5222}) then k.base_amt  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>${SparkMainClass.highLimitRate5222} then  ${SparkMainClass.highLimitRate5222}* k.base_amt -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<${SparkMainClass.lowLimitRate5222} and m.s12m_amt/k.abs_amt > 0 then ${SparkMainClass.lowLimitRate5222} * k.base_amt -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < ${SparkMainClass.microLimitRate5222} then k.base_amt
    else   m.s12m_amt
  end    norm_base,
  case
    when coalesce(k.base_amt,0) < 1 and m.s12m_amt> ${SparkMainClass.amtDownLimit5222}   then    'case1'  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= ${SparkMainClass.amtDownLimit5222} then 'case2' -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
    when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then 'case3'
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between ${SparkMainClass.lowLimitRate5222} and ${SparkMainClass.highLimitRate5222} then 'case4'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <${SparkMainClass.lowLimit2Rate5222} or m.s12m_amt/k.abs_amt >  ${SparkMainClass.highLimit2Rate5222}) then 'case5'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>${SparkMainClass.highLimitRate5222} then  'case6' -- 2018-04-06 Коррекция Никиты. ${SparkMainClass.highLimitRate5222} константа  high_limit_rate
    when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<${SparkMainClass.lowLimitRate5222} and m.s12m_amt/k.abs_amt > 0 then 'case7' -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
    when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < ${SparkMainClass.microLimitRate5222} then 'case8'
    else   'error'
  end    norm_case
    from $Node1t_team_k7m_aux_d_k7m_EL_5222_prepIN  i
    join $Node2t_team_k7m_aux_d_k7m_EL_5222_prepIN m
    on i.org_inn_crm_num = m.org_inn
  left join $Node5t_team_k7m_aux_d_k7m_EL_5222_prepIN k
    on i.org_inn_crm_num = k.org_inn
  where i.org_inn_crm_num is not null
  and m.typ_contr ='YULD'
  ) b
  join    $Node4t_team_k7m_aux_d_k7m_EL_5222_prepIN bprov
    on bprov.inn_st=b.org_inn
  where bprov.ktdt=0 --Клиенты базиса в Кредите
    and  bprov.c_date_prov  between  cast( add_months(date'${dateString}',-12) as string) and '${dateString}'
  and bprov.inn_sec!=bprov.inn_st
  and bprov.predicted_value in (${SparkMainClass.includeYpred5222})
  group by bprov.inn_sec ,
  b.org_inn"""
    ) .write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222_prepOUT")

    logInserted()
    logEnd()
  }
}
