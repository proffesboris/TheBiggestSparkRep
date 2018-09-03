package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_setIN = s"${DevSchema}.set_set_item"
    val Node2t_team_k7m_aux_d_setIN = s"${MartSchema}.clu"
    val Nodet_team_k7m_aux_d_setOUT = s"${DevSchema}.set"
    val dashboardPath = s"${config.auxPath}set"


    override val dashboardName: String = Nodet_team_k7m_aux_d_setOUT //витрина
    override def processName: String = "SET"

    def DoSet() {

      Logger.getLogger(Nodet_team_k7m_aux_d_setOUT).setLevel(Level.WARN)

      val Nodet_team_k7m_aux_d_setOUTEmpty = s"${DevSchema}.set_empty"
      val dashboardPathEmpty = s"${config.auxPath}set_empty"

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
select
    cast(row_number() over (order by u7m_b_id,instrument) as string) set_id,
    u7m_b_id,
    instrument,
    max_borrower_rating,
    '$execId' exec_id --Получить из системы логирования
from (
select
    u7m_id as u7m_b_id,
    instrument as instrument,
    case when
            sum(
            case
            when set_item_status = 'Ok' then 1
            else 0
           end) =0
        and instrument = 'КД/НКЛ/ВКЛ'
        then cast(14 as int)
        else cast(null as int)
    end max_borrower_rating,
    case
    when
        max(
        case
            when set_item_status = 'Block' then 1
            else 0
           end) =1 then 'Block'
        else 'Ok'
     end set_status
    from $Node1t_team_k7m_aux_d_setIN
    group by u7m_id ,instrument
    ) x
    where x.set_status = 'Ok' """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_setOUT")

      val readHiveTable2 = spark.sql(s"""
select
    concat('ZO',c.u7m_id) as set_id,
    'ОВЕР' instrument,
    c.u7m_id as  u7m_b_id
from  $Node2t_team_k7m_aux_d_setIN c
    left join
          $Node1t_team_k7m_aux_d_setIN as s
    on c.u7m_id = s.u7m_id
        and instrument ='ОВЕР'
where c.flag_basis_client = 'Y'
    and s.u7m_id is null

          union all

select
    concat('ZK',c.u7m_id) as set_id,
    'КД/НКЛ/ВКЛ' instrument,
    c.u7m_id as  u7m_b_id
from  $Node2t_team_k7m_aux_d_setIN c
    left join
          $Node1t_team_k7m_aux_d_setIN as s
    on c.u7m_id = s.u7m_id
        and instrument ='КД/НКЛ/ВКЛ'
where c.flag_basis_client = 'Y'
    and s.u7m_id is null
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPathEmpty)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_setOUTEmpty")

      val readHiveTable3 = spark.sql(s"""
select
    set_id,
    u7m_b_id,
    instrument,
    case when instrument='ОВЕР' then cast(null as int ) else cast(14 as int ) end  max_borrower_rating,
    cast(null as int) exec_id
  from $Nodet_team_k7m_aux_d_setOUTEmpty
        """)
        .write
        .format("parquet")
        .insertInto(s"$Nodet_team_k7m_aux_d_setOUT")

      logInserted()
      logEnd()
    }
}

