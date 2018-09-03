package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7Mr4BasisClientClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN = s"${DevSchema}.sep_bc_link_crit_dual"
    val Node2t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN = s"${MartSchema}.pdeksin2"
    val Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN = s"${MartSchema}.clu"
    val Node1t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT = s"${DevSchema}.sep_cnt_neighbours"
    val dashboardPath1 = s"${config.auxPath}sep_cnt_neighbours"
    val Node2t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT = s"${DevSchema}.SET_Prov_base"
    val dashboardPath2 = s"${config.auxPath}SET_Prov_base"
    val Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT = s"${DevSchema}.sep_surety_7M_r4_basis_client"
    val dashboardPath3 = s"${config.auxPath}sep_surety_7M_r4_basis_client"


    override val dashboardName: String = Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7Mr4BasisClient(dt: String) {

      Logger.getLogger(Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT).setLevel(Level.WARN)

      logStart()
//количество соседей используется в set_set_item
      val readHiveTable1 = spark.sql(s"""
select
    bor_obj,
    bor_u7m_id,
    count(*) cnt_neighbours
from $Node1t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN
group by
    bor_obj,
    bor_u7m_id
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath1)
        .saveAsTable(s"$Node1t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT")

//Подготовка связей по транзакциям
        val readHiveTable2 = spark.sql(s"""
select
tr.c_date_prov,
inn_st,inn_sec,ktdt,
tr.c_sum
from
 $Node2t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN  tr
where   inn_st<>inn_sec
    and tr.ypred in ('оплата по договору')
    and tr.c_sum<>0
    and to_date(tr.c_date_prov)<=DATE'${dt}'--!!!!! Дата расчета K7M!
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath2)
        .saveAsTable(s"$Node2t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT")



        val readHiveTable3 = spark.sql(s"""
select b.u7m_id as bor_u7m_id, tr.inn_sec as rel_inn,
    coalesce(sum(
                  case when months_between(DATE'${dt}',to_date(tr.c_date_prov))<=12 --!!!!! Дата расчета K7M!
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr

    ,coalesce(sum(
                  case when months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 3
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_1q
    ,coalesce(sum(
                  case when  months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 6
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 3
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_2q

    ,coalesce(sum(
                  case when  months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 9
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 6
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_3q

    ,coalesce(sum(
                  case when  months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 12
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 9
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_4q

    ,coalesce(sum(
                  case when months_between(DATE'${dt}',to_date(tr.c_date_prov))<=12
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db


       ,coalesce(sum(
                  case when  months_between(DATE'${dt}',to_date(tr.c_date_prov))<=3
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_1q
       ,coalesce(sum(
                  case when   months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 6
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 3
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_2q
           ,coalesce(sum(
                  case when months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 9
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 6
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_3q
             ,coalesce(sum(
                  case when  months_between(DATE'${dt}',to_date(tr.c_date_prov))<= 12
                          and months_between(DATE'${dt}',to_date(tr.c_date_prov))> 9
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_4q

    ,coalesce(max(
                  case when ktdt=1
                        then months_between(DATE'${dt}',to_date(tr.c_date_prov)) end
                  ),0) as oboroty_db_last_tr_m

    ,coalesce(max(
                  case when  ktdt=0
                        then months_between(DATE'${dt}',to_date(tr.c_date_prov)) end
                  ),0) as oboroty_kr_last_tr_m

    ,coalesce(max(months_between(DATE'${dt}',to_date(tr.c_date_prov))),0) as oboroty_last_tr_m
from (select * from $Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientIN where flag_basis_client = 'Y') as b
left join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT tr
    on tr.inn_st = b.inn

group by b.u7m_id, tr.inn_sec
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath3)
        .saveAsTable(s"$Node3t_team_k7m_aux_d_sep_surety_7M_r4_basis_clientOUT")

      logInserted()
      logEnd()
    }
}

