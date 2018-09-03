package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetStep5Class(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_step5IN = s"${DevSchema}.set_factop"
    val Node2t_team_k7m_aux_d_step5IN = s"${DevSchema}.set_cred_filtered"
    val Node1t_team_k7m_aux_d_step5OUT = s"${DevSchema}.set_paym"
    val dashboardPath1 = s"${config.auxPath}set_paym"
    val Node2t_team_k7m_aux_d_step5OUT = s"${DevSchema}.set_give"
    val dashboardPath2 = s"${config.auxPath}set_give"
    val Node3t_team_k7m_aux_d_step5OUT = s"${DevSchema}.set_wro"
    val dashboardPath3 = s"${config.auxPath}set_wro"
    val Node4t_team_k7m_aux_d_step5OUT = s"${DevSchema}.set_cred_active"
    val dashboardPath4 = s"${config.auxPath}set_cred_active"


    override val dashboardName: String = Node1t_team_k7m_aux_d_step5OUT //витрина
    override def processName: String = "SET"

    def DoSetStep5() {

      Logger.getLogger("Step5").setLevel(Level.WARN)
      //STEP 5 Проведем отсев проводок и отсеем договора так, чтобы существовали транзакции выдачи или гашения по ОД

      //отберем транзакции гашения
      logStart()


      val createHiveTable1 = spark.sql(s"""
select
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id
from
     $Node1t_team_k7m_aux_d_step5IN a
inner join
     $Node2t_team_k7m_aux_d_step5IN as b
     on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Гашение'
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath1)
        .saveAsTable(s"$Node1t_team_k7m_aux_d_step5OUT")

      //SET 5-2 отберем транзакции выдачи
      val createHiveTable2 = spark.sql(s"""
select
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id
from
     $Node1t_team_k7m_aux_d_step5IN a
inner join
     $Node2t_team_k7m_aux_d_step5IN as b
    on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Выдача'
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath2)
        .saveAsTable(s"$Node2t_team_k7m_aux_d_step5OUT")

      //отберем транзакции списания
      val createHiveTable3 = spark.sql(s"""
select
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id
  from $Node1t_team_k7m_aux_d_step5IN a
  join
     $Node2t_team_k7m_aux_d_step5IN as b
     on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Списание'""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath3)
        .saveAsTable(s"$Node3t_team_k7m_aux_d_step5OUT")

      // отфильтруем договора по условию того, что у них есть хотя-бы одна операция выдачи или погашения
      val createHiveTable4 = spark.sql(s"""
select A.*
from $Node2t_team_k7m_aux_d_step5IN as A
    inner join (
        select distinct pr_cred_id
        from(
            select pr_cred_id
            from $Node1t_team_k7m_aux_d_step5OUT
            union all select pr_cred_id
            from $Node2t_team_k7m_aux_d_step5OUT) x
            ) as D
    on A.pr_cred_id = D.pr_cred_id""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath4)
        .saveAsTable(s"$Node4t_team_k7m_aux_d_step5OUT")

      logInserted()
      logEnd()
    }
}

