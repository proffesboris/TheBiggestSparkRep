package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Date
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}


class TCredPcClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_t_cred_pcIN = s"${Stg0Schema}.eks_z_product"
    val Node2t_team_k7m_aux_t_cred_pcIN = s"${Stg0Schema}.eks_z_pr_cred"
    val Node3t_team_k7m_aux_t_cred_pcIN = s"${DevSchema}.k7m_cur_courses"
    val Node4t_team_k7m_aux_t_cred_pcIN = s"${Stg0Schema}.eks_z_ft_money"
    val Nodet_team_k7m_aux_t_cred_pcOUT = s"${DevSchema}.t_cred_pc"
    val dashboardPath = s"${config.auxPath}t_cred_pc"


    override val dashboardName: String = Nodet_team_k7m_aux_t_cred_pcOUT //витрина
    override def processName: String = "SET"

    def DoTCredPc() {

      Logger.getLogger(Nodet_team_k7m_aux_t_cred_pcOUT).setLevel(Level.WARN)

      logStart()


      val readHiveTable1 = spark.sql(s"""
    select
        pc.*,
        crs.course issue_course,
        crs.c_cur_short as c_cur_credit,
        ml.c_cur_short c_cur_limit
    from (
        select
            p.class_id,
            p.id,
            c.c_properties ,
            c.C_CLIENT ,
            p.C_COM_STATUS ,
            c.C_DEPART,
            c.C_KIND_CREDIT ,
            c.c_dic_obj_cred ,
            c.C_FILIAL,
            c.c_objects_cred,
            c.C_FT_LIMIT ,
            c.C_FT_CREDIT,
            p.c_date_begin,
            p.c_date_begining,
            c.c_high_level_cr,
            c.collection_id,
            p.C_DATE_ENDING,
            p.c_num_dog ,
            p.C_date_close,
            c.c_summa_dog,
            c.c_limit_saldo,
            c.C_DATE_PAYOUT_LTD,
            c.c_list_pay
        from $Node1t_team_k7m_aux_t_cred_pcIN p
        join $Node2t_team_k7m_aux_t_cred_pcIN AS c
          on c.ID =p.ID
         ) pc
        join $Node3t_team_k7m_aux_t_cred_pcIN crs  -- L4_5 Считается для эк.связей
          on crs.id = pc.C_FT_CREDIT
   left join $Node4t_team_k7m_aux_t_cred_pcIN ml
          on pc.C_FT_LIMIT = ml.ID
  where coalesce(pc.c_date_begin,pc.c_date_begining)  >= crs.from_dt
    and coalesce(pc.c_date_begin,pc.c_date_begining)  <= crs.to_dt
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_t_cred_pcOUT")

      logInserted()
      logEnd()
    }
}

