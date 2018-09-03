package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class HoaDtClass (val spark: SparkSession, val config: Config, val date: String) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_hoa_dtIN = s"${DevSchema}.k7m_prd_dt"
  val Node2t_team_k7m_aux_d_k7m_hoa_dtIN = s"${Stg0Schema}.eks_z_hoz_op_acc"
  val Node3t_team_k7m_aux_d_k7m_hoa_dtIN = s"${Stg0Schema}.eks_z_tip_acc"
   val Nodet_team_k7m_aux_d_k7m_hoa_dtOUT = s"${DevSchema}.k7m_hoa_dt"
  val dashboardPath = s"${config.auxPath}k7m_hoa_dt"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_hoa_dtOUT //витрина
  override def processName: String = "Plan_liab"

  def DoHoaDt()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_hoa_dtOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s""" select  collection_id
                  ,c_name_account
                  ,c_date_beg
                  ,c_date_end
                  ,c_account_dog_1_2
          from (
           select
               hoa.collection_id
              , hoa.c_name_account
              , coalesce(hoa.c_date_beg, cast('1900-01-01' as timestamp)) c_date_beg
              , cast(lead(date_sub(hoa.c_date_beg,1),1,cast('9999-12-31' as timestamp)) over (partition by t.id, hoa.c_name_account order by hoa.c_date_beg ) as timestamp) as  c_date_end
              , hoa.c_account_dog_1_2
                from $Node1t_team_k7m_aux_d_k7m_hoa_dtIN t
                join ( select hoa.collection_id, hoa.c_name_account, hoa.c_date_beg, hoa.c_account_dog_1_2
                                from $Node2t_team_k7m_aux_d_k7m_hoa_dtIN hoa
                                inner join $Node3t_team_k7m_aux_d_k7m_hoa_dtIN ta on hoa.c_name_account = ta.id
                                where c_cod in ('ACCOUNT', 'VNB_UNUSED_LINE') -- Ссудный счет и Неиспользованный лимит по кредитной линии или кредитов в форме "овердрафт"
                     ) hoa on hoa.collection_id = t.c_array_dog_acc
           where t.class_id <> 'GEN_AGREEM_FRAME'
         ) tt where cast('$date' as timestamp) between c_date_beg and c_date_end
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_hoa_dtOUT")

    logInserted()
    logEnd()
  }
}

