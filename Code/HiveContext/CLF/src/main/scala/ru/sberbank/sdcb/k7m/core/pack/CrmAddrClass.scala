package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmAddrClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CRM_ADDRIN = s"${Stg0Schema}.crm_s_con_addr"
  val Node2t_team_k7m_aux_d_K7M_CRM_ADDRIN = s"${Stg0Schema}.crm_s_addr_per"
  val Nodet_team_k7m_aux_d_K7M_CRM_ADDROUT = s"${DevSchema}.K7M_CRM_ADDR"
  val dashboardPath = s"${config.auxPath}K7M_CRM_ADDR"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_ADDROUT //витрина
  override def processName: String = "CLF"

  def DoCrmAddr() {

    Logger.getLogger("org").setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""select   accnt_id
                , contact_id
           , max(case when ca.addr_type_cd='Юридический' then a.addr else null end) as jur_addr
           , max(case when ca.addr_type_cd='Фактический' then a.addr else null end) as fact_addr
         from
          (select addr_per_id,  accnt_id, contact_id, addr_type_cd from $Node1t_team_k7m_aux_d_K7M_CRM_ADDRIN
                  where active_flg = 'Y'
                  and addr_type_cd in ('Юридический','Фактический') --Подтвердить?!
                  ) ca
         ,(select row_id as addr_id , addr  from $Node2t_team_k7m_aux_d_K7M_CRM_ADDRIN) a
         where a.addr_id = ca.addr_per_id
         group by accnt_id, contact_id
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_ADDROUT")

    logInserted()
  }
}



