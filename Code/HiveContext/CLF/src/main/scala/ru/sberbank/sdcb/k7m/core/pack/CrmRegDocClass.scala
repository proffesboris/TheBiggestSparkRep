package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmRegDocClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CRM_reg_docIN = s"${Stg0Schema}.crm_CX_REG_DOC"
   val Nodet_team_k7m_aux_d_K7M_CRM_reg_docOUT = s"${DevSchema}.K7M_CRM_reg_doc"
  val dashboardPath = s"${config.auxPath}K7M_CRM_reg_doc"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_reg_docOUT //витрина
  override def processName: String = "CLF"

  def DoCrmRegDoc() {

    Logger.getLogger("org").setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select
           obj_id as contact_id
         , type
         , series
         , doc_number
         , registrator
         , issue_dt
         , birth_place
         , reg_code
         , end_date
 from $Node1t_team_k7m_aux_d_K7M_CRM_reg_docIN crd
 where status = 'Действует'
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_reg_docOUT")

    logInserted()
    logEnd()
  }
}




