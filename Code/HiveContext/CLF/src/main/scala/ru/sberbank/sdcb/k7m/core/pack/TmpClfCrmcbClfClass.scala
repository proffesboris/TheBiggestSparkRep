package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfCrmcbClfClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_crmcb_clfIN = s"${DevSchema}.tmp_clf_crmcb_filter"
  val Node2t_team_k7m_aux_d_tmp_clf_crmcb_clfIN = s"${Stg0Schema}.crm_s_contact"
   val Nodet_team_k7m_aux_d_tmp_clf_crmcb_clfOUT = s"${DevSchema}.tmp_clf_crmcb_clf"
  val dashboardPath = s"${config.auxPath}tmp_clf_crmcb_clf"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_crmcb_clfOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfCrmcbClf()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_crmcb_clfOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""		select
         		  t.row_id cont_id	--Контакт (ФЛ) - ID контакта
         		, t.birth_dt     	--Контакт (ФЛ) - Дата рождения
         		, t.created    		--Контакт (ФЛ) - Дата создания
         		, t.last_name     	--Контакт (ФЛ) - Фамилия
         		, t.fst_name   		--Контакт (ФЛ) - Имя
         		, t.mid_name   		--Контакт (ФЛ) - Отчество
         		, t.sex_mf     		--Контакт (ФЛ) - Пол
         		, t.work_ph_num    	--Контакт (ФЛ) - Рабочий телефон
         		, t.cell_ph_num    	--Контакт (ФЛ) - Мобильный телефон
         		, t.fax_ph_num     	--Контакт (ФЛ) - Факс
         		, t.email_addr     	--Контакт (ФЛ) - Email
         		, t.active_flg
         		, t.db_last_upd
         		, t.pr_dept_ou_id	--Основная организация (место работы)
         	from
         		$Node1t_team_k7m_aux_d_tmp_clf_crmcb_clfIN filter
         		join $Node2t_team_k7m_aux_d_tmp_clf_crmcb_clfIN t
         			on filter.cont_id = t.row_id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_crmcb_clfOUT")

    logInserted()

  }
}

