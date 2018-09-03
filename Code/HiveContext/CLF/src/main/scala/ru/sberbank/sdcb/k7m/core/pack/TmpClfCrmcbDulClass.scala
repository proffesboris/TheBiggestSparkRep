package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfCrmcbDulClass  (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_crmcb_dulIN = s"${DevSchema}.tmp_clf_crmcb_filter"
  val Node2t_team_k7m_aux_d_tmp_clf_crmcb_dulIN = s"${Stg0Schema}.crm_cx_reg_doc"
   val Nodet_team_k7m_aux_d_tmp_clf_crmcb_dulOUT = s"${DevSchema}.tmp_clf_crmcb_dul"
  val dashboardPath = s"${config.auxPath}tmp_clf_crmcb_dul"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_crmcb_dulOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfCrmcbDul()
{
  Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_crmcb_dulOUT).setLevel(Level.WARN)

  val smartSrcHiveTable_t7 = spark.sql(
  s"""			select cont_id,type,series,doc_number,registrator,issue_dt,reg_code,end_date,status,row_id
     	from
     		(select
     			  t.obj_id cont_id	--Документ удостоверяющий личность - ID контакта
     			, t.type     		--Документ удостоверяющий личность - Тип ДУЛ
     			, t.series    		--Документ удостоверяющий личность - Серия
     			, t.doc_number    	--Документ удостоверяющий личность - Номер
     			, t.registrator   	--Документ удостоверяющий личность - Кем выдан
     			, t.issue_dt     	--Документ удостоверяющий личность - Дата выдачи
     			, t.reg_code     	--Документ удостоверяющий личность - Код подразделения
     			, t.end_date     	--Документ удостоверяющий личность - Дата окончания срока действия
     			, t.status     		--Документ удостоверяющий личность - Статус ДУЛ
     			, t.row_id     		--Документ удостоверяющий личность - ID ДУЛ
     			, row_number() over(
     				partition by
     					t.obj_id
     				order by
     					case when t.status = 'Действует' then 1 else 2 end
     					,t.issue_dt desc
     					,t.created desc
     				) rn
     		from
     			$Node1t_team_k7m_aux_d_tmp_clf_crmcb_dulIN filter
     			join $Node2t_team_k7m_aux_d_tmp_clf_crmcb_dulIN t	on filter.cont_id = t.obj_id
     		where
     			t.type='Паспорт гражданина РФ'
     		) tt
     where rn = 1
    """
  )
  smartSrcHiveTable_t7
  .write.format("parquet")
  .mode(SaveMode.Overwrite)
  .option("path", dashboardPath)
  .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_crmcb_dulOUT")

  logInserted()

}
}

