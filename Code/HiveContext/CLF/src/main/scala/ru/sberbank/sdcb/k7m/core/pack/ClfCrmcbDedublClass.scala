package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfCrmcbDedublClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_crmcb_dedublIN = s"${DevSchema}.clf_crmcb"
   val Nodet_team_k7m_aux_d_tmp_clf_crmcb_dedublOUT = s"${DevSchema}.tmp_clf_crmcb_dedubl"
  val dashboardPath = s"${config.auxPath}tmp_clf_crmcb_dedubl"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_crmcb_dedublOUT //витрина
  override def processName: String = "CLF"

  def DoClfCrmcbDedubl()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_crmcb_dedublOUT).setLevel(Level.WARN)
   //-------------------------
   //--Ранжирование по ключам
   //-------------------------
    val smartSrcHiveTable_t7 = spark.sql(
      s"""	SELECT
          t.crm_id
         ,t.full_name
         ,t.clf_l_name
         ,t.clf_f_name
         ,t.clf_m_name
         ,t.id_series_num
         ,t.id_series
         ,t.id_num
         ,t.registrator
         ,t.reg_code
         ,t.id_date
         ,t.birth_date
         ,t.job
         ,t.id_end_date
         ,t.gender_tp_code
         ,t.tel_mob
         ,t.email
         ,t.last_update_dt
         ,t.full_name_clear
         ,t.id_series_num_clear
         ,t.birth_date_clear
         ,t.tel_mob_clear
         	-- Ключ 1: ФИО + ДР +ДУЛ
         	, case when id_series_num_clear is not null and birth_date_clear is not null then    ----добавил
         	  row_number() over(
         	partition by
         		  full_name_clear
         		, birth_date_clear      ----добавил
         		, id_series_num_clear
         	order by
              id_date
         		, case when nvl(trim(job), '') <> '' then 1 else 2 end	-- сначала ФЛ с должностью
         		, last_update_dt desc
         	) end rn_id
         -- Ключ 2: ФИО + др + телефон
         , case when tel_mob_clear is not null and birth_date_clear is not null then
           row_number() over(
         	partition by
         		  full_name_clear
         		, birth_date_clear
         		, tel_mob_clear
         	order by
         		  case when nvl(trim(job), '') <> '' then 1 else 2 end	-- сначала ФЛ с должностью
         		, last_update_dt desc
         	) end rn_tel
         from
         	$Node1t_team_k7m_aux_d_tmp_clf_crmcb_dedublIN t
         where
         	full_name_clear is not null
         	and
           	((id_series_num_clear is not null
         			and birth_date_clear is not null)     ----добавил
         		or (
         			tel_mob_clear is not null
         			and birth_date_clear is not null
         			)
         		)
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_crmcb_dedublOUT")

    logInserted()

  }
}




