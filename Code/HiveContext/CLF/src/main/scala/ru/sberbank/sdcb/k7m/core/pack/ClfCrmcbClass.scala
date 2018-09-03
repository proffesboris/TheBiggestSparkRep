package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfCrmcbClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_crmcbIN = s"${DevSchema}.tmp_clf_crmcb_clf"
  val Node2t_team_k7m_aux_d_clf_crmcbIN = s"${DevSchema}.tmp_clf_crmcb_dul"
  val Node3t_team_k7m_aux_d_clf_crmcbIN = s"${DevSchema}.tmp_clf_crmcb_positions"
   val Nodet_team_k7m_aux_d_clf_crmcbOUT = s"${DevSchema}.clf_crmcb"
  val dashboardPath = s"${config.auxPath}clf_crmcb"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_crmcbOUT //витрина
  override def processName: String = "CLF"

  def DoClfCrmcb()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_crmcbOUT).setLevel(Level.WARN)

    //------------------
    //---  Витрина CRM
    //-------------------
    val smartSrcHiveTable_t7 = spark.sql(
      s"""	SELECT
         	  clf.cont_id crm_id 			-- a.	Id клиента фл в CRM КБ
         	--, crm_active_flag				-- b.	Флаг активности записи в CRM КБ (Y/N) 	--, crm_status_name				-- наименование статуса в МДМ
         	, trim(concat(clf.last_name, ' ', clf.fst_name, ' ', clf.mid_name)) full_name
         	, clf.last_name clf_l_name
         	, clf.fst_name clf_f_name
         	, clf.mid_name clf_m_name
         	--, status_active_dead			-- c.	Статус клиента (ACTIVE/DEAD)
         	, concat(dul.series, ' ', dul.doc_number) id_series_num  -- d.	ДУЛ (Серия и номер) (если поиск фл осуществлялся по ключу ФИО+ДР+Мобильный телефон или ФИО+ИННфл)
         	, dul.series id_series    		--Документ удостоверяющий личность - Серия
         	, dul.doc_number id_num   	 	--Документ удостоверяющий личность - Номер
         	, dul.registrator   			--Документ удостоверяющий личность - Кем выдан
         	, dul.reg_code     				--Документ удостоверяющий личность - Код подразделения
         	, dul.issue_dt id_date    		--Документ удостоверяющий личность - Дата выдачи
         	--, dul.status     				--Документ удостоверяющий личность - Статус ДУЛ
         	, clf.birth_dt birth_date		-- e.	Дата рождения (если поиск фл осуществлялся по ключу ФИО+ИННфл)
         	, positions.x_job_title job		-- f.	Должность
         	, dul.end_date id_end_date	 	-- g.	Дата истечения ДУЛ
         	, clf.sex_mf gender_tp_code		-- h.	Пол
         	--, citizenship		-- j.	Гражданство   	--, inn		-- k.	ИНН фл  	--, adr_reg	-- l.	Адрес регистрации (список полей),   	--, adr_fact		-- m.	Адрес фактического проживания (список полей),
         	, case
         		when nvl(regexp_replace(clf.cell_ph_num, '[^0-9]', ''), '') <> '' then clf.cell_ph_num
         		else clf.work_ph_num
         		end tel_mob					-- o.	Телефон (мобильный),
         	, clf.email_addr email			-- p.	Адрес электронной почты,
         	--, snils.ref_num snils			-- t.	СНИЛС
         	, clf.db_last_upd last_update_dt
         -- Дальше идут атрибуты, по которым делается матчинг. Для целей матчинга в этих атрибутах производится очистка.
         	, case
         		when regexp_replace(upper(concat(clf.last_name, clf.fst_name, clf.mid_name)),'[^A-ZА-Я]+','') <> ''
         		then trim(regexp_replace(upper(concat(clf.last_name, ' ', clf.fst_name, ' ', clf.mid_name)),'[^A-ZА-Я]+',' '))
         		else null
         		end full_name_clear															-- Полное ФИО для матчинга:
         																					-- В верхнем регистре
         																					-- Только буквы и пробелы (дефисы и прочие знаки заменяются на пробелы)
         																					-- Одиночные пробелы между словами, без пробелов в начале и конце строки
         	, case
         		when length(regexp_replace(concat(dul.series, dul.doc_number) ,'[^0-9]','')) = 10
         		then regexp_replace(concat(dul.series, dul.doc_number) ,'[^0-9]','')
         		else null
         		end id_series_num_clear														-- Серия и номер паспорта для матчинга: только цифры, все пробелы удалены, серия и номер идут подряд без пробела.															-- ИНН для матчинга: только цифры, все пробелы удалены.
         	, case
         		when nvl(date_format(clf.birth_dt, 'yyyy-MM-dd'),'') <> ''
         		then date_format(clf.birth_dt, 'yyyy-MM-dd')
         		else null
         		end birth_date_clear														-- Дата рождения в формате строки '2018-01-01'
         	, case
         		when nvl(regexp_replace(clf.cell_ph_num,'[^0-9]',''),'') <> ''
         		then regexp_replace(clf.cell_ph_num,'[^0-9]','')
         		when nvl(regexp_replace(clf.work_ph_num,'[^0-9]',''),'') <> ''
         		then regexp_replace(clf.work_ph_num,'[^0-9]','')
         		else null
         		end tel_mob_clear															-- Телефон (мобильный)
         FROM
         	$Node1t_team_k7m_aux_d_clf_crmcbIN clf
         	left join $Node2t_team_k7m_aux_d_clf_crmcbIN dul
         		on dul.cont_id = clf.cont_id
         	left join $Node3t_team_k7m_aux_d_clf_crmcbIN positions
         		on positions.cont_id = clf.cont_id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_crmcbOUT")

    logInserted()

  }
}



