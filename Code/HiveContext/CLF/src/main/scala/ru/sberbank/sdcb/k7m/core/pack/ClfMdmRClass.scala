package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfMdmRClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CLF_MDMIN = s"${DevSchema}.K7M_CLF_MDM_RN"
   val Nodet_team_k7m_aux_d_K7M_CLF_MDMOUT = s"${DevSchema}.K7M_CLF_MDM"
  val dashboardPath = s"${config.auxPath}K7M_CLF_MDM"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CLF_MDMOUT //витрина
  override def processName: String = "CLF"

  def DoClfMdmR() {

    Logger.getLogger("org").setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""select
                     f7m_id
                   , position_eks
                   , position_ul
                   , doc_ser_eks
                   , doc_num_eks
                   , doc_date_eks
                   , mdm_id 					-- a.	Id клиента фл в МДМ (CLF.MDM_ID)
                  	, crm_id as crm_id 				-- a2.	Id клиента фл в CRM КБ
                   , inn_eks
                   , inn_ul
                   , inn_ex
                   , inn_gs
                  	, mdm_active_flag 			-- b.	Флаг активности записи в МДМ (Y/N)
                  	, full_name
                  	, clf_l_name
                  	, clf_f_name
                  	, clf_m_name
                  	, status_active_dead		-- c.	Статус клиента (ACTIVE/DEAD)
                  	, id_series_num			    -- d.	ДУЛ (Серия и номер) (если поиск фл осуществлялся по ключу ФИО+ДР+Мобильный телефон или ФИО+ИННфл)
                   , id_date               --Дата выдачи
                   ,identifier_desc
                   ,issue_location
                   , ID_SERIES          --Серия
                   , ID_NUM              --номер
                  	, birth_date				-- e.	Дата рождения (если поиск фл осуществлялся по ключу ФИО+ИННфл)
                   , birth_place       -- место рождения
                  	,cast(null as string) job_title_name		-- f.	Должность
                  	, id_end_date			    -- g.	Дата истечения ДУЛ
                  	, gender_tp_code			-- h.	Пол
                  	, citizenship				-- j.	Гражданство
                  	, inn						-- k.	ИНН фл
                  	, adr_reg					-- l.	Адрес регистрации (список полей),
                  	, adr_fact					-- m.	Адрес фактического проживания (список полей),
                  	, tel_home				-- n.	Телефон (домашний),
                  	, tel_mob				-- o.	Телефон (мобильный),
                  	, email					-- p.	Адрес электронной почты,
                  	, udbo_open_dt			-- q.	Дата открытия договора УДБО/ДБО,
                  	, udbo_active_flag				-- r.	Признак действия договора УДБО/ДБО,
                  	, udbo_agreement_num -- s.	Номер договора УДБО/ДБО.
                  	, snils					-- t.	СНИЛС
                  from
                  (
                  select f7m_id
                        ,position_eks
                        ,position_ul
                        ,doc_ser_eks
                        ,doc_num_eks
                        ,doc_date_eks
                        ,crm_id
                        ,inn_eks
                        ,inn_ul
                        ,inn_ex
                        ,inn_gs
                        ,mdm_id
                        ,mdm_active_flag
                        ,full_name
                        ,clf_l_name
                        ,clf_f_name
                        ,clf_m_name
                        ,status_active_dead
                        ,id_series_num
                        ,id_date
                        ,identifier_desc
                        ,issue_location
                        ,ID_SERIES
                        ,ID_NUM
                        ,birth_date
                        ,birth_place
                        ,id_end_date
                        ,gender_tp_code
                        ,citizenship
                        ,systems_count
                        ,last_update_dt
                        ,inn
                        ,adr_reg
                        ,adr_fact
                        ,tel_home
                        ,tel_mob
                        ,email
                        ,udbo_open_dt
                        ,udbo_active_flag
                        ,udbo_agreement_num
                        ,snils
                        ,mkj
                        ,rn  from $Node1t_team_k7m_aux_d_K7M_CLF_MDMIN where rn=1
                  ) clf
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLF_MDMOUT")

    logInserted()

  }
}




