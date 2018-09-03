package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfCrmClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CLF_CRMIN = s"${DevSchema}.K7M_CLF_CRM_RN"
  val Nodet_team_k7m_aux_d_K7M_CLF_CRMOUT = s"${DevSchema}.K7M_CLF_CRM"
  val dashboardPath = s"${config.auxPath}K7M_CLF_CRM"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CLF_CRMOUT //витрина
  override def processName: String = "CLF"

  def DoClfCrm() {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CLF_CRMOUT).setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""SELECT
            clf.f7m_id
          , clf.position_eks as position_eks
          , clf.position_ul as position_ul
         	, cast(null as string) as mdm_id 					-- a.	Id клиента фл в МДМ (CLF.MDM_ID)
         	, clf.crm_id crm_id 			                -- a2.	Id клиента фл в CRM КБ
         ,clf.doc_ser_eks
         ,clf.doc_num_eks
         ,clf.doc_date_eks
         ,clf.inn_eks
         ,clf.inn_ul
         ,clf.inn_ex
         ,clf.inn_gs
         	, cast(null as string) crm_active_flag			-- b.	Флаг активности записи в CRM КБ (Y/N)
          , trim(concat(clf.clf_l_name, ' ', clf.clf_f_name, ' ', clf.clf_m_name)) full_name
         	, clf.clf_l_name clf_l_name
         	, clf.clf_f_name clf_f_name
         	, clf.clf_m_name clf_m_name
         	, cast(null as string) as status_active_dead	     -- c.	Статус клиента (ACTIVE/DEAD)
         	, concat(clf.id_series, ' ', clf.id_num) id_series_num  -- d.	ДУЛ (Серия и номер) (если поиск фл осуществлялся по ключу ФИО+ДР+Мобильный телефон или ФИО+ИННфл)
          ,clf.id_series id_series
          ,clf.id_num id_num
          ,clf.id_date                                         --Дата выдачи
         	, clf.birth_date as  birth_date		-- e.	Дата рождения (если поиск фл осуществлялся по ключу ФИО+ИННфл)
         	, clf.job as   	job		-- f.	Должность
         	, clf.id_end_date id_end_date	 	-- g.	Дата истечения ДУЛ
         	, clf.gender_tp_code gender_tp_code		-- h.	Пол
         	, cast(null as string) citizenship				-- j.	Гражданство
         	, cast(null as string) inn						-- k.	ИНН фл
          , clf.jur_addr  adr_reg		-- l.	Адрес регистрации (список полей),
         	, clf.fact_addr adr_fact		-- m.	Адрес фактического проживания (список полей),
          , clf.tel_home
         	, clf.tel_mob					-- o.	Телефон (мобильный)
         	, clf.email email			-- p.	Адрес электронной почты
         ,cast(null as string) as udbo_open_dt
         ,cast(null as string) as udbo_active_flag
         ,cast(null as string) as udbo_agreement_num
         	, cast(null as string) as snils					-- t.	СНИЛС
         from
         (
         select
              f7m_id
             ,position_eks
             ,position_ul
             ,crm_id
         ,doc_ser_eks
         ,doc_num_eks
         ,doc_date_eks
            ,inn_eks
            ,inn_ul
            ,inn_ex
            ,inn_gs
             ,clf_l_name
             ,clf_f_name
             ,clf_m_name
             ,id_series
             ,id_num
             ,id_date
             ,birth_date
             ,job
             ,id_end_date
             ,gender_tp_code
             ,jur_addr
             ,fact_addr
             ,tel_home
             ,tel_mob
             ,email
             ,ckj
             ,rn
         from $Node1t_team_k7m_aux_d_K7M_CLF_CRMIN where rn=1
         ) clf
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLF_CRMOUT")

    logInserted()
  }
}


