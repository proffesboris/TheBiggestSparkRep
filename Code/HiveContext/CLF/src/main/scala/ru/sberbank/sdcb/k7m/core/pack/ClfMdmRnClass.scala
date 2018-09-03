package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfMdmRnClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CLF_MDM_RNIN = s"${DevSchema}.clf_keys"
  val Node2t_team_k7m_aux_d_K7M_CLF_MDM_RNIN = s"${DevSchema}.clf_fpers_mdm"
   val Nodet_team_k7m_aux_d_K7M_CLF_MDM_RNOUT = s"${DevSchema}.K7M_CLF_MDM_RN"
  val dashboardPath = s"${config.auxPath}K7M_CLF_MDM_RN"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CLF_MDM_RNOUT //витрина
  override def processName: String = "CLF"

  def DoClfMdmRn() {

    Logger.getLogger("org").setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""select
                   f7m_id
                  ,position_eks
                  ,position_ul
                  ,doc_ser_eks
                  ,doc_num_eks
                  ,doc_date_eks
                  ,max(crm_id) over(	partition by  clf.f7m_id) as crm_id
                  ,max(inn_eks) over (partition by  clf.f7m_id) as inn_eks
                  ,max(inn_ul) over (partition by  clf.f7m_id) as inn_ul
                  ,max(inn_ex) over (partition by  clf.f7m_id) as inn_ex
                  ,max(inn_gs) over (partition by  clf.f7m_id) as inn_gs
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
                  , ID_SERIES
                  , ID_NUM
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
                  , row_number() over(
                  		partition by
                  			  clf.f7m_id
                     order by
                  			    clf.mkj
                         ,	case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
                         , case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
                         , systems_count desc
                         , last_update_dt desc
                         , case when (clf.doc_date_eks) is not null then 1 else 2 end
                         , case when (clf.doc_ser_eks) is not null then 1 else 2 end
                         , case when (clf.doc_num_eks) is not null then 1 else 2 end
                         , case when (clf.position_eks) is not null then 1 else 2 end
                         , case when (clf.position_ul) is not null then 1 else 2 end
                         , case when (clf.ID_SERIES) is not null then 1 else 2 end
                         , case when (clf.ID_NUM) is not null then 1 else 2 end
                         , case when (clf.id_date) is not null then 1 else 2 end
                         , case when (clf.crm_id) is not null then 1 else 2 end
                         , case when (clf.id_series_num) is not null then 1 else 2 end
                         , case when (clf.identifier_desc) is not null then 1 else 2 end
                         , case when (clf.issue_location) is not null then 1 else 2 end
                  			  , case when (clf.birth_date) is not null then 1 else 2 end
                         , case when (clf.birth_place) is not null then 1 else 2 end
                  			  , case when (clf.tel_mob) is not null then 1 else 2 end
                  			  , case when (clf.tel_home) is not null then 1 else 2 end
                  			  , case when (clf.adr_reg) is not null then 1 else 2 end
                  			  , case when (clf.adr_fact) is not null then 1 else 2 end
                  			  , case when status_active_dead ='ACTIVE'  then 1 else 2 end
                         , case when (clf.inn_eks) is not null then 1 else 2 end
                         , case when (clf.inn_ul) is not null then 1 else 2 end
                         , case when (clf.inn_ex) is not null then 1 else 2 end
                         , case when (clf.inn_gs) is not null then 1 else 2 end
                  		) as rn
                  from
                  (
                  select distinct
                       f7m_id
                      , position_eks
                      , position_ul
                      , doc_ser_eks
                      , doc_num_eks
                      , doc_date_eks
                      , cast(crm_id as string) as crm_id
                      ,inn_eks
                      ,inn_ul
                      ,inn_ex
                      ,inn_gs
                      , cast(mdm_id as string) as mdm_id
                      , mdm_active_flag
                      , full_name
                      , clf_l_name
                      , clf_f_name
                      , clf_m_name
                      , status_active_dead
                      , id_series_num
                      , id_date
                      , identifier_desc
                      , issue_location
                      , ID_SERIES
                      , ID_NUM
                      , birth_date
                      , birth_place
                      , id_end_date
                      , gender_tp_code
                      , citizenship
                      ,systems_count
                      ,last_update_dt
                  	   , inn						-- k.	ИНН фл
                  	   , adr_reg					-- l.	Адрес регистрации (список полей),
                  	   , adr_fact					-- m.	Адрес фактического проживания (список полей),
                  	   , tel_home				-- n.	Телефон (домашний),
                  	   , tel_mob				-- o.	Телефон (мобильный),
                  	   , email					-- p.	Адрес электронной почты,
                  	   , udbo_open_dt			-- q.	Дата открытия договора УДБО/ДБО,
                  	   , udbo_active_flag		-- r.	Признак действия договора УДБО/ДБО,
                  	   , udbo_agreement_num -- s.	Номер договора УДБО/ДБО.
                  	   , snils					-- t.	СНИЛС
                   	 , mkj
                  from $Node1t_team_k7m_aux_d_K7M_CLF_MDM_RNIN k
                  join $Node2t_team_k7m_aux_d_K7M_CLF_MDM_RNIN   mdm	  on  k.link_id = mdm.id
                  ) clf
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLF_MDM_RNOUT")

    logInserted()

  }
}



