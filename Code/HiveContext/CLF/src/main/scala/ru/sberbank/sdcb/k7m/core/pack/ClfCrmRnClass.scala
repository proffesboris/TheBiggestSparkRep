package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfCrmRnClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CLF_CRM_RNIN = s"${DevSchema}.clf_keys"
  val Node2t_team_k7m_aux_d_K7M_CLF_CRM_RNIN = s"${DevSchema}.clf_fpers_crmcb"
  val Node3t_team_k7m_aux_d_K7M_CLF_CRM_RNIN = s"${DevSchema}.K7M_CRM_ADDR"
  val Nodet_team_k7m_aux_d_K7M_CLF_CRM_RNOUT = s"${DevSchema}.K7M_CLF_CRM_RN"
  val dashboardPath = s"${config.auxPath}K7M_CLF_CRM_RN"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CLF_CRM_RNOUT //витрина
  override def processName: String = "CLF"

  def DoClfCrmRn() {

    Logger.getLogger("org").setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""select
         clf.f7m_id,
         clf.position_eks,
         clf.position_ul,
         clf.crm_id
           ,clf.doc_ser_eks
           ,clf.doc_num_eks
           ,clf.doc_date_eks
           ,max(clf.inn_eks) over (partition by  clf.f7m_id) as inn_eks
           ,max(clf.inn_ul)  over (partition by  clf.f7m_id) as inn_ul
           ,max(clf.inn_ex)  over (partition by  clf.f7m_id) as inn_ex
           ,max(clf.inn_gs)  over (partition by  clf.f7m_id) as inn_gs,
         clf.clf_l_name,
         clf.clf_f_name,
         clf.clf_m_name,
         clf.id_series,
         clf.id_num,
         clf.id_date,
         clf.birth_date ,
         clf.job,
         clf.id_end_date,
         clf.gender_tp_code,
         clf.jur_addr,
         clf.fact_addr,
         tel_home,
         clf.tel_mob,
         clf.email,
         clf.ckj
         , row_number() over(
         		partition by
         			  clf.f7m_id
         		order by
         			    clf.ckj
                , case when (clf.position_eks) is not null then 1 else 2 end
                , case when (clf.position_ul) is not null then 1 else 2 end
         			  , case when concat(clf.id_series, clf.id_num) is not null then 1 else 2 end
         			  , case when (clf.birth_date) is not null then 1 else 2 end
                , case when (clf.id_date) is not null then 1 else 2 end
         			  , case when (clf.tel_mob) is not null then 1 else 2 end
         			  , case when (clf.jur_addr) is not null then 1 else 2 end
         			  , case when (clf.fact_addr) is not null then 1 else 2 end
         			  , case when (clf.email) is not null then 1 else 2 end
         			  , case when (clf.job) is not null then 1 else 2 end
                     , case when (clf.doc_date_eks) is not null then 1 else 2 end
                     , case when (clf.doc_ser_eks) is not null then 1 else 2 end
                     , case when (clf.doc_num_eks) is not null then 1 else 2 end
                     , case when (clf.inn_eks) is not null then 1 else 2 end
                     , case when (clf.inn_ul) is not null then 1 else 2 end
                     , case when (clf.inn_ex) is not null then 1 else 2 end
                     , case when (clf.inn_gs) is not null then 1 else 2 end
         		) as rn
 from
         (
         select
         distinct
         k.f7m_id,
         clf.position_eks,
         clf.position_ul,
         clf.crm_id
         ,doc_ser_eks
         ,doc_num_eks
         ,doc_date_eks
              ,clf.inn_eks
              ,clf.inn_ul
              ,clf.inn_ex
              ,clf.inn_gs,
         clf.clf_l_name,
         clf.clf_f_name,
         clf.clf_m_name,
         clf.id_series,
         clf.id_num,
         clf.id_date,
         clf.birth_date ,
         clf.job,
         clf.id_end_date,
         clf.gender_tp_code,
         addrc.jur_addr,
         addrc.fact_addr,
         cast(null as string) as tel_home,
         clf.tel_mob,
         clf.email,
         clf.ckj
         from $Node1t_team_k7m_aux_d_K7M_CLF_CRM_RNIN k
         join $Node2t_team_k7m_aux_d_K7M_CLF_CRM_RNIN   clf	    on  k.link_id = clf.id
         left join $Node3t_team_k7m_aux_d_K7M_CLF_CRM_RNIN addrc   on  addrc.contact_id = clf.crm_id
         ) clf
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLF_CRM_RNOUT")

    logInserted()
  }
}

