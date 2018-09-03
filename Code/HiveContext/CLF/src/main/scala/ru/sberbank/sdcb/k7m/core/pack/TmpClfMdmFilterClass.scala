package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmFilterClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_filterIN = s"${DevSchema}.clf_raw_keys"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_filterIN = s"${Stg0Schema}.mdm_personname"
  val Node3t_team_k7m_aux_d_tmp_clf_mdm_filterIN = s"${Stg0Schema}.mdm_identifier"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_filterOUT = s"${DevSchema}.tmp_clf_mdm_filter"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_filter"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_filterOUT //витрина
  override def processName: String = "CLF"
  def DoTmpClfMdmFilter()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_filterOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s""" select distinct cont_id from
       (select t.cont_id
         	from
           (select distinct t.cont_id
         	  from
         		(select distinct
         			trim(regexp_replace(upper(fio),'[^A-ZА-Я]+',' ')) fio
         		from $Node1t_team_k7m_aux_d_tmp_clf_mdm_filterIN
         		where
         			nvl(trim(regexp_replace(upper(fio),'[^A-ZА-Я]+',' ')),'') <> ''
         		) clf
         		join (
         		select
         			  cont_id
         			, trim(regexp_replace(upper(concat(last_name, ' ', given_name_one, ' ', given_name_two)),'[^A-ZА-Я]+',' ')) fio
         		from $Node2t_team_k7m_aux_d_tmp_clf_mdm_filterIN
         		where
         			nvl(trim(regexp_replace(upper(concat(last_name, ' ', given_name_one, ' ', given_name_two)),'[^A-ZА-Я]+',' ')), '') <> ''
         		) t
         			on t.fio=clf.fio)
         union all
         	select cont_id from
         	(select t.cont_id
         	from
         		(select distinct
         			trim(regexp_replace(inn,'[^0-9]','')) as inn
         		from $Node1t_team_k7m_aux_d_tmp_clf_mdm_filterIN
         		where length(trim(regexp_replace(inn,'[^0-9]',''))) = 12
         		) clf2
         		join (
         		select
         			  p.cont_id
         			, trim(regexp_replace(t.ref_num,'[^0-9]','')) as inn
         		 from $Node2t_team_k7m_aux_d_tmp_clf_mdm_filterIN p
         			left join $Node3t_team_k7m_aux_d_tmp_clf_mdm_filterIN t on p.cont_id = t.cont_id and t.id_tp_cd = 1011
         		) t
         			on t.inn=clf2.inn and t.inn not in ('111111111111','100000000000','000000000000'))	t
         			) t
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_filterOUT")

    logInserted()
  }
}
