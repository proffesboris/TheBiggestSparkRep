package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersCrmcbKiClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_crmcb_kiIN = s"${DevSchema}.tmp_clf_crmcb_dedubl"
  val Node2t_team_k7m_aux_d_clf_fpers_crmcb_kiIN = s"${DevSchema}.tmp_clf_raw_keys"
   val Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT = s"${DevSchema}.clf_fpers_crmcb_ki"
  val dashboardPath = s"${config.auxPath}clf_fpers_crmcb_ki"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT //витрина
  override def processName: String = "CLF"

  def DoClfFpersCrmcbKi()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""select id,position_eks,position_ul,
          crm_id,full_name,clf_l_name,clf_f_name,clf_m_name,id_series_num,id_series,id_num,registrator,reg_code,id_date,birth_date,job,id_end_date,gender_tp_code,
                 tel_mob,email,last_update_dt,full_name_clear,id_series_num_clear,birth_date_clear,tel_mob_clear,rn_id,rn_tel,k1,k2,c1,c2
                 , case when (k1 = c1) and (rn_id = 1)  then 1   -- Ключ 1: ФИО + ДУЛ + ДР
                 		    when (k2 = c2) and (rn_tel = 1) then 2	-- Ключ 2: ФИО + ДР + телефон
                 		    else -1
                 	 end as ckj
          from (
                select
       	          keys.id ----Пока это сквозной ключ, но он будет формироваться как concat(upper(substr(crit,1,2)), row_number() over( order by ...)) Пример: UL_1234567890,GS_1234567891, где 1234567890 локальный ид в каждой логической порции связей.
                ,case when keys.id like 'EK%' then keys.position else null end position_eks
                ,case when keys.id like 'UL%' then keys.position else null end position_ul
                ,crmcb.crm_id
                ,crmcb.full_name
                ,crmcb.clf_l_name
                ,crmcb.clf_f_name
                ,crmcb.clf_m_name
                ,crmcb.id_series_num
                ,crmcb.id_series
                ,crmcb.id_num
                ,crmcb.registrator
                ,crmcb.reg_code
                ,crmcb.id_date
                ,crmcb.birth_date
                ,crmcb.job
                ,crmcb.id_end_date
                ,crmcb.gender_tp_code
                ,crmcb.tel_mob
                ,crmcb.email
                ,crmcb.last_update_dt
                ,crmcb.full_name_clear
                ,crmcb.id_series_num_clear
                ,crmcb.birth_date_clear
                ,crmcb.tel_mob_clear
                ,crmcb.rn_id
                ,crmcb.rn_tel
                ,k1
                ,k2
                ,	case when (crmcb.full_name_clear is not null) and (crmcb.id_series_num_clear is not null) and (crmcb.birth_date_clear is not null) then
                  concat(crmcb.full_name_clear,'_', crmcb.id_series_num_clear,'_', crmcb.birth_date_clear)
                  else null
                  end as c1
                ,	case when (crmcb.full_name_clear is not null) and (crmcb.birth_date_clear is not null) and (crmcb.tel_mob_clear is not null) then
                  concat(crmcb.full_name_clear,'_', crmcb.birth_date_clear,'_', crmcb.tel_mob_clear)
                  else null
                  end as c2
                from
                	(select id, keys_fio, keys_doc_ser, cell_ph_num, keys_birth_dt, k1, k2, position
                	 from $Node2t_team_k7m_aux_d_clf_fpers_crmcb_kiIN) keys
                	left join
                	(select crm_id,
                	        full_name,
                	        clf_l_name,
                	        clf_f_name,
                	        clf_m_name,
                	        id_series_num,
                	        id_series,
                	        id_num,
                	        registrator,
                	        reg_code,
                	        id_date,
                	        birth_date,
                	        job,
                	        id_end_date,
                	        gender_tp_code,
                	        tel_mob,
                	        email,
                	        last_update_dt,
                	        full_name_clear,
                	        id_series_num_clear,
                	        birth_date_clear,
                	        tel_mob_clear,
                	        rn_id,
                	        rn_tel
                  from $Node1t_team_k7m_aux_d_clf_fpers_crmcb_kiIN where rn_id=1 or rn_tel=1) crmcb
                		on keys.keys_fio = crmcb.full_name_clear
              )
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT")

    logInserted()
    logEnd()
  }
}