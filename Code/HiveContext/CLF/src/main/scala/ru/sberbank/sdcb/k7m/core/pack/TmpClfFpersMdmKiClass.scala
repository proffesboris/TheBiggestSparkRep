package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfFpersMdmKiClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_fpers_mdm_kiIN = s"${DevSchema}.tmp_clf_mdm_dedubl"
  val  Nodet_team_k7m_aux_d_tmp_clf_fpers_mdm_kiOUT = s"${DevSchema}.tmp_clf_fpers_mdm_ki"
  val dashboardPath = s"${config.auxPath}tmp_clf_fpers_mdm_ki"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_fpers_mdm_kiOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfFpersMdmKi()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_fpers_mdm_kiOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s""" select           mdm_id,         mdm_active_flag,           full_name,           clf_l_name,             clf_f_name,             clf_m_name,
                            status_active_dead,          id_series_num,        id_date,           identifier_desc,        issue_location,
                            birth_date,      id_end_date,           gender_tp_code,       citizenship,        inn,           adr_reg,
                            adr_reg_postal_code,    adr_reg_cntry_name,        adr_reg_region,        adr_reg_city_name,          adr_reg_area,
                            adr_reg_settlement,        adr_reg_street_name,          adr_reg_house_n,       adr_reg_korpus_number,        adr_reg_building_number,
                            adr_reg_residence_num,    adr_reg_kladr,      adr_reg_kladr_1,    adr_reg_kladr_2,    adr_reg_kladr_3,   adr_reg_kladr_4,   adr_reg_kladr_5,     adr_reg_kladr_6,
                            adr_fact,   adr_fact_postal_code,   adr_fact_cntry_name,    adr_fact_region,     adr_fact_city_name,    adr_fact_area,    adr_fact_settlement,
                            adr_fact_street_name,      adr_fact_house_number,  adr_fact_korpus_number,     adr_fact_building_number,    adr_fact_residence_num,
                            adr_fact_kladr,     adr_fact_kladr_1,     adr_fact_kladr_2,   adr_fact_kladr_3,    adr_fact_kladr_4,      adr_fact_kladr_5,        adr_fact_kladr_6,
                            tel_home,        tel_mob,          email,       udbo_open_dt,    udbo_active_flag,  udbo_agreement_num,        snils,       systems_count,
                            last_update_dt,    full_name_clear,     id_series_num_clear,   inn_clear,   birth_date_clear,  tel_mob_clear,      rn_inn,      rn_id,    rn_tel,    rn_inn_only
                          ,	cast((case when (full_name_clear is not null) and (id_series_num_clear is not null) and (birth_date_clear is not null) then
                            concat(full_name_clear,'_', id_series_num_clear,'_', birth_date_clear)
                            else null
                            end) as string) as m1
                          ,	cast((case when (full_name_clear is not null) and (birth_date_clear is not null) and (tel_mob_clear is not null) then
                            concat(full_name_clear ,'_', birth_date_clear ,'_', tel_mob_clear)
                            else null
                            end)as string) as m2
                            ,cast((case when (full_name_clear is not null) and (inn_clear is not null) then
                             concat(full_name_clear,'_', nvl(inn_clear,'-1'))
                             else null
                             end)as string) as m3
                          ,cast((case when (inn_clear is not null) then
                            inn_clear
                            else null
                            end) as string) as m4
         from $Node1t_team_k7m_aux_d_tmp_clf_fpers_mdm_kiIN
         where rn_inn=1 or
               rn_id=1 or
               rn_tel=1 or
               rn_inn_only=1
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_fpers_mdm_kiOUT")

    logInserted()
    logEnd()
  }
}