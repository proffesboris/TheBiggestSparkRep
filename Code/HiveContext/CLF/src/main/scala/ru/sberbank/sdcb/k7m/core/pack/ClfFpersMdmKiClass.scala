package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersMdmKiClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

   val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_mdm_kiIN = s"${DevSchema}.tmp_clf_raw_keys"
  val Node2t_team_k7m_aux_d_clf_fpers_mdm_kiIN = s"${DevSchema}.tmp_clf_mdm_dedubl"
  val Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT = s"${DevSchema}.clf_fpers_mdm_ki"

  val dashboardPath = s"${config.auxPath}clf_fpers_mdm_ki"

  override  val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT //витрина
  override  def processName: String = "CLF"

  def DoClfFpersKiMdm()
   {
     Logger
    .getLogger(Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT).setLevel(Level.WARN)
     logStart()

    val smartSrcHiveTable_t7 = spark.sql(
         s""" select distinct
                id        ,mdm_id        ,mdm_active_flag        ,full_name        ,clf_l_name        ,clf_f_name        ,clf_m_name        ,status_active_dead
               ,id_series_num        ,id_date        ,identifier_desc        ,issue_location        ,birth_date        ,id_end_date        ,gender_tp_code        ,citizenship        ,inn
               ,adr_reg        ,adr_reg_postal_code        ,adr_reg_cntry_name        ,adr_reg_region        ,adr_reg_city_name        ,adr_reg_area        ,adr_reg_settlement
               ,adr_reg_street_name        ,adr_reg_house_number        ,adr_reg_korpus_number        ,adr_reg_building_number        ,adr_reg_residence_num        ,adr_reg_kladr
               ,adr_reg_kladr_1        ,adr_reg_kladr_2        ,adr_reg_kladr_3        ,adr_reg_kladr_4        ,adr_reg_kladr_5        ,adr_reg_kladr_6
               ,adr_fact        ,adr_fact_postal_code        ,adr_fact_cntry_name        ,adr_fact_region        ,adr_fact_city_name        ,adr_fact_area        ,adr_fact_settlement
               ,adr_fact_street_name        ,adr_fact_house_number        ,adr_fact_korpus_number        ,adr_fact_building_number        ,adr_fact_residence_num        ,adr_fact_kladr        ,adr_fact_kladr_1
               ,adr_fact_kladr_2        ,adr_fact_kladr_3        ,adr_fact_kladr_4        ,adr_fact_kladr_5        ,adr_fact_kladr_6        ,tel_home
               ,tel_mob        ,email        ,udbo_open_dt        ,udbo_active_flag        ,udbo_agreement_num        ,snils        ,systems_count
               ,last_update_dt        ,full_name_clear        ,id_series_num_clear        ,inn_clear        ,birth_date_clear        ,tel_mob_clear
               ,rn_inn        ,rn_id        ,rn_tel        ,rn_inn_only
               ,k1    ,k2     ,k3      ,k4
               ,m1    ,m2     ,m3      ,m4
                ,cast((case when (k1=m1) and (rn_id = 1) 		  then 1
                  	   when (k2=m2) and (rn_tel = 1) 		  then 2
                  	   when (k3=m3) and (rn_inn = 1) 		  then 3
                  	   when (k4=m4) and (rn_inn_only = 1) then 4
                  	   else -1 end) as string) as  mkj
       from (  select      keys.id as id,
                           mdm.mdm_id mdm_id,
                           mdm.mdm_active_flag mdm_active_flag,
                           mdm.full_name full_name,
                           mdm.clf_l_name clf_l_name,
                           mdm.clf_f_name clf_f_name,
                           mdm.clf_m_name  clf_m_name,
                           mdm.status_active_dead status_active_dead,
                           mdm.id_series_num id_series_num,
                           mdm.id_date  id_date,
                           mdm.identifier_desc identifier_desc,
                           mdm.issue_location issue_location,
                           mdm.birth_date birth_date,
                           mdm.id_end_date id_end_date,
                           mdm.gender_tp_code gender_tp_code,
                           mdm.citizenship citizenship,
                           mdm.inn inn,
                           mdm.adr_reg adr_reg,
                           mdm.adr_reg_postal_code adr_reg_postal_code,
                           mdm.adr_reg_cntry_name adr_reg_cntry_name,
                           mdm.adr_reg_region adr_reg_region,
                           mdm.adr_reg_city_name adr_reg_city_name,
                           mdm.adr_reg_area adr_reg_area,
                           mdm.adr_reg_settlement adr_reg_settlement,
                           mdm.adr_reg_street_name adr_reg_street_name,
                           mdm.adr_reg_house_number adr_reg_house_number,
                           mdm.adr_reg_korpus_number adr_reg_korpus_number,
                           mdm.adr_reg_building_number adr_reg_building_number,
                           mdm.adr_reg_residence_num adr_reg_residence_num,
                           mdm.adr_reg_kladr adr_reg_kladr,
                           mdm.adr_reg_kladr_1 adr_reg_kladr_1,
                           mdm.adr_reg_kladr_2 adr_reg_kladr_2,
                           mdm.adr_reg_kladr_3 adr_reg_kladr_3,
                           mdm.adr_reg_kladr_4 adr_reg_kladr_4,
                           mdm.adr_reg_kladr_5 adr_reg_kladr_5,
                           mdm.adr_reg_kladr_6 adr_reg_kladr_6,
                           mdm.adr_fact adr_fact,
                           mdm.adr_fact_postal_code adr_fact_postal_code,
                           mdm.adr_fact_cntry_name adr_fact_cntry_name,
                           mdm.adr_fact_region adr_fact_region,
                           mdm.adr_fact_city_name adr_fact_city_name,
                           mdm.adr_fact_area adr_fact_area,
                           mdm.adr_fact_settlement adr_fact_settlement,
                           mdm.adr_fact_street_name  adr_fact_street_name   ,
                           mdm.adr_fact_house_number  adr_fact_house_number    ,
                           mdm.adr_fact_korpus_number    adr_fact_korpus_number  ,
                           mdm.adr_fact_building_number  adr_fact_building_number     ,
                           mdm.adr_fact_residence_num   adr_fact_residence_num      ,
                           mdm.adr_fact_kladr  adr_fact_kladr ,
                           mdm.adr_fact_kladr_1  adr_fact_kladr_1 ,
                           mdm.adr_fact_kladr_2 adr_fact_kladr_2 ,
                           mdm.adr_fact_kladr_3 adr_fact_kladr_3,
                           mdm.adr_fact_kladr_4 adr_fact_kladr_4  ,
                           mdm.adr_fact_kladr_5 adr_fact_kladr_5  ,
                           mdm.adr_fact_kladr_6 adr_fact_kladr_6 ,
                           mdm.tel_home tel_home    ,
                           mdm.tel_mob tel_mob    ,
                           mdm.email   email   ,
                           mdm.udbo_open_dt  udbo_open_dt  ,
                           mdm.udbo_active_flag udbo_active_flag ,
                           mdm.udbo_agreement_num udbo_agreement_num ,
                           mdm.snils   snils ,
                           mdm.systems_count   systems_count    ,
                           mdm.last_update_dt last_update_dt  ,
                           mdm.full_name_clear    full_name_clear   ,
                           mdm.id_series_num_clear id_series_num_clear   ,
                           mdm.inn_clear  inn_clear,
                           mdm.birth_date_clear birth_date_clear   ,
                           mdm.tel_mob_clear    tel_mob_clear     ,
                           mdm.rn_inn rn_inn        ,
                           mdm.rn_id  rn_id       ,
                           mdm.rn_tel rn_tel     ,
                           mdm.rn_inn_only rn_inn_only        ,
                           keys.k1 k1         ,
                           keys.k2 k2         ,
                           keys.k3 k3         ,
                           keys.k4 k4         ,
                           mdm.m1 m1        ,
                           mdm.m2 m2        ,
                           mdm.m3 m3        ,
                           mdm.m4 m4
                           from $Node1t_team_k7m_aux_d_clf_fpers_mdm_kiIN keys
                	left join (select mdm_id,         mdm_active_flag,           full_name,           clf_l_name,             clf_f_name,             clf_m_name,
                                   status_active_dead,          id_series_num,        id_date,           identifier_desc,        issue_location,
                                   birth_date,      id_end_date,           gender_tp_code,       citizenship,        inn,           adr_reg,
                                   adr_reg_postal_code,    adr_reg_cntry_name,        adr_reg_region,        adr_reg_city_name,          adr_reg_area,
                                   adr_reg_settlement,        adr_reg_street_name,          adr_reg_house_number,       adr_reg_korpus_number,        adr_reg_building_number,
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
                from $Node2t_team_k7m_aux_d_clf_fpers_mdm_kiIN where rn_inn=1 or rn_id=1 or rn_tel=1 or rn_inn_only=1) mdm	on (mdm.full_name_clear = keys.keys_fio)
                 union all
                select   keys.id
                         ,mdm.mdm_id
                         ,mdm.mdm_active_flag
                         ,mdm.full_name
                         ,mdm.clf_l_name
                         ,mdm.clf_f_name
                         ,mdm.clf_m_name
                         ,mdm.status_active_dead
                         ,mdm.id_series_num
                         ,mdm.id_date
                         ,mdm.identifier_desc
                         ,mdm.issue_location
                         ,mdm.birth_date
                         ,mdm.id_end_date
                         ,mdm.gender_tp_code
                         ,mdm.citizenship
                         ,mdm.inn
                         ,mdm.adr_reg
                         ,mdm.adr_reg_postal_code
                         ,mdm.adr_reg_cntry_name
                         ,mdm.adr_reg_region
                         ,mdm.adr_reg_city_name
                         ,mdm.adr_reg_area
                         ,mdm.adr_reg_settlement
                         ,mdm.adr_reg_street_name
                         ,mdm.adr_reg_house_number
                         ,mdm.adr_reg_korpus_number
                         ,mdm.adr_reg_building_number
                         ,mdm.adr_reg_residence_num
                         ,mdm.adr_reg_kladr
                         ,mdm.adr_reg_kladr_1
                         ,mdm.adr_reg_kladr_2
                         ,mdm.adr_reg_kladr_3
                         ,mdm.adr_reg_kladr_4
                         ,mdm.adr_reg_kladr_5
                         ,mdm.adr_reg_kladr_6
                         ,mdm.adr_fact
                         ,mdm.adr_fact_postal_code
                         ,mdm.adr_fact_cntry_name
                         ,mdm.adr_fact_region
                         ,mdm.adr_fact_city_name
                         ,mdm.adr_fact_area
                         ,mdm.adr_fact_settlement
                         ,mdm.adr_fact_street_name
                         ,mdm.adr_fact_house_number
                         ,mdm.adr_fact_korpus_number
                         ,mdm.adr_fact_building_number
                         ,mdm.adr_fact_residence_num
                         ,mdm.adr_fact_kladr
                         ,mdm.adr_fact_kladr_1
                         ,mdm.adr_fact_kladr_2
                         ,mdm.adr_fact_kladr_3
                         ,mdm.adr_fact_kladr_4
                         ,mdm.adr_fact_kladr_5
                         ,mdm.adr_fact_kladr_6
                         ,mdm.tel_home
                         ,mdm.tel_mob
                         ,mdm.email
                         ,mdm.udbo_open_dt
                         ,mdm.udbo_active_flag
                         ,mdm.udbo_agreement_num
                         ,mdm.snils
                         ,mdm.systems_count
                         ,mdm.last_update_dt
                         ,mdm.full_name_clear
                         ,mdm.id_series_num_clear
                         ,mdm.inn_clear
                         ,mdm.birth_date_clear
                         ,mdm.tel_mob_clear
                         ,mdm.rn_inn
                         ,mdm.rn_id
                         ,mdm.rn_tel
                         ,mdm.rn_inn_only
                      ,keys.k1              ,keys.k2               ,keys.k3               ,keys.k4
                      ,mdm.m1               ,mdm.m2               ,mdm.m3               ,mdm.m4
                         from  $Node1t_team_k7m_aux_d_clf_fpers_mdm_kiIN keys
                         left join (select mdm_id, mdm_active_flag, full_name, clf_l_name, clf_f_name, clf_m_name, status_active_dead, id_series_num, id_date,identifier_desc,
                                            issue_location, birth_date, id_end_date,gender_tp_code, citizenship, inn, adr_reg, adr_reg_postal_code,
                                            adr_reg_cntry_name, adr_reg_region, adr_reg_city_name, adr_reg_area, adr_reg_settlement, adr_reg_street_name,
                                            adr_reg_house_number, adr_reg_korpus_number, adr_reg_building_number, adr_reg_residence_num, adr_reg_kladr, adr_reg_kladr_1,
                                            adr_reg_kladr_2, adr_reg_kladr_3,  adr_reg_kladr_4, adr_reg_kladr_5, adr_reg_kladr_6, adr_fact, adr_fact_postal_code, adr_fact_cntry_name,
                                            adr_fact_region, adr_fact_city_name, adr_fact_area, adr_fact_settlement, adr_fact_street_name, adr_fact_house_number, adr_fact_korpus_number,
                                            adr_fact_building_number, adr_fact_residence_num,
                                            adr_fact_kladr, adr_fact_kladr_1, adr_fact_kladr_2, adr_fact_kladr_3, adr_fact_kladr_4, adr_fact_kladr_5, adr_fact_kladr_6,
                                            tel_home, tel_mob,  email, udbo_open_dt, udbo_active_flag, udbo_agreement_num, snils, systems_count,
                                            last_update_dt, full_name_clear, id_series_num_clear, inn_clear, birth_date_clear, tel_mob_clear,
                                            rn_inn,   rn_id,    rn_tel, rn_inn_only
                                           ,	cast((case when (full_name_clear is not null) and (id_series_num_clear is not null) and (birth_date_clear is not null) then
                                             concat(full_name_clear,'_', id_series_num_clear,'_', birth_date_clear)
                                             else null
                                             end)as string) as m1
                                           ,cast((case when (full_name_clear is not null) and (birth_date_clear is not null) and (tel_mob_clear is not null) then
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
                                             end)as string) as m4
                         from $Node2t_team_k7m_aux_d_clf_fpers_mdm_kiIN where rn_inn=1 or rn_id=1 or rn_tel=1 or rn_inn_only=1) mdm
                         		on (mdm.inn_clear = keys.keys_inn)
             )
                    """
        )
     smartSrcHiveTable_t7
      .write.format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", dashboardPath)
    .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT")

     logInserted()
     logEnd()

  }
 }