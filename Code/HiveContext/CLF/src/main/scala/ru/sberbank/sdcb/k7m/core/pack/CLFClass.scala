package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CLFClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_CLFIN = s"${DevSchema}.K7M_CLF_MDM"
  val Node2t_team_k7m_aux_d_CLFIN = s"${DevSchema}.K7M_CLF_CRM"
  val Node3t_team_k7m_aux_d_CLFIN = s"${DevSchema}.CLF_keys"
  val Node4t_team_k7m_aux_d_CLFIN = s"${DevSchema}.K7M_VKO"
  val Node5t_team_k7m_aux_d_CLFIN = s"${DevSchema}.clf_bad"
  val Node6t_team_k7m_aux_d_CLFIN = s"${DevSchema}.clf_bad_with_crm"
  val Node7t_team_k7m_aux_d_CLFIN = s"${DevSchema}.etl_exec_stts"
  val Nodet_team_k7m_aux_d_CLF_prepOUT = s"${DevSchema}.CLF_prep"
  val Nodet_team_k7m_aux_d_CLFOUT = s"${MartSchema}.CLF"
  val Nodet_team_k7m_aux_d_clf_keys_oldOUT = s"${DevSchema}.clf_keys_old"
  val dashboardPrepPath = s"${config.auxPath}CLF_prep"
  val dashboardPath = s"${config.paPath}CLF"
  val dashboardKeysOldPath = s"${config.auxPath}clf_keys_old"
  val dashboardKeysNewPath = s"${config.auxPath}clf_keys"


  override val dashboardName: String = Nodet_team_k7m_aux_d_CLFOUT //витрина
  override def processName: String = "CLF"

  def DoCLF() {

    Logger.getLogger(Nodet_team_k7m_aux_d_CLFOUT).setLevel(Level.WARN)

    //  MDM+CRM
    val createHiveTableStage1 = spark.sql(
      s"""
         select distinct
                m.f7m_id as f7m_id_old
               ,case when t.crm_id is not null then t.crm_id else concat('f',m.f7m_id) end f7m_id
               ,cast(null as string) as CRMR_ID
               ,cast(null as string) as TRANSACT_ID
               ,t.crm_id
               ,t.S_NAME
               ,t.F_NAME
               ,t.L_NAME
               ,t.B_PLACE
               ,t.B_DATE
               ,'21' as ID_TYPE
               ,t.id_series
               ,t.id_num
               ,t.ID_SOURCE
               ,t.ID_SOURCE_KOD
               ,t.id_date as ID_DATE
               ,t.inn as INN
               ,cast(null as string) as D_ADDRESS_country
               ,cast(null as string) as D_ADDRESS_postalCode
               ,cast(null as string) as D_ADDRESS_regionName
               ,cast(null as string) as D_ADDRESS_districtName
               ,cast(null as string) as D_ADDRESS_cityName
               ,cast(null as string) as D_ADDRESS_villageName
               ,cast(null as string) as D_ADDRESS_shortVillageTypeName
               ,cast(null as string) as D_ADDRESS_streetName
               ,cast(null as string) as D_ADDRESS_shortStreetTypeName
               ,cast(null as string) as D_ADDRESS_houseNumber
               ,cast(null as string) as D_ADDRESS_buildingNumber
               ,cast(null as string) as D_ADDRESS_blockNumber
               ,cast(null as string) as D_ADDRESS_flatNumber
               ,cast(null as string) as F_ADDRESS_country
               ,cast(null as string) as F_ADDRESS_postalCode
               ,cast(null as string) as F_ADDRESS_regionName
               ,cast(null as string) as F_ADDRESS_districtName
               ,cast(null as string) as F_ADDRESS_cityName
               ,cast(null as string) as F_ADDRESS_villageName
               ,cast(null as string) as F_ADDRESS_shortVillageTypeName
               ,cast(null as string) as F_ADDRESS_streetName
               ,cast(null as string) as F_ADDRESS_shortStreetTypeName
               ,cast(null as string) as F_ADDRESS_houseNumber
               ,cast(null as string) as F_ADDRESS_buildingNumber
               ,cast(null as string) as F_ADDRESS_blockNumber
               ,cast(null as string) as F_ADDRESS_flatNumber
               ,t.snils
               ,cast(null as string) as H_PHONE_phoneNumber
               ,cast(null as string) as W_PHONE_phoneNumber
               ,t.tel_mob as M_PHONE_phoneNumber
               ,t.udbo_open_dt       as UDBO_AGREE_DATE
               ,t.udbo_agreement_num as UDBO_AGREE_NO
               ,t.position_eio as POSITION_EIO
               ,t.email
               ,k.func_podr as FUNC_DIVISION
               ,k.ter_podr as TER_DIVISION
               ,k.terbank as KM_TB
               ,cast(null as string) as exec_id
             from
               (
               select
                coalesce(mdm.f7m_id, crm.f7m_id) as f7m_id
               ,coalesce(mdm.mdm_id, crm.mdm_id) as mdm_id
               ,coalesce(crm.crm_id, mdm.crm_id) as crm_id
               ,coalesce(mdm.mdm_active_flag, crm.crm_active_flag) as active_flag
               ,coalesce(mdm.full_name,crm.full_name) as full_name
               ,coalesce(mdm.clf_l_name,crm.clf_l_name) as S_NAME
               ,coalesce(mdm.clf_f_name,crm.clf_f_name) as F_NAME
               ,coalesce(mdm.clf_m_name,crm.clf_m_name) as L_NAME
               ,mdm.birth_place as B_PLACE
               ,coalesce(mdm.status_active_dead,crm.status_active_dead) as status_active_dead
               ,coalesce(mdm.id_series_num,crm.id_series_num) as id_series_num
               ,coalesce(mdm.birth_date,crm.birth_date) as B_DATE
               ,coalesce(crm.doc_ser_eks,crm.id_series,mdm.doc_ser_eks,mdm.id_series) as id_series
               ,coalesce(crm.doc_num_eks,crm.id_num,mdm.doc_num_eks,mdm.id_num) as id_num
               ,mdm.identifier_desc as ID_SOURCE
               ,mdm.issue_location as ID_SOURCE_KOD
               ,coalesce(mdm.position_eks,crm.position_eks,mdm.position_ul,crm.position_ul) as position_eio
               ,coalesce(crm.doc_date_eks,crm.id_date,mdm.doc_date_eks,mdm.id_date) as id_date
               ,coalesce(mdm.id_end_date,crm.id_end_date) as id_end_date
               ,coalesce(mdm.gender_tp_code,crm.gender_tp_code) as gender_tp_code
               ,coalesce(mdm.citizenship,crm.citizenship) as citizenship
               ,coalesce(crm.inn_eks,crm.inn_ex,crm.inn_ul,crm.inn_gs,mdm.inn_eks,mdm.inn,mdm.inn_ul,mdm.inn_ex,mdm.inn_gs) as inn
               ,coalesce(mdm.adr_reg,crm.adr_reg) as adr_reg
               ,coalesce(mdm.adr_fact,crm.adr_fact) as adr_fact
               ,coalesce(mdm.tel_home,crm.tel_home) as tel_home
               ,coalesce(mdm.tel_mob,crm.tel_mob) as tel_mob
               ,coalesce(mdm.email,crm.email) as email
               ,coalesce(mdm.udbo_open_dt,crm.udbo_open_dt) as udbo_open_dt
               ,coalesce(mdm.udbo_active_flag,crm.udbo_active_flag) as udbo_active_flag
               ,coalesce(mdm.udbo_agreement_num,crm.udbo_agreement_num) as udbo_agreement_num
               ,coalesce(mdm.snils,crm.snils) as snils
               from $Node1t_team_k7m_aux_d_CLFIN mdm
               full join $Node2t_team_k7m_aux_d_CLFIN crm on mdm.f7m_id=crm.f7m_id ) t
               join $Node3t_team_k7m_aux_d_CLFIN m on t.f7m_id = m.f7m_id
               left join $Node4t_team_k7m_aux_d_CLFIN k on t.crm_id = k.crm_id
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPrepPath).saveAsTable(s"$Nodet_team_k7m_aux_d_CLF_prepOUT")

    //Из CLF_BAD дополняем
    val createHiveTableStage2 = spark.sql(
      s"""
         select distinct
                m.f7m_id as f7m_id_old
               ,concat('f',m.f7m_id) as f7m_id
               ,cast(null as string) as CRMR_ID
               ,cast(null as string) as TRANSACT_ID
               ,q.crm_id
               ,split(q.keys_fio,' ')[0] as S_NAME
               ,split(q.keys_fio,' ')[1] as F_NAME
               ,split(q.keys_fio,' ')[2] as L_NAME
               ,cast(null as string) as B_PLACE
               ,cast(q.keys_birth_dt as timestamp) as B_DATE
               ,q.doc_type as ID_TYPE
               ,q.doc_ser as id_series
               ,q.doc_num as id_num
               ,cast(null as string) as ID_SOURCE
               ,cast(null as string) as ID_SOURCE_KOD
               ,q.doc_date as ID_DATE
               ,q.keys_inn as INN
               ,cast(null as string) as D_ADDRESS_country
               ,cast(null as string) as D_ADDRESS_postalCode
               ,cast(null as string) as D_ADDRESS_regionName
               ,cast(null as string) as D_ADDRESS_districtName
               ,cast(null as string) as D_ADDRESS_cityName
               ,cast(null as string) as D_ADDRESS_villageName
               ,cast(null as string) as D_ADDRESS_shortVillageTypeName
               ,cast(null as string) as D_ADDRESS_streetName
               ,cast(null as string) as D_ADDRESS_shortStreetTypeName
               ,cast(null as string) as D_ADDRESS_houseNumber
               ,cast(null as string) as D_ADDRESS_buildingNumber
               ,cast(null as string) as D_ADDRESS_blockNumber
               ,cast(null as string) as D_ADDRESS_flatNumber
               ,cast(null as string) as F_ADDRESS_country
               ,cast(null as string) as F_ADDRESS_postalCode
               ,cast(null as string) as F_ADDRESS_regionName
               ,cast(null as string) as F_ADDRESS_districtName
               ,cast(null as string) as F_ADDRESS_cityName
               ,cast(null as string) as F_ADDRESS_villageName
               ,cast(null as string) as F_ADDRESS_shortVillageTypeName
               ,cast(null as string) as F_ADDRESS_streetName
               ,cast(null as string) as F_ADDRESS_shortStreetTypeName
               ,cast(null as string) as F_ADDRESS_houseNumber
               ,cast(null as string) as F_ADDRESS_buildingNumber
               ,cast(null as string) as F_ADDRESS_blockNumber
               ,cast(null as string) as F_ADDRESS_flatNumber
               ,cast(null as string) as snils
               ,cast(null as string) as H_PHONE_phoneNumber
               ,cast(null as string) as W_PHONE_phoneNumber
               ,q.keys_cell_ph_num   as M_PHONE_phoneNumber
               ,cast(null as string)       as UDBO_AGREE_DATE
               ,cast(null as string)       as UDBO_AGREE_NO
               ,coalesce(q.position_eks,q.position_ul) as POSITION_EIO
               ,cast(null as string) as email
               ,cast(null as string) as FUNC_DIVISION
               ,cast(null as string) as TER_DIVISION
               ,cast(null as string) as KM_TB
               ,cast(null as string) as exec_id
             from $Node3t_team_k7m_aux_d_CLFIN m
           join $Node5t_team_k7m_aux_d_CLFIN q on m.link_id=q.id
  --Берем только f7m_id, которые не встречались в предыдущей выборке
           left join $Nodet_team_k7m_aux_d_CLF_prepOUT p on p.f7m_id_old=m.f7m_id
           where p.f7m_id_old is null
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPrepPath).saveAsTable(s"$Nodet_team_k7m_aux_d_CLF_prepOUT")

    //Из CLF_BAD_WITH_CRM дополняем
    val createHiveTableStage3 = spark.sql(
      s"""
         select distinct
                m.f7m_id as f7m_id_old
               ,q.crm_id as f7m_id
               ,cast(null as string) as CRMR_ID
               ,cast(null as string) as TRANSACT_ID
               ,q.crm_id
               ,coalesce(split(q.keys_fio,' ')[0],k.last_name) as S_NAME
               ,coalesce(split(q.keys_fio,' ')[1],k.fst_name) as F_NAME
               ,coalesce(split(q.keys_fio,' ')[2],k.mid_name) as L_NAME
               ,cast(null as string) as B_PLACE
               ,coalesce(cast(q.keys_birth_dt as timestamp),k.birth_dt) as B_DATE
               ,q.doc_type as ID_TYPE
               ,q.doc_ser as id_series
               ,q.doc_num as id_num
               ,cast(null as string) as ID_SOURCE
               ,cast(null as string) as ID_SOURCE_KOD
               ,cast(null as timestamp) as ID_DATE
               ,q.keys_inn as INN
               ,cast(null as string) as D_ADDRESS_country
               ,cast(null as string) as D_ADDRESS_postalCode
               ,cast(null as string) as D_ADDRESS_regionName
               ,cast(null as string) as D_ADDRESS_districtName
               ,cast(null as string) as D_ADDRESS_cityName
               ,cast(null as string) as D_ADDRESS_villageName
               ,cast(null as string) as D_ADDRESS_shortVillageTypeName
               ,cast(null as string) as D_ADDRESS_streetName
               ,cast(null as string) as D_ADDRESS_shortStreetTypeName
               ,cast(null as string) as D_ADDRESS_houseNumber
               ,cast(null as string) as D_ADDRESS_buildingNumber
               ,cast(null as string) as D_ADDRESS_blockNumber
               ,cast(null as string) as D_ADDRESS_flatNumber
               ,cast(null as string) as F_ADDRESS_country
               ,cast(null as string) as F_ADDRESS_postalCode
               ,cast(null as string) as F_ADDRESS_regionName
               ,cast(null as string) as F_ADDRESS_districtName
               ,cast(null as string) as F_ADDRESS_cityName
               ,cast(null as string) as F_ADDRESS_villageName
               ,cast(null as string) as F_ADDRESS_shortVillageTypeName
               ,cast(null as string) as F_ADDRESS_streetName
               ,cast(null as string) as F_ADDRESS_shortStreetTypeName
               ,cast(null as string) as F_ADDRESS_houseNumber
               ,cast(null as string) as F_ADDRESS_buildingNumber
               ,cast(null as string) as F_ADDRESS_blockNumber
               ,cast(null as string) as F_ADDRESS_flatNumber
               ,cast(null as string) as snils
               ,cast(null as string) as H_PHONE_phoneNumber
               ,k.work_ph_num        as W_PHONE_phoneNumber
               ,coalesce(q.keys_cell_ph_num,k.cell_ph_num) as M_PHONE_phoneNumber
               ,cast(null as string) as UDBO_AGREE_DATE
               ,cast(null as string) as UDBO_AGREE_NO
               ,coalesce(q.position_eks,q.position_ul) as POSITION_EIO
               ,k.alt_email_addr as email
               ,k.func_podr as FUNC_DIVISION
               ,k.ter_podr as TER_DIVISION
               ,k.terbank as KM_TB
               ,CAST(add_months(w.exec_id, 3) as string) as exec_id
             from $Node3t_team_k7m_aux_d_CLFIN m
             join $Node6t_team_k7m_aux_d_CLFIN q on m.link_id=q.id
        left join $Node4t_team_k7m_aux_d_CLFIN k on q.crm_id = k.crm_id
    --Берем только f7m_id, которые не встречались в предыдущей выборке
             left join $Nodet_team_k7m_aux_d_CLF_prepOUT p on p.f7m_id_old=m.f7m_id and p.f7m_id_old is null
       cross join (select max(run_dt) exec_id from $Node7t_team_k7m_aux_d_CLFIN s
         where s.status='RUNNING')w
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPrepPath).saveAsTable(s"$Nodet_team_k7m_aux_d_CLF_prepOUT")

    //Дедубликация

    val createHiveTableStage4 = spark.sql(
      s""" select f7m_id_old
                 ,case when (crm_id is not null and cnt=1) then crm_id  else concat('f',f7m_id_old) end f7m_id
                 ,crmr_id,transact_id,crm_id,s_name,f_name,l_name,b_place,b_date,id_type,id_series,id_num,id_source,id_source_kod
                 ,id_date,inn,d_address_country,d_address_postalcode,d_address_regionname,d_address_districtname,d_address_cityname,d_address_villagename
                 ,d_address_shortvillagetypename,d_address_streetname,d_address_shortstreettypename,d_address_housenumber,d_address_buildingnumber
                 ,d_address_blocknumber,d_address_flatnumber,f_address_country,f_address_postalcode,f_address_regionname
                 ,f_address_districtname,f_address_cityname
                 ,f_address_villagename,f_address_shortvillagetypename
                 ,f_address_streetname,f_address_shortstreettypename
                 ,f_address_housenumber,f_address_buildingnumber
                 ,f_address_blocknumber,f_address_flatnumber,snils,h_phone_phonenumber,w_phone_phonenumber
                 ,m_phone_phonenumber,udbo_agree_date,udbo_agree_no
                 ,position_eio,email,func_division,ter_division
                 ,km_tb,exec_id
           from
                  (select    f7m_id_old
                            ,case when crm_id is not null then crm_id  else f7m_id end f7m_id
                            ,crmr_id,transact_id,crm_id,s_name,f_name,l_name,b_place,b_date,id_type,id_series,id_num,id_source,id_source_kod
                            ,id_date,inn,d_address_country,d_address_postalcode,d_address_regionname,d_address_districtname,d_address_cityname,d_address_villagename
                            ,d_address_shortvillagetypename,d_address_streetname,d_address_shortstreettypename,d_address_housenumber,d_address_buildingnumber
                            ,d_address_blocknumber,d_address_flatnumber,f_address_country,f_address_postalcode,f_address_regionname
                            ,f_address_districtname,f_address_cityname
                            ,f_address_villagename,f_address_shortvillagetypename
                            ,f_address_streetname,f_address_shortstreettypename
                            ,f_address_housenumber,f_address_buildingnumber
                            ,f_address_blocknumber,f_address_flatnumber,snils,h_phone_phonenumber,w_phone_phonenumber
                            ,m_phone_phonenumber,udbo_agree_date,udbo_agree_no
                            ,position_eio,email,func_division,ter_division
                            ,km_tb,exec_id
                            ,row_number () over(
                          		partition by f7m_id
                              order by
                                       id_date desc
                                      ,case when s_name is not null then 1 else 2 end
                                      ,case when f_name is not null then 1 else 2 end
                                      ,case when l_name is not null then 1 else 2 end
                                      ,case when id_series is not null then 1 else 2 end
                                      ,case when id_num is not null then 1 else 2 end
                                      ,case when b_place is not null then 1 else 2 end
                                      ,case when b_date is not null then 1 else 2 end
                                      ,case when id_type is not null then 1 else 2 end
                                      ,case when id_source is not null then 1 else 2 end
                                      ,case when id_source_kod is not null then 1 else 2 end
                                      ,case when position_eio is not null then 1 else 2 end
                                      ,case when email is not null then 1 else 2 end
                                      ,case when func_division is not null then 1 else 2 end
                                      ,case when ter_division is not null then 1 else 2 end
                                      ,case when km_tb is not null then 1 else 2 end
                                      ,case when snils is not null then 1 else 2 end
                                      ,case when udbo_agree_date is not null then 1 else 2 end
                                      ,case when udbo_agree_no is not null then 1 else 2 end
                                     ) as rn_2
--Подсчитываем кол-во f7m_id на 1 crm_id. Если CNT<>1, то в качестве f7m_id берем concat('f',f7m_id_old)
         ,count(*) over (partition by crm_id) as cnt
                                                        from (select  f7m_id_old
                                                                     ,case when crm_id is not null then crm_id  else f7m_id end f7m_id
                                                                     ,crmr_id
                                                                     ,transact_id
                                                                     ,max(crm_id) over (partition by  f7m_id_old) as crm_id
                                                                     ,s_name
                                                                     ,f_name
                                                                     ,l_name
                                                                     ,b_place
                                                                     ,b_date
                                                                     ,id_type
                                                                     ,id_series
                                                                     ,id_num
                                                                     ,id_source
                                                                     ,id_source_kod
                                                                     ,id_date
                                                                     ,inn
                                                                     ,d_address_country
                                                                     ,d_address_postalcode
                                                                     ,d_address_regionname
                                                                     ,d_address_districtname
                                                                     ,d_address_cityname
                                                                     ,d_address_villagename
                                                                     ,d_address_shortvillagetypename
                                                                     ,d_address_streetname
                                                                     ,d_address_shortstreettypename
                                                                     ,d_address_housenumber
                                                                     ,d_address_buildingnumber
                                                                     ,d_address_blocknumber
                                                                     ,d_address_flatnumber
                                                                     ,f_address_country
                                                                     ,f_address_postalcode
                                                                     ,f_address_regionname
                                                                     ,f_address_districtname
                                                                     ,f_address_cityname
                                                                     ,f_address_villagename
                                                                     ,f_address_shortvillagetypename
                                                                     ,f_address_streetname
                                                                     ,f_address_shortstreettypename
                                                                     ,f_address_housenumber
                                                                     ,f_address_buildingnumber
                                                                     ,f_address_blocknumber
                                                                     ,f_address_flatnumber
                                                                     ,snils
                                                                     ,h_phone_phonenumber
                                                                     ,w_phone_phonenumber
                                                                     ,m_phone_phonenumber
                                                                     ,udbo_agree_date
                                                                     ,udbo_agree_no
                                                                     ,position_eio
                                                                     ,email
                                                                     ,func_division
                                                                     ,ter_division
                                                                     ,km_tb
                                                                     ,exec_id
                                                                     ,row_number () over(
                                                                        		partition by f7m_id_old --Дедубликация по f7m_id_old
                                                                            order by
                                                                                     id_date desc
                                                                                    ,case when s_name is not null then 1 else 2 end
                                                                                    ,case when f_name is not null then 1 else 2 end
                                                                                    ,case when l_name is not null then 1 else 2 end
                                                                                    ,case when id_series is not null then 1 else 2 end
                                                                                    ,case when id_num is not null then 1 else 2 end
                                                                                    ,case when b_place is not null then 1 else 2 end
                                                                                    ,case when b_date is not null then 1 else 2 end
                                                                                    ,case when id_type is not null then 1 else 2 end
                                                                                    ,case when id_source is not null then 1 else 2 end
                                                                                    ,case when id_source_kod is not null then 1 else 2 end
                                                                                    ,case when position_eio is not null then 1 else 2 end
                                                                                    ,case when email is not null then 1 else 2 end
                                                                                    ,case when func_division is not null then 1 else 2 end
                                                                                    ,case when ter_division is not null then 1 else 2 end
                                                                                    ,case when km_tb is not null then 1 else 2 end
                                                                                    ,case when snils is not null then 1 else 2 end
                                                                                    ,case when udbo_agree_date is not null then 1 else 2 end
                                                                                    ,case when udbo_agree_no is not null then 1 else 2 end
                                                                                   ) as rn
                                                        from $Nodet_team_k7m_aux_d_CLF_prepOUT ) t
                                                        where t.rn = 1
                   ) t2
         where t2.rn_2 = 1
     """
    )
    // .write
    // .format("parquet")
    // .mode(SaveMode.Overwrite)
    // .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_CLFOUT")

    createHiveTableStage4
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPrepPath}_2")
      .saveAsTable(s"${Nodet_team_k7m_aux_d_CLF_prepOUT}_2")

    import spark.implicits._

    createHiveTableStage4
      //      .filter($"rn" === 1)
      .drop("rn_2","f7m_id_old")
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_CLFOUT")


    //Сохранение clf_keys в clf_keys_old

    val createHiveTableStage5 = spark.sql(
      s"""
    select
         link_id
        ,f7m_id
     from $Node3t_team_k7m_aux_d_CLFIN
    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardKeysOldPath).saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keys_oldOUT")

    // Обновление f7m_id в clf_keys

    val createHiveTableStage6 = spark.sql(
      s"""
    select distinct
         k.link_id
        ,p.f7m_id
     from $Nodet_team_k7m_aux_d_clf_keys_oldOUT k
         join  ${Nodet_team_k7m_aux_d_CLF_prepOUT}_2 p on p.f7m_id_old = k.f7m_id
    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path",dashboardKeysNewPath ).saveAsTable(s"$Node3t_team_k7m_aux_d_CLFIN")

    logInserted()
  }
}




