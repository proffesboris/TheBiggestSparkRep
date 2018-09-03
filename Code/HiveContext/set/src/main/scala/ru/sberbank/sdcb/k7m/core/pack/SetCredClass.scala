package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Date
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetCredClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_set_credIN = s"${DevSchema}.t_cred_pc"
    val Node2t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_client"
    val Node3t_team_k7m_aux_set_credIN = s"${DevSchema}.basis_client"
    val Node4t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_cl_corp"
    val Node5t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_com_status_prd"
    val Node6t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_depart"
    val Node7t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_kind_credits"
    val Node8t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_types_cred"
    val Node9t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_obj_kred"
    val Node10t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_Z_branch"
    val Node11t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_object_cred"
    val Node13t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_properties"
    val Node14t_team_k7m_aux_set_credIN = s"${Stg0Schema}.eks_z_prod_property"
    val Nodet_team_k7m_aux_set_credOUT = s"${DevSchema}.set_cred"
    val dashboardPath = s"${config.auxPath}set_cred"


    override val dashboardName: String = Nodet_team_k7m_aux_set_credOUT //витрина
    override def processName: String = "SET"

    def DoSetCred(dt: String) {

      Logger.getLogger(Nodet_team_k7m_aux_set_credOUT).setLevel(Level.WARN)

      logStart()

      val readHiveTable1 = spark.sql(s"""
SELECT
    pc.C_Client as client_id,
    cc.C_INN as inn,
    cc.c_kpp as kpp,
    cc.C_NAME as client_name,
    if(cc.c_crm_segment='Средний','Средние',cc.c_crm_segment) as crm_segment, --Бизнес-сегмент
    pc.id as pr_cred_id,  --идентификатор договора (ВНИМАНИЕ! ЭТО ИДЕНТИФИКАТОР ВЕРХНЕГО УРОВНЯ!)
    pc.class_id as pr_cred_class_id,
    pc.collection_id as pr_cred_collection_ID,
    pc.c_num_dog as ndog,  --номер кр.договора
    coalesce(pc.c_date_begin,pc.c_date_begining) as ddog,  --дата кр.договора
    datediff(pc.C_DATE_ENDING,pc.c_date_begin) as dur_in_days,  --дюрация в днях
    months_between(pc.C_DATE_ENDING,pc.c_date_begin) as dur_in_month,  --дюрация в месяцах (ВНИМАНИЕ! может быть дробной)
    pc.C_DATE_ENDING as C_DATE_ENDING,
    pc.C_date_close as date_close,
    cl.c_register_date_reg,  --дата регистрации
    if(months_between(coalesce(pc.c_date_begin,pc.c_date_begining),Cl.c_register_date_reg)> 15 
           ,'Старая','Молодая') as age,  --разметка на молодые и старые компании (молодая = менее 15 месяцев с даты регистрации)
    pc.c_summa_dog as summa_base,  --сумма договора в валюте договора
    pc.c_summa_dog*pc.issue_course as summa_ru,  --сумма договора в рублях на дату договора
    pc.C_FT_CREDIT as C_FT_CREDIT,  --екс id валюты договора (точнее это валюта с головы договора. в его рамках могут быть мультивалютные транши)
    pc.c_cur_credit as summa_currency, --нименование валюты договора
    pc.c_limit_saldo as limit_amt,  --лимит по договору (поле не исследовалось)
    pc.c_cur_limit as limit_currency,  --наименование валюты лимита
    kc.C_NAME as regime_prod,  --код режима
    --INSERTED 11.04.18 TEMIN-LV START  
    if((pc.class_id == 'KRED_CORP') and (tc.C_SHORT_NAME =='CRED_CONT'),1,0) as cred_flag        
    ,if((pc.class_id == 'KRED_CORP') and (tc.C_SHORT_NAME =='CRED_LINE'),1,0) as nkl_flag        
    ,if((pc.class_id == 'KRED_CORP') and (tc.C_SHORT_NAME =='CRED_OVER'),1,0) as vkl_flag        
    ,if( pc.class_id == 'OVERDRAFTS',1,0) as over_flag
    ,if(pc.class_id == 'KRED_CORP','КД/НКЛ/ВКЛ', if(pc.class_id == 'OVERDRAFTS','ОВЕР','ERROR')) as INSTRUMENT,
    --INSERTED 11.04.18 TEMIN-LV END
    --общий инструмент
    tc.c_name,
    tc.c_short_name,
    datediff(pc.C_DATE_PAYOUT_LTD, coalesce(pc.c_date_begin,pc.c_date_begining)) as AVP_in_days,
    months_between(pc.C_DATE_PAYOUT_LTD, coalesce(pc.c_date_begin,pc.c_date_begining)) as AVP_in_month,
    nvl(ok.c_sys_name , zok.c_sys_name) as target_cred,  --цель кредитования по справочнику
    nvl(nvl(ok.c_sys_name , zok.c_sys_name),prop1.c_str) as target_cred2, --цель кредитования скрещенная справочник+ свободный ввод
    prod_prop.c_name as CR_TYPE26,  --вид кредитования
    pc.c_list_pay as  c_list_pay, 
    st.C_NAME as product_status,  --статус продукта
    b.c_prefix as branch ,  --филиал, код
    b.c_shortlabel as branch_ru, --филиал, наименовние
    cc.id as cc_id, 
    CL.id as cl_id, --Поменять на t_team_k7m_stg.eks_z_cl_corp
    st.id as st_id, 
    dep.id as dep_id, 
    kc.id as kc_id, 
    tc.id as tc_id, 
    ok.id as ok_id, 
    b.id as b_id, 
    zoc.id as zoc_id,
    zok.id as zok_id, 
    prop1.id as prop1_id,  
    prop2.id prop2_id, 
    prod_prop.id prod_prop_id
FROM 
   $Node1t_team_k7m_aux_set_credIN pc  -- A1_2
    inner join  $Node2t_team_k7m_aux_set_credIN AS cc --A3_1 Поменять на t_team_k7m_stg.eks_z_client
        on pc.C_CLIENT = cc.ID
    inner join $Node3t_team_k7m_aux_set_credIN bc
    on bc.org_inn_crm_num = cc.c_inn
    inner join $Node4t_team_k7m_aux_set_credIN AS CL --Поменять на t_team_k7m_stg.eks_z_cl_corp nternal_eks_ibs.z_cl_corp
        on cl.ID = cc.ID
    left join $Node5t_team_k7m_aux_set_credIN AS st-- A5_1
        on pc.C_COM_STATUS = st.ID
    left join $Node6t_team_k7m_aux_set_credIN AS dep--A14_1
     on pc.C_DEPART = dep.ID
    left join $Node7t_team_k7m_aux_set_credIN AS kc --A9_1
        on pc.C_KIND_CREDIT = kc.ID
    left join $Node8t_team_k7m_aux_set_credIN AS tc--A35_1
       on kc.c_reg_rules = tc.ID
     left join $Node9t_team_k7m_aux_set_credIN  AS ok --A15_1
        on pc.c_dic_obj_cred = ok.ID
     left join $Node10t_team_k7m_aux_set_credIN AS b--A31_1
        on pc.C_FILIAL = b.ID
     left join $Node11t_team_k7m_aux_set_credIN   AS zoc--L1_2
        on 
            zoc.c_main_obj = '1' 
            and zoc.collection_id   = pc.c_objects_cred --Убрать CAST после перевода zoc на нулевой слой!!!
     left join $Node9t_team_k7m_aux_set_credIN AS zok --L1_1
        on zok.id = zoc.c_obj_cred  --Убрать CAST после перевода zoc на нулевой слой!!!
     left join (
         select 
            max(pip.id) id,
            pip.collection_id,
            max(pip.c_str) c_str
         from $Node13t_team_k7m_aux_set_credIN pip
         where pip.c_group_prop = 1136892346138
                 and timestamp'${dt} 00:00:00' between pip.c_date_beg and coalesce(pip.c_date_end, timestamp'9999-02-01 00:00:00')
         group by    pip.collection_id           
            )  as prop1 --L3_4  заменить на  t_team_k7m_stg.eks_z_properties  
        on 
         prop1.collection_id = pc.c_properties
     left join  
        (select 
            max(pip.id) id,
            pip.collection_id,
            max(pip.c_prop) c_prop
         from $Node13t_team_k7m_aux_set_credIN pip
         where pip.c_group_prop = 64828905673
                 and timestamp'${dt} 00:00:00' between pip.c_date_beg and coalesce(pip.c_date_end, timestamp'9999-02-01 00:00:00')
         group by    pip.collection_id                
        ) as prop2 -- L3_5 заменить на  t_team_k7m_stg.eks_z_properties  
        on prop2.collection_id = pc.c_properties 
     left join $Node14t_team_k7m_aux_set_credIN as prod_prop--L3_6
        on prod_prop.id=prop2.c_prop
WHERE 
    pc.class_id in ('KRED_CORP','OVERDRAFTS')            
    And pc.c_date_begin>= to_date('2000-01-01') and pc.c_date_begin<=to_date('${dt}') and datediff(pc.C_DATE_ENDING,pc.c_date_begin) <=30.5*60
    and pc.c_high_level_cr is null --объект не входит в кредитную линию, т.е. не транш
    and pc.collection_id is null --объект не входит в рамочное соглашение, т.е. не транш  
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_set_credOUT")

      logInserted()
      logEnd()
    }
}

