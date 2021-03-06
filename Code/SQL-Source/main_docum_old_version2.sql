﻿create table user_iyarukov.innimport190417_t17_1 stored as orc
as
select * 
from (select  id
  ,C_INN
  ,odsopc
  ,row_number() over (partition by id order by odsvalidfrom desc, odssequenceid desc ) as rn
  from core_internal_eks.Z_CLIENT  a 
)t
 where t.rn=1 
 and t.C_INN in (Select inn from user_iyarukov.for_transact190417_till2017)
 ;

create table user_iyarukov.innimport190417_t17_2 stored as orc
as 
Select a.* 
from (
                               select  
                                               z_ac_fin.id                                               
                                               ,z_ac_fin.c_client_v
                                               ,from_unixtime(cast(z_ac_fin.odsvalidfrom / 1000 as int)) as odsvalidfrom
                                               ,z_ac_fin.odsopc 
                                               ,z_ac_fin.odssequenceid
                                               --для фильтрации истории добавим счетчик записей
                                               ,row_number() over (partition by id order by from_unixtime(cast(z_ac_fin.odsvalidfrom / 1000 as int)) desc) as rn
                               from core_internal_eks.z_ac_fin
                               where 1=1
                               ) a
where 1=1
--нужна только последняя версия 
and a.rn = 1
and a.c_client_v in (Select id from user_iyarukov.innimport190417_t17_1)
;

create table user_iyarukov.innimport190417_t17_3 stored as orc
as select 
v.id,
'0' as ktdt, 
v.sn,
v.su,
v.class_id,
v.state_id,
v.c_acc_dt,
v.c_acc_kt,
from_unixtime(cast(v.c_date_doc / 1000 as int)) as c_date_doc,
from_unixtime(cast(v.c_date_pl / 1000 as int)) as c_date_pl,
from_unixtime(cast(v.c_date_prov / 1000 as int)) c_date_prov,
from_unixtime(cast(v.c_date_val / 1000 as int)) as c_date_val,
v.c_kl_dt_0,
v.c_kl_dt_1_1,
v.c_kl_dt_1_2,
v.c_kl_dt_2_1,
v.c_kl_dt_2_inn,
v.c_kl_dt_2_2,
v.c_kl_dt_2_3,
v.c_kl_dt_2_friends,
v.c_kl_dt_2_part,
v.c_kl_dt_2_kpp,
v.c_kl_kt_0,
v.c_kl_kt_1_1,
v.c_kl_kt_1_2,
v.c_kl_kt_2_1,
v.c_kl_kt_2_inn,
v.c_kl_kt_2_2,
v.c_kl_kt_2_3,
v.c_kl_kt_2_friends,
v.c_kl_kt_2_part,
v.c_kl_kt_2_kpp,
v.c_nazn,
v.c_prioritet,
cast(v.c_sum as decimal(23,5)) as c_sum,
v.c_valuta,
v.c_vid_doc,
v.c_kod_nazn_pay,
v.c_doc_send_ref,
v.c_vid_oborota,
v.c_prov_user,
v.c_sum_kspl,
v.c_acc_oper,
v.c_num_ks,
v.c_valuta_po,
cast(v.c_sum_po as decimal(23,5)) as c_sum_po,
v.c_multicurr,
cast(v.c_sum_nt as decimal(23,5)) as c_sum_nt,
v.c_product_dt_acc_prod,
v.c_product_dt_acc_req,
v.c_product_dt_fact_oper,
v.c_product_dt_summa,
v.c_product_dt_acc_doc,
v.c_num_ks1,
v.c_product_ct_acc_prod,
v.c_product_ct_acc_req,
v.c_product_ct_fact_oper,
v.c_product_ct_summa,
v.c_product_ct_acc_doc,
v.c_date_send,
v.c_req_man_name,
v.c_req_man_passport,
v.c_req_man_seria,
v.c_req_man_number,
v.c_req_man_date_v,
v.c_req_man_who_v,
v.c_req_man_rezident,
v.c_req_man_addr,
v.c_req_man_birthday,
v.c_req_man_place,
v.c_req_man_country,
v.c_req_man_birth_place,
v.c_dpp,
v.c_quit_doc,
v.c_labels_client,
v.c_labels_list,
v.c_labels_req_cl_name,
v.c_labels_req_cl_passport,
v.c_labels_req_cl_seria,
v.c_labels_req_cl_number,
v.c_labels_req_cl_date_v,
v.c_labels_req_cl_who_v,
v.c_labels_req_cl_rezident,
v.c_labels_req_cl_addr,
v.c_labels_req_cl_birthday,
v.c_labels_req_cl_place,
v.c_labels_req_cl_country,
v.c_labels_req_cl_birth_place,
v.c_num_check,
v.c_series,
v.c_i93_op_type,
v.c_type_mess,
v.c_budget_payment,
v.c_nochange_acc,
v.c_to_zp,
v.c_code_doc,
v.c_cond_pay_memo,
v.c_rate_dt,
v.c_rate_kt,
v.c_sbe_razn_sum,
v.c_for_svod,
v.c_tune_delta_0,
v.c_tune_delta_oper_sc_minus,
v.c_tune_delta_oper_sc_plus,
v.c_date_give,
v.c_rc_send_date,
v.c_rc_user_created,
v.c_rc_signed_by,
v.c_not_calc,
v.c_date_note,
v.c_date_card,
v.c_date_debet,
v.c_bud_reqs_date_nu,
v.c_bud_reqs_num_exp,
v.c_bud_reqs_date_exp,
v.c_bud_reqs_kbk_str,
v.c_bud_reqs_kod_nazn_pay_str,
v.c_bud_reqs_okato_str,
v.c_bud_reqs_taxpayer_str,
v.c_bud_reqs_type_exp_str,
v.c_date_inp,
v.c_auto_proc,
v.c_key,
v.c_sbp_archive,
v.c_curr_oper,
v.c_num_dt,
v.c_deal_pasp,
v.c_num_kt,
v.c_main_smart,
v.c_sbp_serv,
v.c_val_doc_reqs,
v.c_kass_out_user,
v.c_sbe_shiv,
v.c_sbp_terror,
v.c_sbe_shiv_gr,
v.c_document_num,
v.c_document_date,
v.c_document_user,
v.c_date_exec,
v.c_history_state,
v.c_in_folder,
v.c_document_uno,
v.c_edition_uno,
v.c_dt_send,
v.c_kt_send,
v.c_astr_date_prov,
v.c_user_exec,
v.c_text_vozv,
v.c_pachka,
v.c_subdocuments,
v.c_filial,
v.c_depart,
v.c_comment,
v.c_storno_doc,
v.c_correction_doc,
v.c_vto_marker,
v.odsvalidfrom,
v.odsopc,
v.odssequenceid,
v.odsopc_pk,
case when upper(v.c_nazn) like '%ЭЛЕКТРО%' then 'Э'
     when upper(v.c_nazn) like '%РЖД%' then 'RGD'
     when upper(v.c_nazn) like '%ЖЕЛЕЗНЫЕ ДОР%' then 'RGD'
     when upper(v.c_nazn) like '%ТРАНСНЕФТЬ%' then 'ТРАНСНЕФТЬ'
     else null
end  flg1,
case 
    when lower(v.c_nazn) like '%возврат%'
         and lower(v.c_nazn) not like ('%эквайринг%')
         and upper(v.c_nazn) not rlike ('ЗА(Й|Ё|Е)М|LOAN|INTEREST|ФИНАНС.+ПОМОЩ|МЕРЧАНТ')
         and substr(v.c_kl_dt_2_1,1,3) <> '706'
         and substr(v.c_kl_dt_2_1,1,5) <> '40101'
    then 'ВОЗВРАТ'

    when upper(v.c_nazn) rlike ('ЗА(Й|Ё|Е)М|LOAN|INTEREST|ФИНАНС.+ПОМОЩ|МЕРЧАНТ')
    then 'Займ'

    when substr(v.c_kl_dt_2_1,1,3) between '441' and '456'
         and substr(v.c_kl_dt_2_1,1,3) <> '455'
         and substr(v.c_kl_dt_2_1,4,2) <> '15'
         and (coalesce(v.c_kl_dt_2_inn,'&') = '7707083893' or coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9'))
    then 'Кредит'

    when (substr(v.c_kl_dt_2_1,1,1) = '3' or substr(v.c_kl_dt_2_1,1,2) in ('40','41') or substr(v.c_kl_dt_2_1,1,5) = '47426')
          and  substr(v.c_kl_dt_2_1,1,5) <> '40906' -- счет получателя  не начинается на 40906
          and coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9')
          and mbik.FLDBIC is not null -- есть в списке БИКов Банка
    then 'Пополнение.Сбер'

    when (substr(v.c_kl_dt_2_1,1,1) = '3' or substr(v.c_kl_dt_2_1,1,2) in ('40','41') or substr(v.c_kl_dt_2_1,1,5) = '47426')
          and  substr(v.c_kl_dt_2_1,1,5) <> '40906' -- счет получателя  не начинается на 40906
          and coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9')
           and mbik.FLDBIC is null   -- нет в списке БИКов Банка
    then 'Пополнение.НЕ Сбер'

    when (substr(v.c_kl_dt_2_1,1,3) in ('421','422','425')
         and coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9'))
            or
         (substr(v.c_kl_dt_2_1,1,5) = '47426'
         and coalesce(v.c_kl_dt_2_inn,'&') <> coalesce(v.c_kl_kt_2_inn,'&'))
    then 'ДЕПОЗИТЫ'

    when  substr(v.c_kl_dt_2_1,1,5) in ('47407','47408')
          and (coalesce(v.c_kl_dt_2_inn,'&') = '7707083893' or coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9'))
    then 'ВАЛЮТНЫЕ ОПЕРАЦИИ'

    when  (substr(v.c_kl_dt_2_1,1,2) in ('52')
           and (coalesce(v.c_kl_dt_2_inn,'&') = '7707083893' or coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9')))
             or
          (substr(v.c_kl_dt_2_1,1,2) in ('30','40')
           and coalesce(v.c_kl_dt_2_inn,'&') <>  coalesce(v.c_kl_kt_2_inn,'&')
           and upper(v.c_nazn) rlike '%ВЕКСЕЛ(Ь|Я)%')
    then 'ВЕКСЕЛЯ'

    when  substr(v.c_kl_dt_2_1,1,2) in ('401')
          and  upper(v.c_nazn) rlike 'СУБ.|СУБС.|СУБСИД'
    then 'СУБСИДИИ'

    when  substr(v.c_kl_dt_2_1,1,2) in ('401')
          and  upper(v.c_nazn) not rlike 'СУБ.|СУБС.|СУБСИД'
    then 'ВОЗВРАТ НАЛОГОВ'

    when  upper(v.c_nazn)  rlike 'ШТРАФ|ПЕНИ|НЕУСТОЙК'
          and coalesce(v.c_kl_dt_2_inn,'8') <>  coalesce(v.c_kl_kt_2_inn,'9')
    then 'ШТРАФЫ'

    when  upper(v.c_nazn)  rlike 'ДИВИДЕНД'
          and coalesce(v.c_kl_dt_2_inn,'8') <>  coalesce(v.c_kl_kt_2_inn,'9')
    then 'ДИВИДЕНДЫ'

    when  substr(v.c_kl_dt_2_1,1,5) in ('70608','99999')
    then 'СИСТЕМНЫЕ'

    when  upper(v.c_nazn)  rlike 'ОТСТУПНО(М|Е|ГО)|ЦЕССИ(Я|И|ЕЙ)'
    then 'ПРОЧИЕ'
                else 'ВЫРУЧКА'
end   as kras,
a.c_client_v

from core_internal_eks.z_main_docum_v2  v
inner join user_iyarukov.innimport190417_t17_2  a
                on (a.id = v.c_acc_kt)
left join core_internal_eks.z_cl_bank_n plbic on plbic.id = v.c_kl_dt_2_3
left join (select distinct FLDBIC from  user_iyarukov.bik_num_id) mbik -- список БИКов Сбербанка
    on mbik.FLDBIC = plbic.c_bic
                
UNION ALL

select 
v.id,
'1' as ktdt, 
v.sn,
v.su,
v.class_id,
v.state_id,
v.c_acc_dt,
v.c_acc_kt,
from_unixtime(cast(v.c_date_doc / 1000 as int)) as c_date_doc,
from_unixtime(cast(v.c_date_pl / 1000 as int)) as c_date_pl,
from_unixtime(cast(v.c_date_prov / 1000 as int)) c_date_prov,
from_unixtime(cast(v.c_date_val / 1000 as int)) as c_date_val,
v.c_kl_dt_0,
v.c_kl_dt_1_1,
v.c_kl_dt_1_2,
v.c_kl_dt_2_1,
v.c_kl_dt_2_inn,
v.c_kl_dt_2_2,
v.c_kl_dt_2_3,
v.c_kl_dt_2_friends,
v.c_kl_dt_2_part,
v.c_kl_dt_2_kpp,
v.c_kl_kt_0,
v.c_kl_kt_1_1,
v.c_kl_kt_1_2,
v.c_kl_kt_2_1,
v.c_kl_kt_2_inn,
v.c_kl_kt_2_2,
v.c_kl_kt_2_3,
v.c_kl_kt_2_friends,
v.c_kl_kt_2_part,
v.c_kl_kt_2_kpp,
v.c_nazn,
v.c_prioritet,
cast(v.c_sum as decimal(23,5)) as c_sum,
v.c_valuta,
v.c_vid_doc,
v.c_kod_nazn_pay,
v.c_doc_send_ref,
v.c_vid_oborota,
v.c_prov_user,
v.c_sum_kspl,
v.c_acc_oper,
v.c_num_ks,
v.c_valuta_po,
cast(v.c_sum_po as decimal(23,5)) as c_sum_po,
v.c_multicurr,
cast(v.c_sum_nt as decimal(23,5)) as c_sum_nt,
v.c_product_dt_acc_prod,
v.c_product_dt_acc_req,
v.c_product_dt_fact_oper,
v.c_product_dt_summa,
v.c_product_dt_acc_doc,
v.c_num_ks1,
v.c_product_ct_acc_prod,
v.c_product_ct_acc_req,
v.c_product_ct_fact_oper,
v.c_product_ct_summa,
v.c_product_ct_acc_doc,
v.c_date_send,
v.c_req_man_name,
v.c_req_man_passport,
v.c_req_man_seria,
v.c_req_man_number,
v.c_req_man_date_v,
v.c_req_man_who_v,
v.c_req_man_rezident,
v.c_req_man_addr,
v.c_req_man_birthday,
v.c_req_man_place,
v.c_req_man_country,
v.c_req_man_birth_place,
v.c_dpp,
v.c_quit_doc,
v.c_labels_client,
v.c_labels_list,
v.c_labels_req_cl_name,
v.c_labels_req_cl_passport,
v.c_labels_req_cl_seria,
v.c_labels_req_cl_number,
v.c_labels_req_cl_date_v,
v.c_labels_req_cl_who_v,
v.c_labels_req_cl_rezident,
v.c_labels_req_cl_addr,
v.c_labels_req_cl_birthday,
v.c_labels_req_cl_place,
v.c_labels_req_cl_country,
v.c_labels_req_cl_birth_place,
v.c_num_check,
v.c_series,
v.c_i93_op_type,
v.c_type_mess,
v.c_budget_payment,
v.c_nochange_acc,
v.c_to_zp,
v.c_code_doc,
v.c_cond_pay_memo,
v.c_rate_dt,
v.c_rate_kt,
v.c_sbe_razn_sum,
v.c_for_svod,
v.c_tune_delta_0,
v.c_tune_delta_oper_sc_minus,
v.c_tune_delta_oper_sc_plus,
v.c_date_give,
v.c_rc_send_date,
v.c_rc_user_created,
v.c_rc_signed_by,
v.c_not_calc,
v.c_date_note,
v.c_date_card,
v.c_date_debet,
v.c_bud_reqs_date_nu,
v.c_bud_reqs_num_exp,
v.c_bud_reqs_date_exp,
v.c_bud_reqs_kbk_str,
v.c_bud_reqs_kod_nazn_pay_str,
v.c_bud_reqs_okato_str,
v.c_bud_reqs_taxpayer_str,
v.c_bud_reqs_type_exp_str,
v.c_date_inp,
v.c_auto_proc,
v.c_key,
v.c_sbp_archive,
v.c_curr_oper,
v.c_num_dt,
v.c_deal_pasp,
v.c_num_kt,
v.c_main_smart,
v.c_sbp_serv,
v.c_val_doc_reqs,
v.c_kass_out_user,
v.c_sbe_shiv,
v.c_sbp_terror,
v.c_sbe_shiv_gr,
v.c_document_num,
v.c_document_date,
v.c_document_user,
v.c_date_exec,
v.c_history_state,
v.c_in_folder,
v.c_document_uno,
v.c_edition_uno,
v.c_dt_send,
v.c_kt_send,
v.c_astr_date_prov,
v.c_user_exec,
v.c_text_vozv,
v.c_pachka,
v.c_subdocuments,
v.c_filial,
v.c_depart,
v.c_comment,
v.c_storno_doc,
v.c_correction_doc,
v.c_vto_marker,
v.odsvalidfrom,
v.odsopc,
v.odssequenceid,
v.odsopc_pk,
case when upper(v.c_nazn) like '%ЭЛЕКТРО%' then 'Э'
     when upper(v.c_nazn) like '%РЖД%' then 'RGD'
     when upper(v.c_nazn) like '%ЖЕЛЕЗНЫЕ ДОР%' then 'RGD'
     when upper(v.c_nazn) like '%ТРАНСНЕФТЬ%' then 'ТРАНСНЕФТЬ'
     else null
end  flg1,
case 
    when upper(v.c_nazn) rlike ('ЗА(Й|Ё|Е)М|LOAN|INTEREST|ФИНАНС.+ПОМОЩ')
    then 'Займ'
                
    when  upper(v.c_nazn)  rlike 'ДИВИДЕНД'
          and coalesce(v.c_kl_dt_2_inn,'9') <>  coalesce(v.c_kl_kt_2_inn,'8')
    then 'ДИВИДЕНДЫ'
                
    when  upper(v.c_nazn)  rlike 'ШТРАФ|ПЕНИ|НЕУСТОЙК'
          and coalesce(v.c_kl_dt_2_inn,'9') <>  coalesce(v.c_kl_kt_2_inn,'8')
    then 'ШТРАФЫ'
                
    when lower(v.c_nazn) like '%возврат%'
          and coalesce(v.c_kl_dt_2_inn,'9') <>  coalesce(v.c_kl_kt_2_inn,'8')
   then 'ВОЗВРАТ'           
                
    when  substr(v.c_kl_kt_2_1,1,5) = '40101'
                                 and upper(v.c_nazn) rlike 'НДФЛ|ДОХОД.+ФИЗИЧЕСК'
                                 and upper(v.c_nazn) not rlike 'ПЕНИ'
    then 'Налог.НДФЛ'    
                
    when  substr(v.c_kl_kt_2_1,1,5) = '40101'
                                 and upper(v.c_nazn) rlike 'ПРИБЫЛ'
                                 and upper(v.c_nazn) not rlike 'ПЕНИ'
    then 'Налог.ПРИБЫЛЬ'
                
    when  substr(v.c_kl_kt_2_1,1,5) = '40101'
                                 and upper(v.c_nazn) rlike 'ПРИБЫЛ'
                                 and upper(v.c_nazn) not rlike 'АРЕНД|ОХРАН'
    then 'Налог.ПРОЧЕЕ'                

    when  (coalesce(substr(v.c_kl_kt_2_1,1,2),'11') in ('52')
           and (coalesce(v.c_kl_dt_2_inn,'&') <> '7707083893' and  coalesce(v.c_kl_dt_2_inn,'8') <> coalesce(v.c_kl_kt_2_inn,'9')))
             or
          (coalesce(substr(v.c_kl_kt_2_1,1,1),'4') in ('3','4')
           and coalesce(v.c_kl_dt_2_inn,'&') <>  coalesce(v.c_kl_kt_2_inn,'&')
           and upper(v.c_nazn) rlike '%ВЕКСЕЛ(Ь|Я)%')
    then 'ВЕКСЕЛЯ'            

    when  coalesce(v.c_kl_dt_2_inn,'8') <> coalesce(v.c_kl_kt_2_inn,'9')
                                  and upper(v.c_nazn) rlike 'АРЕНД(А|У|Ы|НЫЕ|НЫЙ|НАЯ)'
    then 'АРЕНДА'             

    when substr(v.c_kl_kt_2_1,1,3) between '441' and '456'
         and substr(v.c_kl_kt_2_1,1,3) <> '455'
         and substr(v.c_kl_kt_2_1,4,2) <> '15'
         and (coalesce(v.c_kl_kt_2_1,'&') = '7707083893' or coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9'))
    then 'Кредит'

    when coalesce(substr(v.c_kl_kt_2_1,1,5),'1') = '47427'
    then '%% по Кредиту'

    when coalesce(substr(v.c_kl_kt_2_1,1,3),'1') in ('458','459') or coalesce(substr(v.c_kl_kt_2_1,1,5),'1') = '901604'
    then 'ПРОСРОЧКА'

                when (coalesce(substr(v.c_kl_kt_2_1,1,3),'1') = 423 or coalesce(substr(v.c_kl_kt_2_1,1,5),'1') in ('30232','40817','40820','47422'))
                and upper(v.c_nazn) rlike 'ЗАР(АБОТН)?(\.\s*)?ПЛАТ|ЗАЧ(ИСЛЕНИЕ|\.)?\s+З/П|АЛИМЕНТ(Ы|ОВ)|НЕТРУДОСПОСОБ|ОТПУСКНЫ|ПОСОБИ(Е|Я)'
                then 'ЗАРПЛАТА'
                
                when coalesce(substr(v.c_kl_kt_2_1,1,3),'1') = 423 or coalesce(substr(v.c_kl_kt_2_1,1,5),'1') in ('30232','40817','40820','47422')
                then 'Перечесление на ФЛ'

                when coalesce(v.c_kl_dt_2_inn,'8') = coalesce(v.c_kl_kt_2_inn,'9')
                and (substr(v.c_kl_kt_2_1,1,1) = '3' or substr(v.c_kl_kt_2_1,1,2) in ('40','41'))
                then 'Перевод собств средств'

    when  substr(v.c_kl_kt_2_1,1,5) in ('47407','47408')
    then 'ВАЛЮТНЫЕ ОПЕРАЦИИ'            
                
    when substr(v.c_kl_kt_2_1,1,3) in ('421','422','425')
                then 'ДЕПОЗИТЫ'
                
    when substr(v.c_kl_kt_2_1,1,5) in ('70603')
                then 'СИСТЕМНЫЕ'


else 'ПРОЧИЕ' end  as kras,
a.c_client_v

from core_internal_eks.z_main_docum_v2  v
inner join user_iyarukov.innimport190417_t17_2  a
                on (a.id = v.c_acc_dt)
;

create table user_iyarukov.innimport190417_t17_4 stored as orc
as 
SELECT A.*
,B.C_CUR_ATTR_CUR_P_2UNIT AS C_VALUTA_LABEL
,C.C_CUR_ATTR_CUR_P_2UNIT AS c_valuta_po_LABEL
,D.C_NAME AS c_vid_doc_LABEL
,E.C_MEMO_NAZN AS c_kod_nazn_pay_LABEL
,F.C_VALUE AS c_acc_oper_LABEL
,g.c_inn as inn_st
,h.c_shortlabel as c_filial_label
,i.c_value as c_req_man_country_label
,case when c_vid_oborota=1986062 then 'Основные обороты'
   when c_vid_oborota=1986063 then 'Эмиссионные обороты'
   when c_vid_oborota=1986064 then 'Заключительные обороты'
   when c_vid_oborota=2991942620 then 'СПОД'
   else null
 end as c_vid_oborota_label
,case when c_multicurr=1920418 then 'Область покрытия плательщика'
   when c_multicurr=1920419 then 'Область покрытия  получателя'
   when c_multicurr=1920420 then 'Мультивалютность'
   when c_multicurr=1920421 then 'Область национального покрытия'
   else null
 end as c_multicurr_label
,k.c_vid_send as Z_VID_SEND_DOCUM_label
,row_number() over (partition by a.id,ktdt order by a.odsvalidfrom desc, a.odssequenceid desc, a.odsopc desc) as rn
FROM user_iyarukov.innimport190417_t17_3  A 
LEFT JOIN core_internal_eks.Z_FT_MONEY B ON A.C_VALUTA=B.ID
LEFT JOIN core_internal_eks.Z_FT_MONEY C ON A.c_valuta_po=C.ID
LEFT JOIN core_internal_eks.z_name_paydoc D on A.C_VID_DOC=D.id
LEFT JOIN core_internal_eks.Z_KOD_N_PAY E ON A.c_kod_nazn_pay=E.id
LEFT JOIN core_internal_eks.Z_ACC_OPER F ON A.c_acc_oper=F.ID
inner join user_iyarukov.innimport190417_t17_1 g on a.c_client_v=g.id
left join core_internal_eks.Z_branch h on a.c_filial=h.id
left join core_internal_eks.z_country i on a.c_req_man_country=i.id
left join core_internal_eks.Z_VID_SEND_DOCUM k on a.c_doc_send_ref=k.id
where upper(a.state_id)='PROV'
;


create table user_iyarukov.innimport190417_t17_5 stored as orc
as 
select * from user_iyarukov.innimport190417_t17_4
where rn=1;
 
 

drop table if exists user_iyarukov.dt1_hive010617;
create table user_iyarukov.dt1_hive010617 stored as orc as 
select id,ktdt, C_KL_DT_1_1,C_KL_DT_2_INN
from user_iyarukov.innimport190417_t17_5
where ktdt=0;

drop table if exists user_iyarukov.kt1_hive010617;
create table user_iyarukov.kt1_hive010617 stored as orc as 
select id,ktdt, C_KL_KT_1_1,C_KL_KT_2_INN
from user_iyarukov.innimport190417_t17_5
where ktdt=1;


drop table if exists user_iyarukov.distclient_hive010617;
create table user_iyarukov.distclient_hive010617 stored as orc as 
select distinct idclient from (
select  C_KL_DT_1_1 as idclient from  user_iyarukov.dt1_hive010617
union all
select  C_KL_KT_1_1 as idclient from  user_iyarukov.kt1_hive010617) aa;

drop table if exists user_iyarukov.innsec_hive010617;
create table user_iyarukov.innsec_hive010617 stored as orc
as
select * 
from (select  a.*
	,row_number() over (partition by id order by odsvalidfrom desc, odssequenceid desc) as rn
	from src_eks.Z_CLIENT  a 
	where a.id in (select idclient from user_iyarukov.distclient_hive010617 where idclient is not null)
)t
 where t.rn=1;


drop table if exists user_iyarukov.innimport190417_6; 
create table user_iyarukov.innimport190417_6 stored as orc as 
select a.*,coalesce(a.c_kl_kt_2_INN,b.c_inn) as inn_sec
from user_iyarukov.innimport190417_t17_5 a
left join user_iyarukov.innsec_hive010617 b on a.c_kl_kt_1_1=b.id 
where a.ktdt=1
union all
select a.*,coalesce(a.c_kl_dt_2_INN,c.c_inn) as inn_sec
from user_iyarukov.innimport190417_t17_5 a
left join user_iyarukov.innsec_hive010617 c on a.c_kl_dt_1_1=c.id 
where a.ktdt=0 
;
