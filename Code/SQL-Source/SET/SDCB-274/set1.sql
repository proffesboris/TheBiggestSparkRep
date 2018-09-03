

drop table t_team_k7m_aux_p.t_cred_pc   
/ 
drop table t_team_k7m_aux_p.set_cred
/
DROP table t_team_k7m_aux_p.set_factop_1
/
drop table t_team_k7m_aux_p.set_factop_2
/
drop table t_team_k7m_aux_p.set_factop
/
drop table t_team_k7m_aux_p.set_cred_filtered 
/
Drop table t_team_k7m_aux_p.set_paym
/
Drop table t_team_k7m_aux_p.set_give
/
drop table t_team_k7m_aux_p.set_wro
/
drop table t_team_k7m_aux_p.set_cred_active
/
drop table t_team_k7m_stg.test_ratings2
/
drop table t_team_k7m_aux_p.set_cred_rating
/
drop table t_team_k7m_aux_p.set_cred_rep1
/
drop table t_team_k7m_aux_p.set_cred_rep2
/
drop table t_team_k7m_aux_p.set_cred_rep3
/
drop table t_team_k7m_aux_p.set_cred_rep4
/
drop table t_team_k7m_aux_p.set_cred_rep5
/
drop table t_team_k7m_aux_p.set_cred_rep6
/
drop table t_team_k7m_aux_p.set_cred_rep1d
/
drop table t_team_k7m_aux_p.set_cred_rep2d
/
drop table t_team_k7m_aux_p.set_cred_enriched
/
drop  table t_team_k7m_aux_p.set_z_sur
/
drop table t_team_k7m_aux_p.set_suretyships_stat_nc


--STEP 1

-- Пока руками из /user/team/corp_risks/temin_lv/prov_type/
-- например tbl.write.format("parquet").mode('overwrite').saveAsTable(c_tbl_name)
--Потом будем брать из RDM
--create table t_team_k7m_stg.rdm_set_prov_type_mast
--stored as parquet
--as
--select * from t_team_k7m_stg.prov_type

    
    
create table t_team_k7m_aux_p.t_cred_pc
stored as parquet
as 
    select 
        pc.*,
        crs.course issue_course,
        crs.c_cur_short as c_cur_credit,
        ml.c_cur_short c_cur_limit
    from (
    --EXPLAIN
        select 
            p.class_id,
            p.id,
            c.c_properties ,
            c.C_CLIENT ,
            p.C_COM_STATUS ,
            c.C_DEPART,
            c.C_KIND_CREDIT ,
            c.c_dic_obj_cred ,
            c.C_FILIAL,
            c.c_objects_cred,
            c.C_FT_LIMIT ,
            c.C_FT_CREDIT,
            p.c_date_begin,
            p.c_date_begining,
            c.c_high_level_cr,
            c.collection_id,
            p.C_DATE_ENDING,
            p.c_num_dog ,
            p.C_date_close,
            c.c_summa_dog,
            c.c_limit_saldo,
            c.C_DATE_PAYOUT_LTD,
            c.c_list_pay
        from     
            internal_eks_ibs.z_product p --t_team_k7m_stg.eks_z_product 
            inner join internal_eks_ibs.z_pr_cred AS c --A1_1  t_team_k7m_stg.eks_z_pr_cred 
            on c.ID =p.ID 
         ) pc
        join t_team_k7m_aux_d.k7m_cur_courses crs  -- L4_5 Считается для эк.связей  
        on crs.id = pc.C_FT_CREDIT  
        left join t_team_k7m_stg.eks_z_ft_money ml
        on pc.C_FT_LIMIT = ml.ID
        where coalesce(pc.c_date_begin,pc.c_date_begining)  between crs.from_dt and crs.to_dt
   
    
--STEP2
create table t_team_k7m_aux_p.set_cred
stored as parquet
as 
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
    t_team_k7m_aux_p.t_cred_pc pc  -- A1_2  
    inner join  t_team_k7m_stg.eks_z_client AS cc --A3_1 Поменять на t_team_k7m_stg.eks_z_client 
        on pc.C_CLIENT = cc.ID
    inner join t_team_k7m_aux_p.basis_client bc
    on bc.org_inn_crm_num = cc.c_inn
    inner join t_team_k7m_stg.eks_z_cl_corp AS CL --Поменять на t_team_k7m_stg.eks_z_cl_corp nternal_eks_ibs.z_cl_corp
        on cl.ID = cc.ID
    left join t_team_k7m_stg.eks_z_com_status_prd AS st-- A5_1 
        on pc.C_COM_STATUS = st.ID
    left join t_team_k7m_stg.eks_z_depart AS dep--A14_1 
     on pc.C_DEPART = dep.ID
    left join t_team_k7m_stg.eks_z_kind_credits AS kc --A9_1 
        on pc.C_KIND_CREDIT = kc.ID
    left join t_team_k7m_stg.eks_z_types_cred AS tc--A35_1 
       on kc.c_reg_rules = tc.ID
     left join t_team_k7m_stg.eks_z_obj_kred  AS ok --A15_1 
        on pc.c_dic_obj_cred = ok.ID
     left join t_team_k7m_stg.eks_Z_branch AS b--A31_1 
        on pc.C_FILIAL = b.ID
     left join t_team_k7m_stg.eks_z_object_cred   AS zoc--L1_2 
        on 
            zoc.c_main_obj = '1' 
            and zoc.collection_id   = pc.c_objects_cred --Убрать CAST после перевода zoc на нулевой слой!!!
     left join t_team_k7m_stg.eks_z_obj_kred AS zok --L1_1 
        on zok.id = zoc.c_obj_cred  --Убрать CAST после перевода zoc на нулевой слой!!!
     left join (
         select 
            max(pip.id) id,
            pip.collection_id,
            max(pip.c_str) c_str
         from t_team_k7m_stg.eks_z_properties pip
         where pip.c_group_prop = 1136892346138
                 and timestamp'2018-02-01 00:00:00' between pip.c_date_beg and coalesce(pip.c_date_end, timestamp'9999-02-01 00:00:00')          
         group by    pip.collection_id           
            )  as prop1 --L3_4  заменить на  t_team_k7m_stg.eks_z_properties  
        on 
         prop1.collection_id = pc.c_properties
     left join  
        (select 
            max(pip.id) id,
            pip.collection_id,
            max(pip.c_prop) c_prop
         from t_team_k7m_stg.eks_z_properties pip
         where pip.c_group_prop = 64828905673
                 and timestamp'2018-02-01 00:00:00' between pip.c_date_beg and coalesce(pip.c_date_end, timestamp'9999-02-01 00:00:00')
         group by    pip.collection_id                
        ) as prop2 -- L3_5 заменить на  t_team_k7m_stg.eks_z_properties  
        on prop2.collection_id = pc.c_properties 
     left join t_team_k7m_stg.eks_z_prod_property as prod_prop--L3_6 
        on prod_prop.id=prop2.c_prop
WHERE 
    pc.class_id in ('KRED_CORP','OVERDRAFTS')            
    And pc.c_date_begin>= to_date('2000-01-01') and pc.c_date_begin<=to_date('2018-02-01') and datediff(pc.C_DATE_ENDING,pc.c_date_begin) <=30.5*60
    and pc.c_high_level_cr is null --объект не входит в кредитную линию, т.е. не транш
    and pc.collection_id is null --объект не входит в рамочное соглашение, т.е. не транш  
  
--  STEP3

set hive.auto.convert.join = true;
--part1
--3.32.


GO
create table t_team_k7m_aux_p.set_factop_1
stored as parquet
as 
--explain
select 
     fo.id,
     lcr.pr_cred_id as pr_cred_id,
     fo.c_date,
     fo.c_summa ,
     fo.c_valuta, 
     fo.c_reg_currency_sum,
     lcr.summa_currency as to_currency,
     lcr.C_FT_CREDIT,
     vd.c_sys_name,
     pt.vid,
     pt.ob
from 
   t_team_k7m_aux_p.set_cred as lcr 
   join t_team_k7m_stg.eks_z_pr_cred as cr 
      on lcr.pr_cred_id = cr.c_high_level_cr
   join internal_eks_ibs.z_fact_oper  fo --Вернуть 0 слой
     on fo.collection_id = cr.c_list_pay
   join t_team_k7m_stg.eks_z_vid_oper_dog vd 
      on vd.id = fo.c_oper 
   left join (
        SELECT
            nm,
            MAX(ob) AS ob,
            MAX(vid) as vid
        FROM t_team_k7m_stg.rdm_set_prov_type_mast
        group by nm 
        )  as pt  
    on pt.nm = lower(vd.c_sys_name)
GO
--part2
GO
create table t_team_k7m_aux_p.set_factop_2
stored as parquet
as 
select 
     fo.id,
     lcr.pr_cred_id as pr_cred_id,
     fo.c_date,
     fo.c_summa ,
     fo.c_valuta, 
     fo.c_reg_currency_sum,
     lcr.summa_currency as to_currency,
     lcr.C_FT_CREDIT,
     vd.c_sys_name,
     pt.vid,
     pt.ob,
     pt.nm,
     fo.c_oper
from 
   t_team_k7m_aux_p.set_cred as lcr 
   join internal_eks_ibs.z_fact_oper  fo --Вернуть 0 слой
     on fo.collection_id = lcr.c_list_pay
   join t_team_k7m_stg.eks_z_vid_oper_dog vd 
      on vd.id = fo.c_oper 
   left join
        (
        SELECT
            nm,
            MAX(ob) AS ob,
            MAX(vid) as vid
        FROM t_team_k7m_stg.rdm_set_prov_type_mast
        group by nm 
        )  as pt 
    on pt.nm = lower(vd.c_sys_name)
GO
-- result
--2m

create table t_team_k7m_aux_p.set_factop
stored as parquet
as 
select
        id,
       c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob   
from     (
    select 
        id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob   
    from t_team_k7m_aux_p.set_factop_1
    where c_valuta=c_ft_credit
    union all
    select 
        id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        c_summa c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob   
    from t_team_k7m_aux_p.set_factop_2
    where c_valuta=c_ft_credit
    union all
    select 
        f.id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        (f.c_summa *crs1.course)/crs2.course c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob   
    from 
        t_team_k7m_aux_p.set_factop_1 f
        join t_team_k7m_aux_d.k7m_cur_courses crs1 -- L4_5 Считается для эк.связей  
            on crs1.id = f.c_valuta  
        join t_team_k7m_aux_d.k7m_cur_courses crs2 -- L4_5 Считается для эк.связей  
            on crs2.id = f.to_currency  
    where f.c_date  between crs1.from_dt and crs1.to_dt
       and f.c_date  between crs2.from_dt and crs2.to_dt
       and f.c_valuta <> f.c_ft_credit
    union all
    select 
        f.id,
        c_date,
        c_summa,
        c_valuta,
        c_reg_currency_sum,
        to_currency,
        (f.c_summa *crs1.course)/crs2.course c_summa_in_cr_v,
        pr_cred_id,
        c_sys_name,
        vid,
        ob   
    from 
        t_team_k7m_aux_p.set_factop_2 f
        join t_team_k7m_aux_d.k7m_cur_courses crs1 -- L4_5 Считается для эк.связей  
            on crs1.id = f.c_valuta  
        join t_team_k7m_aux_d.k7m_cur_courses crs2 -- L4_5 Считается для эк.связей  
            on crs2.id = f.to_currency  
    where f.c_date  between crs1.from_dt and crs1.to_dt
       and f.c_date  between crs2.from_dt and crs2.to_dt
       and f.c_valuta <> f.c_ft_credit
) x
    

 
-- Пока руками из:
-- 1) /user/team/corp_risks/temin_lv/deal_target/
-- 2) /user/team/corp_risks/temin_lv/regimes/
-- 3) /user/team/corp_risks/temin_lv/vid_cr/
-- соответственно 
--Потом будем брать из RDM
--1
-- create table t_team_k7m_stg.rdm_deal_target_mast
-- stored as parquet
-- as 
-- select 
    -- distinct lower(regexp_replace(target_cred,' |\\r|\\n|\\"','')) as target_cred,
      -- target 
 -- from t_team_k7m_stg.deal_target_new


--2
--create table t_team_k7m_stg.rdm_set_regimes_mast
--stored as parquet
--as
--select * from t_team_k7m_stg.regimes

--3
--create table t_team_k7m_stg.rdm_set_vid_cred_mast
--stored as parquet
--as
--select * from t_team_k7m_stg.vid_cr
 
--STEP 4
--1m10s

create table custom_cb_k7m_aux.rdm_deal_target_prepared
stored as parquet
as
select 
    max(target) as target,
    lower(regexp_replace(B.target_cred,' |\\r|\\n|\\"','')) target_cred
from custom_cb_k7m_stg.rdm_deal_target_mast b
group by lower(regexp_replace(B.target_cred,' |\\r|\\n|\\"',''));


create table t_team_k7m_aux_p.set_cred_filtered 
 stored as parquet
 as
 --explain
select
    distinct 
    a.client_id,
a.inn,
a.kpp,
a.client_name,
a.crm_segment,
a.pr_cred_id,
a.pr_cred_class_id,
a.pr_cred_collection_id,
a.ndog,
a.ddog,
a.dur_in_days,
a.dur_in_month,
a.c_date_ending,
a.date_close,
a.c_register_date_reg,
a.age,
a.summa_base,
a.summa_ru,
a.c_ft_credit,
a.summa_currency,
a.limit_amt,
a.limit_currency,
a.regime_prod,
a.cred_flag,
a.nkl_flag,
a.vkl_flag,
a.over_flag,
a.instrument,
a.c_name,
a.c_short_name,
a.avp_in_days,
a.avp_in_month,
a.target_cred,
a.target_cred2,
a.cr_type26,
a.c_list_pay,
a.product_status,
a.branch,
a.branch_ru,
a.cc_id,
a.cl_id,
a.st_id,
a.dep_id,
a.kc_id,
a.tc_id,
a.ok_id,
a.b_id,
a.zoc_id,
a.zok_id,
a.prop1_id,
a.prop2_id,
a.prod_prop_id,
    B.target b_target,
    C.use as c_USE,
    D.target  d_target
from t_team_k7m_aux_p.set_cred as A
left join custom_cb_k7m_aux.rdm_deal_target_prepared as B on 
    lower(regexp_replace(A.target_cred2,' |\\r|\\n|\\"','')) = B.target_cred
left join t_team_k7m_stg.rdm_set_regimes_mast as C on
    lower(regexp_replace(A.regime_prod,' |\\r|\\n|\\"','')) = lower(regexp_replace(C.regime_prod,' |\\r|\\n|\\"',''))
left join t_team_k7m_stg.rdm_set_vid_cred_mast as D on
    lower(regexp_replace(A.CR_TYPE26,' |\\r|\\n|\\"','')) = lower(regexp_replace(D.C_name,' |\\r|\\n|\\"',''))
where 1=1 
    --DELETED
    --and (B.target <> 'исключить' or a.over_flag =1 or B.target is null)
    --and C.use <> 'исключить'
   -- and (D.target <> 'исключить' or D.target is null)
    --- DELETED
    --INSERTED 12.04.2018 TEMIN-LV START
    and (B.target = 'оставить' or a.over_flag =1)
    and C.use = 'оставить'
    and (D.target = 'оставить' or D.target is null)
    --INSERTED 12.04.2018 TEMIN-LV END
    and a.product_status not in ( 'Отменен', 'Утверждено мировое соглашение')
    

--STEP 5 Проведем отсев проводок и отсеем договора так, чтобы существовали транзакции выдачи или гашения по ОД

--отберем транзакции гашения
create table t_team_k7m_aux_p.set_paym 
 stored as parquet
 as
select 
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id 
from 
     t_team_k7m_aux_p.set_factop a
inner join 
     t_team_k7m_aux_p.set_cred_filtered as b 
     on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Гашение'

--SET 5-2 отберем транзакции выдачи 

create table t_team_k7m_aux_p.set_give
 stored as parquet
 as
select 
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id 
from 
     t_team_k7m_aux_p.set_factop a
    inner join 
     t_team_k7m_aux_p.set_cred_filtered as b 
    on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Выдача'

--отберем транзакции списания
create table t_team_k7m_aux_p.set_wro
 stored as parquet
 as
select 
    a.c_date,
    a.c_summa as c_summa,
    a.c_summa_in_cr_v as c_summa_in_cr_v,
    a.c_valuta,
    a.c_reg_currency_sum,
    a.pr_cred_id 
from t_team_k7m_aux_p.set_factop a
    inner join 
     t_team_k7m_aux_p.set_cred_filtered as b 
     on a.pr_cred_id = b.pr_cred_id
where a.ob = 'Основной долг' and a.vid = 'Списание'

-- отфильтруем договора по условию того, что у них есть хотя-бы одна операция выдачи или погашения
create table t_team_k7m_aux_p.set_cred_active
stored as parquet
as
select A.*
from t_team_k7m_aux_p.set_cred_filtered as A
    inner join (
        select distinct pr_cred_id 
        from(
            select pr_cred_id 
            from t_team_k7m_aux_p.set_paym 
            union all select pr_cred_id 
            from t_team_k7m_aux_p.set_give) x
            ) as D 
    on A.pr_cred_id = D.pr_cred_id


-- create table t_team_k7m_stg.test_ratings2
-- stored as parquet
-- as
-- select  
    -- from_unixtime(cast(unix_timestamp(request_date,'ddMMMyyyy:HH:mm:ss')  as int)) as request_date, 
    -- INN, 
    -- PDW_MAPBACK as pd
-- from t_team_k7m_stg.test_ratings

--подклеим рейтинг к договорам --SKIP - не нужно
/*
create table t_team_k7m_aux_p.set_cred_rating
stored as parquet
as
select 
    A.client_id,
    a.client_name,
    a.inn,
    a.kpp, 
    a.crm_segment,
    a.ndog,
    a.ddog,
    a.dur_in_days,
    a.dur_in_month,
    a.c_date_ending,
    a.date_close,
    a.summa_base, 
    a.summa_ru,
    a.C_FT_CREDIT,
    a.limit_amt,
    a.regime_prod,
    a.cred_flag,
    a.nkl_flag,
    a.vkl_flag,
    a.over_flag,
    a.instrument,
    a.avp_in_days,
    a.avp_in_month,
    a.target_cred,
    a.product_status,
    a.c_list_pay,
    a.pr_cred_id,
    a.rating,
    a.pd, 
    a.branch, 
    a.branch_ru,
    row_number() over (partition by A.inn, A.instrument order by A.ddog desc) as num_cr_desc
from (
    select 
        A.*, 
        B.rating, 
        B.pd,
        row_number() over (partition by A.pr_cred_id order by B.request_date desc ) as rn1
    from t_team_k7m_aux_p.set_cred_active as A
    left join (
            select 
                A.inn , 
                max(B.request_date) request_date,
                max(C.r) as rating,
                max(C.pd) pd
            from t_team_k7m_aux_p.set_cred_active as A
            join t_team_k7m_stg.test_ratings2 as B on B.inn = A.inn
            cross join t_team_k7m_stg.test_rating_rates c
            where 
                unix_timestamp(A.ddog, 'yyyy-MM-dd HH:mm:ss.s') > unix_timestamp(B.request_date, 'yyyy-MM-dd HH:mm:ss')  
                and unix_timestamp(add_months(A.ddog,-12),'yyyy-MM-dd')<= unix_timestamp(B.request_date, 'yyyy-MM-dd HH:mm:ss')
                and c.`from` <=B.pd 
                and B.pd <c.`to`                
           group by a.inn ) as B 
     on 
        B.inn = A.inn        
    ) as A 
where A.rn1 =1
*/


--STEP 6. Добавим отчетность

--честная (за 10 месяцев до договора) отчетность из интегрума-
create table t_team_k7m_aux_p.set_cred_rep1
stored as parquet
as
--explain
select 
    pr_cred_id, 
    inn, 
    ndog, 
    ddog,
    int_revenue,
    integrum_period_start_dt 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active as cred --set_cred_active
        inner join 
        t_team_k7m_aux_d.int_fin_stmt_rsbu as integ 
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, 10) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 22)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x 
where rn=1

--не очень честная (за 0 месяцев до договора) отчетность из интегрума
create table t_team_k7m_aux_p.set_cred_rep2
stored as parquet
as
select 
    pr_cred_id, 
    inn, 
    ndog, 
    ddog,
    int_revenue,
    integrum_period_start_dt 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating 
        inner join 
        t_team_k7m_aux_d.int_fin_stmt_rsbu as integ 
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, 0) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 12)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x 
where rn=1

--не очень честная (за 0 месяцев до договора) отчетность из интегрума
create table t_team_k7m_aux_p.set_cred_rep3
stored as parquet
as
select 
    pr_cred_id, 
    inn, 
    ndog, 
    ddog,
    int_revenue,
    integrum_period_start_dt 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating 
        inner join 
        t_team_k7m_aux_d.int_fin_stmt_rsbu as integ 
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, -6) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 6)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x 
where rn=1

--честная (за 6 месяцев до договора) отчетность из фок
create table t_team_k7m_aux_p.set_cred_rep4
stored as parquet
as
select 
     pr_cred_id,
     inn, 
     ndog, 
     ddog,
     fok_period_start_dt,
     fok_revenue 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active  as cred -- set_cred_rating 
        inner join
        t_team_k7m_aux_d.k7m_crm_org_mj mj
        on mj.inn = cred.inn
        join t_team_k7m_aux_d.fok_fin_stmt_rsbu as fok 
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, 6) <= ddog
   and ddog < add_months(fok.fin_stmt_start_dt, 18)
   and fok.fin_stmt_2110_amt is not null 
   and fok.fin_stmt_2110_amt>0
) x 
where rn=1

--почти честная (за 0 месяцев до договора) отчетность из фок
create table t_team_k7m_aux_p.set_cred_rep5
stored as parquet
as
select 
     pr_cred_id,
     inn, 
     ndog, 
     ddog,
     fok_period_start_dt,
     fok_revenue 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active  as cred -- set_cred_rating
        inner join
        t_team_k7m_aux_d.k7m_crm_org_mj mj
        on mj.inn = cred.inn
        join t_team_k7m_aux_d.fok_fin_stmt_rsbu as fok 
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, 0) <= ddog
   and ddog < add_months(fok.fin_stmt_start_dt, 12)
   and fok.fin_stmt_2110_amt is not null 
   and fok.fin_stmt_2110_amt>0
) x 
where rn=1


--совсем не честная (до 6 месяцев после договора) отчетность из фок
create table t_team_k7m_aux_p.set_cred_rep6
stored as parquet
as
select 
     pr_cred_id,
     inn, 
     ndog, 
     ddog,
     fok_period_start_dt,
     fok_revenue 
from (
    select 
        cred.pr_cred_id, 
        cred.inn, 
        cred.ndog, 
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from  
        t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating 
        inner join
        t_team_k7m_aux_d.k7m_crm_org_mj mj
        on mj.inn = cred.inn
        join t_team_k7m_aux_d.fok_fin_stmt_rsbu as fok 
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, -6) <= ddog
   and fok.fin_stmt_2110_amt is not null 
   and fok.fin_stmt_2110_amt>0
) x 
where rn=1


--оценка даты отчетности интегрум которая есть в БД (спользуется для отсева компаний)
create table t_team_k7m_aux_p.set_cred_rep1d
stored as parquet
as
select 
    cred.pr_cred_id, 
    cred.inn, 
    cred.ndog, 
    cred.ddog,
    min(integ.fin_stmt_start_dt) as int_md
from
    t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating  
    inner join  t_team_k7m_aux_d.int_fin_stmt_rsbu  as integ 
    on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, -6) <= cred.ddog  
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
group by  cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog

--оценка даты отчетности фок которая есть в БД (спользуется для отсева компаний)
create table t_team_k7m_aux_p.set_cred_rep2d
stored as parquet
as
select 
    cred.pr_cred_id, 
    cred.inn, 
    cred.ndog, 
    cred.ddog,
    min(fok.fin_stmt_start_dt) as fok_md
from
     t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating 
     inner join
     t_team_k7m_aux_d.k7m_crm_org_mj mj
     on mj.inn = cred.inn
     join t_team_k7m_aux_d.fok_fin_stmt_rsbu as fok 
      on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, -6) <= cred.ddog  
   and fok.fin_stmt_2110_amt is not null and fok.fin_stmt_2110_amt>0
group by  cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog



create table t_team_k7m_aux_p.set_cred_enriched
stored as parquet
as
select 
    cred.*,
    coalesce(pastinteg.int_revenue,nextinteg.int_revenue,nextinteg2.int_revenue) as int_revenue,
    coalesce(pastfok.fok_revenue  ,nextfok.fok_revenue ,nextfok2.fok_revenue)   as fok_revenue,
    months_between(cred.ddog, coalesce(fok_md,int_md))    as report_month_ago,
    row_number() over (partition by cred.inn, cred.instrument order by cred.ddog desc) as num_cr_desc
from 
    t_team_k7m_aux_p.set_cred_active as cred --set_cred_rating 
    left join t_team_k7m_aux_p.set_cred_rep1 as pastinteg on pastinteg.pr_cred_id = cred.pr_cred_id
    left join t_team_k7m_aux_p.set_cred_rep2 as nextinteg on nextinteg.pr_cred_id = cred.pr_cred_id
    left join t_team_k7m_aux_p.set_cred_rep3 as nextinteg2 on nextinteg2.pr_cred_id = cred.pr_cred_id
    
    left join t_team_k7m_aux_p.set_cred_rep4 as pastfok on pastfok.pr_cred_id = cred.pr_cred_id
    left join t_team_k7m_aux_p.set_cred_rep5 as nextfok on nextfok.pr_cred_id = cred.pr_cred_id
    left join t_team_k7m_aux_p.set_cred_rep6 as nextfok2 on nextfok2.pr_cred_id = cred.pr_cred_id
    
    left join t_team_k7m_aux_p.set_cred_rep1d as integmd on integmd.pr_cred_id = cred.pr_cred_id
    left join t_team_k7m_aux_p.set_cred_rep2d as fokmd on fokmd.pr_cred_id = cred.pr_cred_id

--ОБРАБОТКА В PANDAS (python script SET_bo_agg_prepare.ipynb). Результат в t_team_k7m_aux_p.set_bo_agg
--Конвертация даты: select from_unixtime(  cast(1404849600000000000/1000000000 as bigint))

create table t_team_k7m_aux_p.set_z_sur
stored as parquet
as
select 
    distinct 
        t1.pr_cred_id,
        t1.inn, 
        t1.ndog,
        from_unixtime(cast(t1.ddog/(1000000*1000) as int)) ddog,
        case 
            when acc.c_main_v_id like '91414%' and guarantor.class_id = 'CL_PRIV' then 'ПФЛ'
            when acc.c_main_v_id like '91414%' and guarantor.class_id = 'CL_ORG'  then 'ПЮЛ'
        end as type_sur, 
        case 
            when zal.id is not null 
                and acc.c_main_v_id not like '91414%' 
                then 1 
                else 0 
                end as zal_flag,
        guarantor.id as sur_client_id, 
        guarantor.c_inn 
    from  
        t_team_k7m_aux_p.set_bo_agg as t1
        inner join t_team_k7m_stg.eks_z_part_to_loan pl 
            on pl.c_product= t1.pr_cred_id 
        inner join t_team_k7m_stg.eks_Z_zalog zal 
            on zal.c_part_to_loan = pl.collection_id
        inner join t_team_k7m_stg.eks_z_ac_fin as acc 
            on zal.c_acc_zalog = acc.id
        inner join t_team_k7m_stg.eks_z_client as guarantor 
            on zal.c_user_zalog = guarantor.id
        
      

--Сводим в финальный датасет, вводим пользовательские флаги, классификацию и т.п.

create table t_team_k7m_aux_p.set_suretyships_stat_nc
stored as parquet
as
select 
    cast(t1.client_id as bigint) as  client_id, 
    t1.client_name, 
    t1.inn,  
    cast(cast (t1.pr_cred_id as double ) as bigint) as  pr_cred_id ,  
    t1.ndog, 
    from_unixtime(cast(t1.ddog/(1000000*1000) as int)) as ddog, 
    t1.date_close, 
    t1.cred_flag, 
    t1.nkl_flag,  
    t1.vkl_flag,  
    t1.over_flag, 
    t1.instrument, 
    t1.branch, 
    t1.branch_ru, 
    case when t1.num_cr_desc = 1 then 'Последний' else 'Не последний' end as last_deal_flag, 
    t1.dur_in_days, 
    t1.product_status, 
    case when  t1.dur_in_days <= 366 then '1. менее 1 года' 
          when 366<  t1.dur_in_days and  t1.dur_in_days <= 366*1.5 then '2. от 1 года до 1.5-х'
          when 366*1.5<  t1.dur_in_days and  t1.dur_in_days <= 366*2 then '3. от 1.5 года до 2-х'
          when 366*2<  t1.dur_in_days and  t1.dur_in_days <= 366*3 then '4. от 2 лет до 3-х'
    end as DUR,
    max(case when t2.type_sur is not null then 1 else 0 end) as any_surety,
    max(case when t2.type_sur = 'ПФЛ' then 1 else 0 end) as fis_surety,
    max(case when t2.type_sur = 'ПЮЛ' then 1 else 0 end) as jur_surety,
    max(case when t2.zal_flag is not null and t2.zal_flag<> 0 then 1 else 0 end) as collateral
from  t_team_k7m_aux_p.set_bo_agg as t1
    left join t_team_k7m_aux_p.set_z_sur 
    as t2 on t1.pr_cred_id  = t2.pr_cred_id
where
    (t1.cred_flag=1 or t1.nkl_flag=1 or t1.vkl_flag=1 or t1.over_flag=1)
group by 
    t1.client_id, 
    t1.client_name, 
    t1.inn,  
    t1.pr_cred_id,  
    t1.ndog,  
    t1.ddog, 
    t1.date_close, 
    t1.cred_flag, 
    t1.nkl_flag,  
    t1.vkl_flag,  
    t1.over_flag, 
    t1.instrument, 
    t1.branch, 
    t1.branch_ru,
    case when t1.num_cr_desc = 1 then 'Последний' else 'Не последний' end,
    t1.dur_in_days, 
    t1.product_status


select * from t_team_k7m_aux_p.set_suretyships_stat_nc
limit 100