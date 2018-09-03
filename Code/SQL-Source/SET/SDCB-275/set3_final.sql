--********************************************
--Поиск потенциальных поручителей.

--ВНИМАНИЮ РАЗРАБОТЧИКА:
--Константа DATE'2018-02-01' - это параметр Дата расчета K7M!
--(см. метку SCALE: ниже) Для шкалы использовать таблицу t_team_k7m_aux_p.dict_BO_PD_by_SCALE. вместо значения для сравнения (например 0.00032, 0.00044 ...) использовать поле upper_bound. Вместо результирующего значения (например 1,2...) использовать поле rating
--*******************************************

drop table t_team_k7m_aux_p.set_basis_client_instr_rep
create table  t_team_k7m_aux_p.set_basis_client_instr_rep
stored as parquet
as
select 
    c.u7m_id, 
    c.inn,    
    coalesce(c.short_name,c.full_name) org_name,
    c.bsegment,
   'КД/НКЛ/ВКЛ' as instrument 
from t_team_k7m_pa_d.clu c
where c.flag_basis_client = 'Y'
union all
select  
    c.u7m_id, 
    c.inn,    
    coalesce(c.short_name,c.full_name) org_name,
    c.bsegment, 
    'ОВЕР' as instrument 
from t_team_k7m_pa_d.clu c
where c.flag_basis_client = 'Y'
/


create table  t_team_k7m_pa_d.set_surety_heritage_dog_
stored as parquet
as
--поиск договора для наследования
select 
    b.u7m_id,
    b.inn,
    b.org_name as org_short_crm_name,
    b.bsegment org_segment_name,
    b.instrument,
    case when coalesce(s.pr_cred_id) is not null then 1 else 0 end as heritance_flag,
    case 
        when (s.any_surety = 0 and s.collateral = 0) then "without_surety" 
        when (s.any_surety = 1 and s.collateral = 1) then "surety_and_coll" 
        when (s.any_surety = 1 and s.collateral = 0) then "only_surety"
        when (sc.any_surety = 0 and sc.collateral = 1) then "only_collateral"  
        when (w.pr_cred_id <> s.pr_cred_id and w.pr_cred_id <> sc.pr_cred_id and w.pr_cred_id is not NULL)   then "Old_or_More_then_3_year"          
        when (oc.cnt_over > 0  and oc.cnt_credit < 1) then "only_over"
        when (oc.cnt_over > 0  and oc.cnt_credit > 0) then "over and credit"
        when (oc.cnt_over < 1  and oc.cnt_credit > 0) then "only credit"
    end as sur_dog_type,
    s.pr_cred_id,
    sc.pr_cred_id as  sc_pr_cred_id,
    w.pr_cred_id as w_pr_cred_id,
    s.ndog,
    s.ddog
from t_team_k7m_aux_p.set_basis_client_instr_rep b
    left join (
    select a.* from (
        select * 
            ,row_number() over (
                partition by inn,instrument 
                order by ddog desc, 
                    pr_cred_id desc) as rn
        from t_team_k7m_aux_p.set_suretyships_stat_nc
        where dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию') 
                  or  
                  (product_status = 'Закрыт' and months_between(DATE'2018-02-01',to_date(from_unixtime(cast(date_close/(1000000*1000) as int)))) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
            and not (any_surety = 0 and collateral = 1)
        ) a
        where rn = 1 
            and pr_cred_id is not null 
            and ndog is not null 
            and ddog is not null
) s 
on 
   b.inn = s.inn 
   and b.instrument = s.instrument 
left join (
    select a2.* from (
        select * 
            ,row_number() over (partition by inn,instrument order by ddog desc, pr_cred_id desc) as rn
        from t_team_k7m_aux_p.set_suretyships_stat_nc
        where  dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию') 
                  or  
                  (product_status = 'Закрыт' and months_between(DATE'2018-02-01',to_date(from_unixtime(cast(date_close/(1000000*1000) as int)))) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) a2
    where rn = 1 
        and pr_cred_id is not null 
        and ndog is not null 
        and ddog is not null
    ) sc 
on 
    b.inn = sc.inn 
    and b.instrument = sc.instrument 
left join (
    select a3.* 
    from (
        select * 
            ,row_number() over (partition by inn,instrument order by ddog desc, pr_cred_id desc) as rn
        from t_team_k7m_aux_p.set_suretyships_stat_nc
        where  instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) a3
        where rn = 1 
            and pr_cred_id is not null 
            and ndog is not null 
            and ddog is not null
) w on b.inn = w.inn and b.instrument = w.instrument 
left join (
    select a4.* from (
        select 
            inn,
            sum(case when instrument = 'ОВЕР' then 1 else 0 end)  as cnt_over,
            sum(case when instrument = 'КД/НКЛ/ВКЛ' then 1 else 0 end) as cnt_credit
        from t_team_k7m_aux_p.set_suretyships_stat_nc
        where instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        group by inn
        ) a4
) oc on b.inn = oc.inn
/



--соберем данные по поручителям (ФПЛ и ПЮЛ)
create table t_team_k7m_aux_p.set_surety_heritage_dog_sur
stored as parquet
as
 select distinct 
    t1.u7m_id,
    t1.inn,
    t1.org_short_crm_name,
    t1.org_segment_name,
    t1.instrument,
    t1.heritance_flag,
    t1.sur_dog_type,
    t1.pr_cred_id,
    t1.sc_pr_cred_id,
    t1.w_pr_cred_id,
    t1.ndog,
    t1.ddog,
    s.from_obj sur_obj,
    lkc.u7m_id_from as sur_f7m_id, 
    s.from_inn as sur_inn,
    s.loc_id
from t_team_k7m_pa_d.set_surety_heritage_dog_ as t1
    inner join t_team_k7m_aux_p.exsgen s 
        on s.cred_id= t1.pr_cred_id 
    left join t_team_k7m_aux_p.lk_lkc lkc 
        on lkc.link_id= CONCAT('EX_',cast(s.loc_id as string))
            and lkc.t_from = 'CLF'
  where  s.crit_id = 'exsgen_set_sur'
 

--добавим кол-во поручителей
drop table t_team_k7m_aux_p.set_surety_heritage_dog_sur_cnt
create table t_team_k7m_aux_p.set_surety_heritage_dog_sur_cnt
stored as parquet
as
select 
    t1.u7m_id,
    t1.inn,
    t1.org_short_crm_name,
    t1.org_segment_name,
    t1.instrument,
    t1.heritance_flag,
    t1.sur_dog_type,
    t1.pr_cred_id,
    t1.sc_pr_cred_id,
    t1.w_pr_cred_id,
    t1.ndog,
    t1.ddog,
    t1.sur_obj,
    t1.sur_f7m_id, 
    t1.sur_inn,
    t1.loc_id,
    case when cnt.cnt_sur > 10 then 1 else 0 end as too_much_sur_flag, 
    cnt.cnt_sur
from t_team_k7m_aux_p.set_surety_heritage_dog_sur t1
left join (
    select pr_cred_id, instrument, sum(case when sur_f7m_id is not null or sur_inn is not null then 1 else 0 end) as cnt_sur 
    from  t_team_k7m_aux_p.set_surety_heritage_dog_sur 
    group by pr_cred_id, instrument 
    ) cnt
on 
    t1.pr_cred_id = cnt.pr_cred_id 
    and t1.instrument = cnt.instrument
    
    
--добавим поручителей базису
 drop table t_team_k7m_aux_p.set_basis_with_heritance
create table t_team_k7m_aux_p.set_basis_with_heritance
stored as parquet
as
select 
    b.*,
    hs.sur_obj,
    hs.sur_f7m_id, 
    hs.sur_inn,
    hs.loc_id,
    coalesce(hs_cnt.cnt_sur,0) as cnt_heritage_sur
from 
    t_team_k7m_pa_d.set_surety_heritage_dog_ b
    left join t_team_k7m_aux_p.set_surety_heritage_dog_sur_cnt hs
    on b.pr_cred_id = hs.pr_cred_id 
        and b.instrument = hs.instrument and 
        hs.too_much_sur_flag = 0
    left join (
        select 
            distinct 
                pr_cred_id,
                instrument, 
                cnt_sur  
        from t_team_k7m_aux_p.set_surety_heritage_dog_sur_cnt) hs_cnt
    on 
        b.pr_cred_id = hs_cnt.pr_cred_id 
        and b.instrument = hs_cnt.instrument 


---a.Овердрафты предлагаем вообще без поручительств. (все дальнейшее только для всех остальных) b.Если заемщик не имеет бенефициаров (не заведены в CRM), то другие БО ему не предлагаются.
drop table  t_team_k7m_aux_p.set_basis_heritance_plus_over
create table t_team_k7m_aux_p.set_basis_heritance_plus_over
stored as parquet
as
select  *
    ,case when heritance_flag = 1 and cnt_heritage_sur <= 10  then 'Ok'
          when heritance_flag = 1 and cnt_heritage_sur > 10  then 'Block'
          when heritance_flag = 0 and instrument = 'ОВЕР' then 'Ok'
          else null
     end as surety_status 
from t_team_k7m_aux_p.set_basis_with_heritance
/
--соберём финальный набор поручителей:
---#добавим поручителей базису 
drop table t_team_k7m_aux_p.set_basis_with_surety_set
create table t_team_k7m_aux_p.set_basis_with_surety_set
stored as parquet
as
select  
     t1.u7m_id,
     t1.org_short_crm_name,
     t1.org_segment_name
    ,t1.instrument
    ,t1.heritance_flag
    ,t1.sur_dog_type
    ,t1.pr_cred_id
    ,t1.ndog
    ,t1.ddog
    ,t1.cnt_heritage_sur,
    case when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.sur_obj
          when t2.u7m_id is not null then 'CLU' 
          else null
     end as sur_obj,
     t1.sur_f7m_id,
     t1.loc_id,
     case when t1.surety_status = 'Ok' and heritance_flag = 1 then 'SUCL'
          when t2.u7m_id is not null then 'SUNCL' 
          else null
     end as sur_class,     
     case when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.sur_inn
          when t2.u7m_id is not null then t2.sur_inn 
          else null
     end as sur_inn,
     case 
        when t1.surety_status = 'Ok' and heritance_flag = 1 then t1.surety_status
        when t2.u7m_id is not null then 'Ok'
        when t1.instrument ='ОВЕР' then t1.surety_status
        when t1.surety_status <> 'Ok' then t1.surety_status          
        else 'FIND' 
     end as surety_status,
     case when t3.inn is not null then 1 else 0 end as Clients_7m
from 
    t_team_k7m_aux_p.set_basis_heritance_plus_over t1
    left join t_team_k7m_aux_p.sep_surety_7M_for_non_client_sur_init t2 -- =surety_7M_for_non_client_final !
    on  
        t1.u7m_id =t2.u7m_id
        and t1.instrument = 'КД/НКЛ/ВКЛ' 
        and t1.surety_status is null
    left join (
        select distinct inn
        from t_team_k7m_aux_p.set_suretyships_stat_nc
        where  dur_in_days <= 366*3
            and ( product_status in ('Работает','Помечен к закрытию') 
                  or  
                  (product_status = 'Закрыт' and months_between(date'2018-02-01',to_date(from_unixtime(cast(date_close/(1000000*1000) as int)))) <=12)
                )
            and instrument in ('КД/НКЛ/ВКЛ','ОВЕР')
        ) t3 
     on t3.inn = t1.inn

drop table t_team_k7m_aux_p.set_prep
create table t_team_k7m_aux_p.set_prep
stored as parquet
as
select
    s.u7m_id,
    s.instrument,
    s.sur_class,
    s.sur_obj,
    s.sur_u7m_id,
    s.surety_status,
    row_number () over (partition by s.u7m_id,s.instrument, s.sur_obj, s.sur_u7m_id) rn
from (
select 
    s.u7m_id,
    s.instrument,
    s.sur_class,
    s.sur_obj,
    case 
        when s.sur_obj = 'CLF'then s.sur_f7m_id
        else c.u7m_id
    end sur_u7m_id,
    s.surety_status
from 
    t_team_k7m_aux_p.set_basis_with_surety_set s
    left join
    t_team_k7m_pa_d.clu c
    on c.inn = s.sur_inn 
) s

drop table t_team_k7m_aux_p.set_prep_risk
create table t_team_k7m_aux_p.set_prep_risk
stored as parquet
as
select 
    s.u7m_id,
    s.instrument,
    s.sur_class,
    s.sur_obj,
    s.sur_u7m_id,
    s.surety_status,
        case 
            when coalesce(rs.risk_segment_nm,'X') in  ('Кредитование по пилот. моделям','Корпор. компания-резидент (РФ)')    then 'Y'
            else 'N'
        end sur_corporate_flag,
     case when pd.pdstandalone<0.00032 then 1   --SCALE:Для шкалы использовать таблицу t_team_k7m_aux_p.dict_BO_PD_by_SCALE. вместо значения для сравнения (например 0.00032, 0.00044 ...) использовать поле upper_bound. Вместо результирующего значения (например 1,2...) использовать поле rating
            when pd.pdstandalone<0.00044 then 2 
            when pd.pdstandalone<0.0006 then 3 
            when pd.pdstandalone<0.00083 then 4
            when pd.pdstandalone<0.00114 then 5
            when pd.pdstandalone<0.00156 then 6
            when pd.pdstandalone<0.00215 then 7
            when pd.pdstandalone<0.00297 then 8
            when pd.pdstandalone<0.00409 then 9
            when pd.pdstandalone<0.00563 then 10
            when pd.pdstandalone<0.00775 then 11
            when pd.pdstandalone<0.01067 then 12
            when pd.pdstandalone<0.0147 then 13
            when pd.pdstandalone<0.02024 then 14
            when pd.pdstandalone<0.02788 then 15
            when pd.pdstandalone<0.03839 then 16
            when pd.pdstandalone<0.05287 then 17
            when pd.pdstandalone<0.0728 then 18
            when pd.pdstandalone<0.10026 then 19
            when pd.pdstandalone<0.13807 then 20
            when pd.pdstandalone<0.19014 then 21
            when pd.pdstandalone<0.26185 then 22
            when pd.pdstandalone<0.36059  then 23
            when pd.pdstandalone<0.49659 then 24
            when pd.pdstandalone<1 then 25
            when pd.pdstandalone>=1 then 26 
            when pd.pdstandalone is null then 99
         end as sur_rating       
from t_team_k7m_aux_p.set_prep s
    left join  t_team_k7m_aux_d.proxyrs_int_final rs
    on s.sur_u7m_id = rs.u7m_id
       and s.sur_class ='CLU'
    left join t_team_k7m_PA_d.PDStandalone pd
    on s.sur_class ='CLU'
     and s.sur_u7m_id = pd.u7m_id 
where s.rn = 1


drop table t_team_k7m_aux_p.set_set_item
create table t_team_k7m_aux_p.set_set_item
stored as parquet
as
select 
    row_number() over(order by sr.u7m_id, sr.instrument, sr.sur_obj, sr.sur_u7m_id) set_item_id,
    sr.u7m_id,
    sr.instrument,
    sr.sur_class,
    sr.sur_obj,
    sr.sur_u7m_id,
    case 
        when sr.sur_class ='SUNCL' 
             and surety_status ='FIND'
             and sur_corporate_flag='N'
             then 'Ignore_by_segment'
        when sr.sur_class ='SUNCL' 
             and surety_status ='FIND'
        then 'Ok'
        when  sr.sur_class ='SUNCL'
              and sr.sur_rating>21
        then  'Ignore_by_rating'     
        else sr.surety_status
     end   set_item_status,
    sr.sur_corporate_flag,
    sr.sur_rating,
    n.cnt_neighbours
from t_team_k7m_aux_p.set_prep_risk sr
    left join 
    t_team_k7m_aux_p.sep_cnt_neighbours n
    on sr.u7m_id = n.bor_u7m_id;
    
    

--Добавить пустые значения в SET
create table  t_team_k7m_aux_p.set_empty
stored as parquet 
as
select
    concat('ZO',c.u7m_id) as set_id,
    'ОВЕР' instrument,
    c.u7m_id as  u7m_b_id
from  t_team_k7m_pa_d.clu c 
    left join 
          t_team_k7m_aux_d.set_set_item as s
    on c.u7m_id = s.u7m_id
        and instrument ='ОВЕР'
where c.flag_basis_client = 'Y' 
    and s.u7m_id is null;
  
insert into t_team_k7m_aux_p.set_empty
select
    concat('ZK',c.u7m_id) as set_id,
    'КД/НКЛ/ВКЛ' instrument,
    c.u7m_id as  u7m_b_id
from  t_team_k7m_pa_d.clu c 
    left join 
          t_team_k7m_aux_d.set_set_item as s
    on c.u7m_id = s.u7m_id
        and instrument ='КД/НКЛ/ВКЛ'
where c.flag_basis_client = 'Y'  
    and s.u7m_id is null;    
    

-- Результирующая таблица: 
drop table t_team_k7m_aux_p.set
create table t_team_k7m_aux_p.set
stored as parquet 
as
select 
    cast(row_number() over (order by u7m_b_id,instrument) as string) set_id,
    u7m_b_id,
    instrument,
    max_borrower_rating,
    'EXEC_ID from ETL_EXEC!' exec_id --Получить из системы логирования
from (
select
    u7m_id as u7m_b_id,
    instrument as instrument,
    case when 
            sum(
            case 
            when set_item_status = 'Ok' 
            then 1
            else 0
           end) =0        
        and instrument = 'КД/НКЛ/ВКЛ'
        then cast(14 as int)
        else cast(null as int)
    end max_borrower_rating,
    case  
    when 
        max(
        case 
            when set_item_status = 'Block' then 1
            else 0
           end) =1 then 'Block'
        else 'Ok'    
     end set_status
    from t_team_k7m_aux_p.set_set_item
    group by u7m_id ,instrument
    ) x
    where x.set_status = 'Ok'   

drop table t_team_k7m_aux_p.set_item
create table t_team_k7m_aux_p.set_item
stored as parquet
as
select
  cast(i.set_item_id as string) set_item_id,   
  cast(s.set_id as string) set_id,
  i.sur_obj as obj,
  i.sur_u7m_id as u7m_id,
  'EXEC_ID from ETL_EXEC!' exec_id --Получить из системы логирования
from 
    t_team_k7m_aux_p.set s
    join 
    t_team_k7m_aux_p.set_set_item i
    on s.u7m_b_id = i.u7m_id   
       and s.instrument = i.instrument 
where set_item_status = 'Ok'
    and sur_u7m_id is not null;
    
 
insert into t_team_k7m_aux_d.`set`
select
    set_id,
    u7m_b_id,
    instrument,
    case when instrument='ОВЕР' then cast(null as int ) else cast(14 as int ) end  max_borrower_rating,
    cast(null as int )  exec_id
from t_team_k7m_aux_p.set_empty    ;
    

insert overwrite table  t_team_k7m_pa_d.`set_item`
select 
    set_id,
    u7m_b_id,
    cast(null as string) as exec_id,
    cast(null as bigint) as okr_id
from t_team_k7m_aux_d.`set`

insert overwrite table  t_team_k7m_pa_d.`set_item`
select 
    set_item_id,
    set_id,
    u7m_id,
    obj,
    cast(null as string) as exec_id,
    cast(null as bigint) as okr_id,
    cast(null as string) as offline_mmz_mark,
    cast(null as string) as concerment
from t_team_k7m_aux_d.`set_item`    
    

