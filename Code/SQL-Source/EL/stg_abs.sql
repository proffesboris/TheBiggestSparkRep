--*****Промежуточные сущности на общий срез (независимо от даты) ****-------
set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;



--Данные по Интегрум
--Организации Интегрума
drop table t_team_k7m_aux_p.k7m_integrum_org;
create table t_team_k7m_aux_p.k7m_integrum_org stored as parquet as    
select distinct
    ul_inn,
    ul_org_id,
    ul_branch_cnt,
    ul_okopf_cd
from  (
    select  
        ul_inn,
        ul_org_id,
        ul_branch_cnt,
        ul_okopf_cd,
        count(*) over (partition by s.ul_inn) cnt
    from core_internal_integrum.ul_organization_rosstat    s
    where current_timestamp  between cast(valid_from as timestamp) and  cast(valid_to  as timestamp)
        and coalesce(egrul_org_id,'')<>''
        and ul_inn is not null
    ) s
where cnt=1 or cast(ul_branch_cnt as  bigint)>=1;

--Данные по Интегруму
drop table t_team_k7m_aux_p.k7m_integrum_1600;
create table t_team_k7m_aux_p.k7m_integrum_1600 stored as parquet as                
select
    distinct 
    regexp_replace(f.period_nm,' год','-12-25') dt,
    f.ul_org_id,
    f.fs_unit,
    f.fs_value,
    o.ul_inn
from core_internal_integrum.financial_statement_rosstat  f
    join t_team_k7m_aux_p.k7m_integrum_org  o      
        on f.ul_org_id = o.ul_org_id
where  fs_column_nm = 'На дату отч. периода'
    and fs_form_num =1
    and fs_line_num ='1600'
    ;
--Строки из отчета о Фин.результатах
drop table t_team_k7m_aux_p.k7m_integrum_OFR;
create table t_team_k7m_aux_p.k7m_integrum_OFR stored as orc as                
select
    regexp_replace(f.period_nm,' год','') yr,
    f.ul_org_id,
    f.fs_unit,
    o.ul_inn,
    sum(case when fs_line_num ='2110' then cast(f.fs_value as double) else 0 end) vyruchka,
    sum(case when fs_line_num in ('2220','2350','2210','2120') then cast(f.fs_value as double) else 0 end) total_expenses
from t_team_k7m_aux_p.k7m_integrum_org  o      
    join core_internal_integrum.financial_statement_rosstat  f 
        on f.ul_org_id = o.ul_org_id
where  fs_form_num=2 and fs_column_nm='За отч. период' and fs_line_num in('2110','2220','2350','2210','2120')
group by regexp_replace(f.period_nm,' год',''), 
    f.ul_org_id,
    f.fs_unit,
    o.ul_inn;    
    
    
--Данные по ФОК
 --Шаг 1
drop table t_team_k7m_aux_p.k7m_fok_header;
create table t_team_k7m_aux_p.k7m_fok_header stored as parquet as        
select 
    id,
    divid,
    docs_status,
    version,
    create_date,
    modify_date,
    title,
    year,
    period,
    formid,
    user_name,
    dealid,
    template_datefr,
    SBRF_INN
from (
select 
    h.id,
    h.divid,
    h.docs_status,
    h.version,
    h.create_date,
    h.modify_date,
    h.title,
    h.year,
    h.period,
    h.formid,
    h.user_name,
    h.dealid,
    h.template_datefr,
    e.SBRF_INN,
    row_number() over (partition by id order by h.ods_validfrom desc,e.ods_validfrom desc) as rn
from 
    core_internal_fok.docs_header as h
    inner join
    core_internal_crm_kb.S_ORG_EXT_X as e
    on h.divid = e.row_id 
where 
    h.formid='SB0121') x
where   rn=1  ;    

--Шаг 2
drop table t_team_k7m_aux_p.k7m_fok_header_selected;
create table t_team_k7m_aux_p.k7m_fok_header_selected stored as orc as    
select 
    a.*
from 
    t_team_k7m_aux_p.k7m_fok_header a
    inner join 
    (select  
    SBRF_INN,
    divid, 
    formid, 
    year, 
    period, 
    max(case when docs_status=9 then version end) as mv_9,
    max(version) as mv_all,
    coalesce(max(case when docs_status=9 then version end),max(version) ) as mv
from 
     t_team_k7m_aux_p.k7m_fok_header
group by 
    SBRF_INN, divid, formid, year, period) mv
    on a.SBRF_INN=mv.SBRF_INN and a.divid=mv.divid and a.year=mv.year and a.period=mv.period and a.version=mv.mv
where 
    not (a.SBRF_INN='5249072845' and a.divid='1-1CGATR' and a.DOCS_STATUS=3 and a.year=2016 and a.period=3)
    ;

--Шаг 3
drop table t_team_k7m_aux_p.k7m_fok_reports;
create table t_team_k7m_aux_p.k7m_fok_reports stored as orc as 
select
    dh.SBRF_INN as inn,
    concat(dh.year,'-',lpad(cast(cast(dh.period as smallint)*3 as string),2,'0'),'-28') dt ,
    max(case when dd.field_name = "MEASURE"  then dd.ch end)  as measure, -- тыс., млн.
    cast(cast(max(case when dd.field_name = "BB1.1600.3"  then dd.nm end) as double) as decimal(17,2) ) as TA1600    
from 
    (select 
               ods_validfrom ,
               ods_opc       ,
               docs_header_id,
               field_name    ,
               nm            ,
               dt            ,
               ch            ,
                row_number() over (partition by docs_header_id, field_name order by ods_validfrom desc) rn
          from core_internal_fok.docs_data) dd
    inner join 
    t_team_k7m_aux_p.k7m_fok_header_selected dh 
    on dd.docs_header_id = dh.id
where dd.rn    = 1
group by dh.SBRF_INN,     dh.year, dh.period
;
--Выручка и Расходы всего
drop table t_team_k7m_aux_p.k7m_fok_OFR;
create table t_team_k7m_aux_p.k7m_fok_OFR stored as orc as 
select
    dh.SBRF_INN as inn,
    dh.year as yr,
    dh.period as qt,
    max(case when dd.field_name = 'MEASURE'  then dd.ch end)  as measure, -- тыс., млн.
    cast(sum(case when dd.field_name = 'PU1.2110.3'  then cast(dd.nm  as double)end) as decimal(17,2) ) as vyruchka,
    cast(sum(case when dd.field_name in ('PU1.2210.3','PU1.2350.3','PU1.2220.3','PU1.2120.3')  then cast(dd.nm  as double) end)  as decimal(17,2) ) as total_expenses    
from 
    (select 
               ods_validfrom ,
               ods_opc       ,
               docs_header_id,
               field_name    ,
               nm            ,
               dt            ,
               ch            ,
                row_number() over (partition by docs_header_id, field_name order by ods_validfrom desc) rn
          from core_internal_fok.docs_data) dd
    inner join 
    t_team_k7m_aux_p.k7m_fok_header_selected dh 
    on dd.docs_header_id = dh.id
where dd.rn    = 1
     and dd.field_name in(  'PU1.2210.3','PU1.2350.3','PU1.2220.3','PU1.2110.3','PU1.2120.3','MEASURE') 
group by dh.SBRF_INN,     dh.year, dh.period;




--Свод по отчетности: ФОК+Интегрум
drop table t_team_k7m_aux_p.k7m_rep1600_final;
create table t_team_k7m_aux_p.k7m_rep1600_final stored as orc as 
select
    ul_inn,
    ta1600,
    dt,
    source
from (
    select 
        ul_inn,
        ta1600,
        dt,
        source,
        row_number() over (partition by ul_inn order by dt desc) rn
    from (
        select 
            ul_inn ,
            cast(dt as date) dt,
            cast(fs_value as double) ta1600,
            'I' source
        from t_team_k7m_aux_p.k7m_integrum_1600
        where fs_value<>00
        union all
        select 
            inn ul_inn, 
            cast(dt as date) dt,
            case 
                when measure = 'тыс.' then 1000
                when measure = 'млн.' then 1000000
            end*cast(ta1600 as double) ta1600,
            'F' source
        from t_team_k7m_aux_p.k7m_fok_reports
        where measure in ('тыс.','млн.')
            and ta1600 <> 0
        ) x
    ) x
where rn = 1
;

--******** ЕКС Загрузка очищенных от тех. версионности справочников**********    

--1 Клиенты
drop table t_team_k7m_aux_p.k7m_Z_CLIENT;
create table t_team_k7m_aux_p.k7m_Z_CLIENT stored as parquet as
select id,
        c_inn,
        c_kpp,
        c_filial,
        c_depart,
        c_okveds,
        class_id,
        c_name
from (
    select id,
        c_inn,
        c_kpp,
        c_filial,
        c_depart,
        c_okveds,
        class_id,
        c_name,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_client
    ) s
where rn = 1
    and ods_opc != 'D';

--2 Кредиты
drop table t_team_k7m_aux_p.k7m_z_all_cred;
create table t_team_k7m_aux_p.k7m_z_all_cred stored as orc as
select id,
        c_client,
        class_id,
        c_zalog,
        c_list_pay,
        c_num_dog,
        c_summa_dog,
        c_high_level_cr,
        c_ft_credit
        
from (
    select id,
        c_client,
        class_id,
        c_num_dog,
        c_summa_dog,
        c_zalog,
        c_list_pay,
        c_high_level_cr,
        c_ft_credit,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_pr_cred
    ) s
where rn = 1
    and ods_opc != 'D';
    
--Гарантии
Insert into t_team_k7m_aux_p.k7m_z_all_cred 
select id,
        c_client,
        class_id,
        c_zalog,
        c_list_pay,
        c_num_dog,
        c_summa,
        NULL,
        c_valuta
from (
    select id,
        c_PRINCIPAL c_client,
        'GUARANTIES' class_id,
        c_guarantie_num  c_num_dog,
        c_Security  c_zalog,
        c_list_fact c_list_pay,
        ods_opc,
        c_summa,
        c_valuta,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_guaranties
    ) s
where rn = 1
    and ods_opc != 'D';
   
--3 Счета
drop table t_team_k7m_aux_p.k7m_z_ac_fin;
create table t_team_k7m_aux_p.k7m_z_ac_fin stored as parquet as
select id,
        c_main_v_id,
        c_saldo,
        c_arc_move,
        c_filial,
        c_depart_u,
        c_depart,
        c_client_r,
        c_client_v
from (
    select id,
        c_main_v_id,
        c_saldo,
        c_arc_move,
        c_filial,
        c_depart_u,
        c_depart,
        c_client_r,
        c_client_v,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_ac_fin
    ) s
where rn = 1
    and ods_opc != 'D';
    
    
--4      Выдачи + гашения
drop table t_team_k7m_aux_p.k7m_z_fact_oper_issues;
create table t_team_k7m_aux_p.k7m_z_fact_oper_issues stored as parquet as
select
    id,
    collection_id,
    c_date,
    oper_type,
    c_summa
from (
    select id,
        collection_id,
        c_date,
        c_summa,
        ods_opc,
        case when c_oper in (
        --выдачи 
        2052198 , 2052228) then
        'ISSUE'
        else 'PAYMENT'
        end oper_type,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_fact_oper 
    where  c_oper in (
        --выдачи 
        2052198 , 2052228,
        --Гашения 
2052201,
2052204,
2052384,
2052417,
2052438,
2052468,
64828926174,
114471178372,
114471178660,
114471178716,
266761003265,
288061977365,
288061977381,
910050126028,
921448827992,
921448828072,
921448828084,
921448828088,
921448828092,
921448828104,
2922900801244,
2922900801250,
2922900801283,
2922900801290,
4505159680210,
5104943125537,
5104945093024
)
--and        c_date between unix_timestamp(cast(add_months(date'2017-12-06',-12) as timestamp))*1000 and unix_timestamp(cast(date'2017-12-06'as timestamp))*1000
    ) s
where rn = 1
    and ods_opc != 'D';
    

--5.Залоги
drop  table t_team_k7m_aux_p.k7m_z_zalog;
create table t_team_k7m_aux_p.k7m_z_zalog stored as orc as
select
    id,
    collection_id,
    c_acc_zalog,
        c_user_zalog,
        C_PART_TO_LOAN,
        c_market_sum,
        c_properties 
from (
    select id,
        collection_id,
        c_acc_zalog,
        c_user_zalog,
        C_PART_TO_LOAN,
        c_market_sum,
        c_properties,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_zalog c
    ) s
where rn = 1
    and ods_opc != 'D';

drop table t_team_k7m_aux_p.k7m_part_to_loan;
--6.Залоги-Крелиты
create table t_team_k7m_aux_p.k7m_part_to_loan stored as parquet as
select
    id,
    collection_id,
    c_product,
    c_prc_attr,
    c_part
from (
    select id,
        collection_id,
          c_product,
          c_part,
          c_prc_attr,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_part_to_loan c
    ) s
where rn = 1
    and ods_opc != 'D';
    
    
--7. Cчета   
drop table t_team_k7m_aux_p.k7m_Z_hoz_Op_Acc;
create table t_team_k7m_aux_p.k7m_Z_hoz_Op_Acc stored as orc as
select
    id,
    collection_id,
    C_ACCOUNT_DOG_1_2,
      C_NAME_ACCOUNT
from (
    select 
        id,
        collection_id,
        C_ACCOUNT_DOG_1_2,
          C_NAME_ACCOUNT,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.Z_hoz_Op_Acc c
    ) s
where rn = 1
    and ods_opc != 'D';


--8. Продукты   
drop table t_team_k7m_aux_p.k7m_Z_product;
create table t_team_k7m_aux_p.k7m_Z_product stored as orc as
select
    id,
    class_id,
    from_unixtime(cast(c_date_begin/1000 as bigint))  c_date_begin,
    C_ARRAY_DOG_ACC
from (
    select 
        id,
        class_id,
        c_date_begin,
        C_ARRAY_DOG_ACC,
          ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.Z_product c
    ) s
where rn = 1
    and ods_opc != 'D';


--9. Рын. стоимость обесп.
drop table t_team_k7m_aux_p.k7m_Z_ZALOG_MARKET_SUM;
create table t_team_k7m_aux_p.k7m_Z_ZALOG_MARKET_SUM stored as orc as
select
    id,
    collection_id,
    c_curr,
    c_summ,--TODO сделать перевод валюты в рубли
    c_date_beg,
    c_date_end
from (
    select c.id,
     c.collection_id,
    c.c_curr,
        c.c_summ,
        c_date_beg,
        c_date_end,
          ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.Z_ZALOG_MARKET_SUM c
    ) s
where rn = 1
    and ods_opc != 'D' ;

--10. св-ва
drop table t_team_k7m_aux_p.k7m_z_properties;
create table t_team_k7m_aux_p.k7m_z_properties stored as orc as
select
    id,
    collection_id,
    c_group_prop,
    c_bool,
    c_date_beg,
    c_date_end
from (
    select
        c.id,
        c.collection_id,
        c.c_group_prop,
        c_bool,
        c_date_beg,
        c_date_end,
          ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_properties c
    ) s
where rn = 1
    and ods_opc != 'D'
    ;
    
--11. Группы свойств:
drop table t_team_k7m_aux_p.k7m_z_property_grp;
create table t_team_k7m_aux_p.k7m_z_property_grp stored as orc as
select
    id,
    c_code
from (
    select
        c.id,
        c.c_code ,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_property_grp c
    ) s
where rn = 1
    and ods_opc != 'D'
    ;    
    
    
--12. Справочник валют
drop table t_team_k7m_aux_p.k7m_z_ft_money;
create table t_team_k7m_aux_p.k7m_z_ft_money stored as orc as
select
    id,
    c_jour_recont    ,
    c_cur_short
from (
    select id,
        c_jour_recont    ,
        c_cur_short,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_ft_money
    ) s
where rn = 1
    and ods_opc != 'D';    

drop table t_team_k7m_aux_p.k7m_z_recont;
create table t_team_k7m_aux_p.k7m_z_recont stored as orc as
select
        id,
        collection_id    ,
        c_course,
        c_unit,
        c_date_recount
from (
    select 
        id,
        collection_id    ,
        c_course,
        c_unit,
        c_date_recount,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_recont    
    ) s
where rn = 1
    and ods_opc != 'D';     
    
--Курсы валют
drop table t_team_k7m_aux_p.k7m_cur_courses;
create table t_team_k7m_aux_p.k7m_cur_courses stored as parquet as
select 
    id,
    c_cur_short,
    c_date_recount,
next_date_recount-1 till_c_date_recount,
cast(concat(substr(cast(  from_unixtime(cast(c_date_recount/1000 as bigint)) as string),1,10),' 00:00:00') as timestamp)   from_dt,
   cast(concat(substr(cast(date_sub(from_unixtime(cast(next_date_recount/1000 as bigint)),1) as string),1,10),' 23:59:59') as timestamp)  to_dt,
course
from (
select 
    m.id,
    m.c_cur_short,
    r.c_date_recount,
    lead(c_date_recount,1,unix_timestamp(cast(date'4000-01-01'as timestamp))*1000) over (partition by m.id order by c_date_recount) next_date_recount,
    cast(r.c_course as double)/cast(r.c_unit as double) course
from     
    t_team_k7m_aux_p.k7m_z_ft_money m
    join 
    t_team_k7m_aux_p.k7m_z_recont r
    on m.c_jour_recont  = r.collection_id       ) x;


drop table t_team_k7m_aux_p.k7m_z_subbranch;
create table t_team_k7m_aux_p.k7m_z_subbranch stored as parquet as
select id,
        c_terbank
from (
    select id,
        c_terbank,
        ods_opc,
        row_number() over ( partition by id order by ods_validfrom desc ) as rn
    from core_internal_eks.z_subbranch
    ) s
where rn = 1
    and ods_opc != 'D';
    
    
    