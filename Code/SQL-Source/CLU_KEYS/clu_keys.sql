drop table t_team_k7m_aux_p.clu_keys_raw;
drop table t_team_k7m_aux_p.clu_keys_core;
drop table t_team_k7m_aux_p.clu_keys_crm_only;
drop table t_team_k7m_aux_p.clu_keys_inn_only;
drop table t_team_k7m_aux_p.clu_keys_prepare;
drop table t_team_k7m_aux_p.clu_keys_active;


drop table t_team_k7m_aux_p.clu_keys_new;
drop table t_team_k7m_aux_p.clu_keys_new_close_1;
drop table t_team_k7m_aux_p.clu_keys_new_close_2;
drop table t_team_k7m_aux_p.clu_keys_result;



--ИНИЦИАЛИЗАЦИЯ  : предполагается, что этот блок отработает 1 раз при развертывании витрины.
--УСЛОВИЕ ЗАПУСКА БЛОКА ЭТО отсутствие таблицы clu_keys
--IF 
    create table t_team_k7m_aux_p.clu_keys
    (
     u7m_id string,
     status string,
     closed_and_merged_to_u7m_id string,
     create_exec_id string,
     crm_id string,
     inn string,
     u7m_from_crm_flag string
     )
    stored as parquet;
     
    insert into t_team_k7m_aux_p.clu_keys
    select 
     id as u7m_id ,
     'INACTIVE' as  status,
     cast(null as string) as closed_and_merged_to_u7m_id,
     'INI' as create_exec_id,
     id as crm_id,
     inn ,
     'Y' as u7m_from_crm_flag
    from (
    select 
        id,
        inn,
        row_number() over (partition by inn order by s ) rn
    from (
        --Приоритет базису при определении crm_id для ИНН
        select 
            id as id,
            inn,
            2 s
        from t_team_k7m_aux_d.k7m_crm_org_major
        union all
        select 
            org_crm_id as id,
            org_inn_crm_num as inn,
            1 s
        from t_team_k7m_aux_d.basis_client
        ) x
        ) y
    where y.rn = 1  ;
--END IF   
--ИНИЦИАЛИЗАЦИЯ КОНЕЦ



--0. Соберем все ключи
create table    t_team_k7m_aux_p.clu_keys_raw
stored as parquet
as
select 
    distinct 
    inn,
    case when  cnt>1 
    then
        cast (null as string)
    else crm_id
    end crm_id
from 
(
select 
    max(crm_id) as crm_id ,
    max(inn) as inn , 
    count(inn) cnt
from (
select 
    crm_id,
    inn
from (
select 
    id_from crm_id,
    inn_from inn
from t_team_k7m_aux_d.lk_lkc_raw
where t_from = 'CLU'
union ALL
select 
    id_to crm_id,
    inn_to inn
from t_team_k7m_aux_d.lk_lkc_raw
where t_to = 'CLU'
union ALL
select 
    org_crm_id crm_id,
    org_inn_crm_num inn
from t_team_k7m_aux_d.basis_client
) x
where 
     (crm_id is not null or 
     inn is not null
     AND length(inn) in (10,12)
     and inn not like '00%'
     )
group by 
    crm_id,
    inn
)  y
group by coalesce(inn, crm_id) 
) z;
    

--ПРОВЕРКИ
-- Если один  4-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ    
select inn,
       count(*) 
from t_team_k7m_aux_p.clu_keys_raw    
where crm_id is not null    
    and inn is  not null 
group  by inn;
    
select crm_id,
       count(*) 
from t_team_k7m_aux_p.clu_keys_raw    
where crm_id is not null    
    and inn is  not null 
group  by crm_id;
    
--1. Полные ключи (те, у которых есть CRM_ID и ИНН)    
create table t_team_k7m_aux_p.clu_keys_core
stored as parquet
as
select 
    crm_id, 
    inn,
    0 lvl
from t_team_k7m_aux_p.clu_keys_raw
where inn is not null
    and crm_id is not null;    

--2. Только CRM_ID
create table t_team_k7m_aux_p.clu_keys_crm_only
stored as parquet
as
select ce.crm_id 
from t_team_k7m_aux_p.clu_keys_raw ce
    left join 
    t_team_k7m_aux_p.clu_keys_core kc
on kc.crm_id = ce.crm_id
where ce.inn is null
   and ce.crm_id is not null
   and kc.inn is null;
    
--3. Обогащение ИНН по CRM_ID    
insert into t_team_k7m_aux_p.clu_keys_core
select c.crm_id,
       max(m.sbrf_inn) inn,
       1 lvl
from t_team_k7m_aux_p.clu_keys_crm_only c
    left join t_team_k7m_stg.crm_s_org_ext_x m
    on m.row_id = c.crm_id
group by     c.crm_id;  
    
    
--4. Только ИНН
create table t_team_k7m_aux_p.clu_keys_inn_only
stored as parquet
as
select ce.inn
from t_team_k7m_aux_p.clu_keys_raw ce
    left join 
    t_team_k7m_aux_p.clu_keys_core kc
on kc.inn = ce.inn
where ce.inn is not null
    and ce.crm_id is null
    and kc.crm_id is null;   
    
--5. Обогащение CRM_ID по ИНН
insert into  t_team_k7m_aux_p.clu_keys_core
select 
    max(m.id) crm_id,
    c.inn,
    2 lvl
from  t_team_k7m_aux_p.clu_keys_inn_only c
    left join t_team_k7m_aux_d.k7m_crm_org_major m
    on m.inn = c.inn
group by     c.inn;

--Промежуточная таблица с метками дублей по ИНН и CRM_ID
create table t_team_k7m_aux_p.clu_keys_prepare
stored as parquet
as
select 
    c.crm_id,
    c.inn,
    c.lvl,
    row_number() over (partition by c.crm_id order by c.lvl, c.inn)  crm_dup_no,
    row_number() over (partition by c.inn order by c.lvl, c.crm_id) inn_dup_no
from t_team_k7m_aux_p.clu_keys_core c;
--ПРОВЕРКИ:
--1. Противоречия crm_id и ИНН в поступившем наборе данных. Если есть записи - записать в CUSTOM_LOG с ошибкой
 select *
 from t_team_k7m_aux_p.clu_keys_prepare x
where x.crm_dup_no>1 and x.crm_id is not null
 or x.inn_dup_no>1 and x.inn_id is not null;

--Формируем непротиворечивый, обогащенный набор ключей
create table t_team_k7m_aux_p.clu_keys_active
stored as parquet
as
select 
    case 
        when crm_id is not null 
            and crm_dup_no=1 
            then  crm_id
            else cast(null as string)
    end crm_id,
    case 
       when inn is not null 
            and inn_dup_no=1 
            then  inn
            else cast(null as string)
    end inn
from t_team_k7m_aux_p.clu_keys_prepare
where (crm_dup_no=1 or inn_dup_no=1);

--ПРОВЕРКИ:
--Дубли ИНН. Если есть записи - записать в CUSTOM_LOG с ошибкой
select inn, count(*) 
from t_team_k7m_aux_p.clu_keys_active
where inn is not null
group by inn
having count(*)>1;

--Дубли CRM_ID. Если есть записи - записать в CUSTOM_LOG с ошибкой
select crm_id, count(*) 
from t_team_k7m_aux_p.clu_keys_active
where crm_id is not null
group by crm_id
having count(*)>1;   
    



--ВНИМАНИЕ! ЗАПОЛНЕНИЕ ПЕРЕМЕННОЙ:
--НАЙТИ ЧИСЛО START_ID=select coalesce(max(cast(u7m_id as bigint)),0) from t_team_k7m_aux_p.clu_keys where u7m_from_crm_flag='N'

--ПРОВЕРКИ
--Предусловия на ключи:
-- Если один  4-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
--1
select 
 u7m_id,closed_and_merged_to_u7m_id,
 count(*)
 from t_team_k7m_aux_p.clu_keys
group by u7m_id,closed_and_merged_to_u7m_id
having count(*)>1
--2
select 
 inn,
 count(*)
 from  t_team_k7m_aux_p.clu_keys
where closed_and_merged_to_u7m_id is  null
    and inn is not null
group by inn
having count(*)>1
--3
select 
 crm_id,
 count(*)
 from  t_team_k7m_aux_p.clu_keys
where closed_and_merged_to_u7m_id is  null
    and crm_id is not null
group by crm_id
having count(*)>1
--4
select *
from t_team_k7m_aux_p.clu_keys
where u7m_id is null or inn is null and crm_id is null   


create table t_team_k7m_aux_p.clu_keys_new
 stored as parquet
 as
 select 
    COALESCE(x.u7m_id,coalesce(x.crm_id, cast(!START_ID!+RN as string) )) u7m_id,
    x.crm_id,
    x.inn,
    1 priority,
    cast (null as string)  closed_and_merged_to_u7m_id,
    create_exec_id,
    k1_u7m_id,
    k1_inn,
    k1_crm_ID,
    k2_u7m_id,
    k2_inn,
    k2_crm_ID    ,
    case 
       when x.u7m_id is not null then x.u7m_from_crm_flag
       when x.crm_id is not null then 'Y'
       else 'N'
    end    u7m_from_crm_flag,
    sel_type
 FROM (
 SELECT 
    --ВНИМАНИЕ ПРИ ИЗМЕНЕНИИ CASE нужно его дублировать 3 раза!
    ROW_NUMBER() OVER (PARTITION BY   CASE 
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS  string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.U7M_ID
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.U7M_ID
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.U7M_ID
        ELSE CAST(NULL AS  string) END ORDER BY A.CRM_ID ) RN,
   CASE 
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.U7M_ID
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.U7M_ID
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.U7M_ID
        ELSE CAST(NULL AS string) END
        U7M_ID,
        CASE 
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS  string)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS  string)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.create_exec_id
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.create_exec_id
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.create_exec_id
        ELSE CAST(NULL AS  string) END create_exec_id,
        a.crm_id,
        a.INN ,
        k1.u7m_id k1_u7m_id,
        k1.inn  k1_inn,
        k1.crm_ID  k1_crm_ID,
        k2.u7m_id k2_u7m_id,
        k2.inn  k2_inn,
        k2.crm_ID  k2_crm_ID,
        CASE 
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then CAST(NULL AS STRING)
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then CAST(NULL AS STRING)
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  K2.u7m_from_crm_flag
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  K1.u7m_from_crm_flag
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  K1.u7m_from_crm_flag
        ELSE CAST(NULL AS STRING) END u7m_from_crm_flag,
        CASE 
        when K1.inn<>a.inn or K2.inn<>a.inn or K1.crm_id<>a.crm_id or K2.crm_id<>a.crm_id then '1'
        when K1.inn is not null and a.inn is null or   K2.crm_id is not null and a.crm_id is null        then '2'
        WHEN K1.U7M_ID IS NULL AND K2.U7M_ID IS NOT NULL THEN  '3'
        WHEN K2.U7M_ID IS NULL AND K1.U7M_ID IS NOT NULL THEN  '4'
        WHEN K2.U7M_ID =K1.U7M_ID  THEN  '5'
        ELSE CAST(NULL AS string) END sel_type
        
    from 
    t_team_k7m_aux_p.clu_keys_active a
    left join t_team_k7m_aux_p.clu_keys k1
    on k1.crm_id = a.crm_id
        AND k1.closed_and_merged_to_u7m_id is null
    left join t_team_k7m_aux_p.clu_keys k2
    on k2.inn = a.inn
        AND k2.closed_and_merged_to_u7m_id is null
       ) X


        
--Закрытые (из-за перебитого соответствия ИНН-CRM_ID ключи) кейс1
create table t_team_k7m_aux_p.clu_keys_new_close_1
stored as parquet
as
select  k.u7m_id,
        k.crm_id,
        k.inn,
        2 priority,
        n.u7m_id closed_and_merged_to_u7m_id ,
        n.inn as actual_inn,
        k.u7m_from_crm_flag
 from t_team_k7m_aux_p.clu_keys_new n 
   join t_team_k7m_aux_p.clu_keys k
   on n.crm_id = k.crm_id
where k.closed_and_merged_to_u7m_id is null
    and (n.inn <> k.inn        
        or n.inn is null and k.inn is not null 
        or n.u7m_id <>k.u7m_id and  n.inn is not null and k.inn is  null        
        );
    
--Закрытые (из-за перебитого соответствия ИНН-CRM_ID ключи) кейс2
create table t_team_k7m_aux_p.clu_keys_new_close_2
stored as parquet
as
select  k.u7m_id,
        k.crm_id,
        k.inn,
        3 priority,
        n.u7m_id closed_and_merged_to_u7m_id,
        n.crm_id actual_crm_id,
        k.u7m_from_crm_flag
 from t_team_k7m_aux_p.clu_keys_new n 
   join t_team_k7m_aux_p.clu_keys k
   on n.inn = k.inn
where k.closed_and_merged_to_u7m_id is null
    and ( n.crm_id<>k.crm_id     
        or n.crm_id is null and k.crm_id is not null
        or n.u7m_id <>k.u7m_id and  n.crm_id is not null and k.crm_id is  null
        );


--Готовая таблица с ключами
--Необходимо проставить вместь !EXEC_ID! exec_id текущего расчета
create table t_team_k7m_aux_p.clu_keys_result
 stored as parquet
 as
 select 
        u7m_id,
        crm_id,
        inn,
        closed_and_merged_to_u7m_id,
        coalesce(create_exec_id,!EXEC_ID!)  create_exec_id,
        status,
        u7m_from_crm_flag
 from ( 
    select 
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        create_exec_id,
        status,
        row_number() over (partition by u7m_id, closed_and_merged_to_u7m_id order by priority ) rn,
        u7m_from_crm_flag
    from (
     select 
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        create_exec_id,
        'ACTIVE' status,
        u7m_from_crm_flag
     from t_team_k7m_aux_p.clu_keys_new
     union all
     select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        cast(null as string) as create_exec_id,
        'INACTIVE' status,
        u7m_from_crm_flag
     from  t_team_k7m_aux_p.clu_keys_new_close_1
     union all
     select
        u7m_id,
        crm_id,
        inn,
        priority,
        closed_and_merged_to_u7m_id,
        cast(null as string) as create_exec_id,
        'INACTIVE' status,
        u7m_from_crm_flag
     from  t_team_k7m_aux_p.clu_keys_new_close_2
     union all
     select
        b.u7m_id,
        b.crm_id,
        b.inn,
        99 priority,
        b.closed_and_merged_to_u7m_id,
        b.create_exec_id,
        'INACTIVE' status,
        b.u7m_from_crm_flag
     from  t_team_k7m_aux_p.clu_keys b
        left join t_team_k7m_aux_p.clu_keys_new_close_1 c1
        on c1.u7m_id = b.u7m_id
        left join t_team_k7m_aux_p.clu_keys_new_close_2 c2
        on c2.u7m_id = b.u7m_id        
     where 
        c1.u7m_id is null    
        and c2.u7m_id is null    
    ) act
    ) reslt
    where rn=1;
    

--Постусловия на ключи:
-- Если один  6-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
--1
select 
 u7m_id,closed_and_merged_to_u7m_id,
 count(*)
 from t_team_k7m_aux_p.clu_keys_result
group by u7m_id,closed_and_merged_to_u7m_id
having count(*)>1
--2
select 
 inn,
 count(*)
 from  t_team_k7m_aux_p.clu_keys_result
where closed_and_merged_to_u7m_id is  null
    and inn is not null
group by inn
having count(*)>1
--3
select 
 crm_id,
 count(*)
 from  t_team_k7m_aux_p.clu_keys_result
where closed_and_merged_to_u7m_id is  null
    and crm_id is not null
group by crm_id
having count(*)>1
--4
select *
from t_team_k7m_aux_p.clu_keys_result
where u7m_id is null or inn is null and crm_id is null    
    
--5    
select *
from t_team_k7m_aux_p.clu_keys_raw s
    left join 
    t_team_k7m_aux_p.clu_keys_result k
    on s.crm_id = k.crm_id
        and k.closed_and_merged_to_u7m_id is   null
        and k.status = 'ACTIVE'
where k.u7m_id is null     
       and s.crm_id is not null
--6
select *
from t_team_k7m_aux_p.clu_keys_raw s
    left join 
    t_team_k7m_aux_p.clu_keys_result k
    on s.inn = k.inn
        and k.closed_and_merged_to_u7m_id is   null
        and k.status = 'ACTIVE'
where k.u7m_id is null     
    and s.inn is not null
    
    
    
    
--В ПРОТОТИПЕ НЕ ЗАПУСКАЮ    
---Собираем единое представление 
--Резервируем ключи на случай падения 
drop table t_team_k7m_aux_p.clu_keys_backup
alter table  t_team_k7m_aux_p.clu_keys rename to  t_team_k7m_aux_p.clu_keys_backup
alter table t_team_k7m_aux_p.clu_keys_result rename to t_team_k7m_aux_p.clu_keys
    
    
    