--Заполнение LK/LKC
--Запускать после формирования ключей по CLU-CLF (для проверки важно чтоб формирование ключей отработало по  t_team_k7m_aux_p.lk_lkc_raw  - иначе будут ошибки)
drop table t_team_k7m_aux_p.lk_lkc_with_keys
drop table t_team_k7m_aux_p.lk_lkc
drop table t_team_k7m_pa_p.lk
drop table t_team_k7m_pa_p.lkc



create table t_team_k7m_aux_p.lk_lkc_with_keys
stored as parquet
as
    select 
        rw.crit_id,
        rw.flag_new ,
        rw.link_prob,
        rw.quantity,
        rw.link_id,
        rw.T_FROM,
        rw.ID_FROM,
        rw.INN_FROM, 
        rw.post_nm_from,
        rw.T_TO,
        rw.ID_TO,
        rw.INN_TO , 
        k1i.u7m_id u7m_id_from_by_inn,
        k1c.u7m_id u7m_id_from_by_crm,
        k1f.f7m_id as f7m_id_from,
        k2i.u7m_id u7m_id_to_by_inn,
        k2c.u7m_id u7m_id_to_by_crm        
    from t_team_k7m_aux_p.lk_lkc_raw rw
    left join t_team_k7m_aux_d.clu_keys k1i
          on 
            k1i.inn = rw.inn_from
            and rw.t_from = 'CLU' 
            and k1i.status='ACTIVE' 
    left join t_team_k7m_aux_d.clu_keys k1c
          on 
            k1c.crm_id = rw.ID_FROM
            and rw.t_from = 'CLU'
            and k1c.status='ACTIVE' 
    left join t_team_k7m_aux_d.clu_keys k2i
          on 
            k2i.inn = rw.inn_to
            and rw.t_to = 'CLU' 
            and k2i.status='ACTIVE' 
    left join t_team_k7m_aux_d.clu_keys k2c
          on 
            k2c.crm_id = rw.ID_to
            and rw.t_to =  'CLU'
            and k2c.status='ACTIVE' 
    left join t_team_k7m_aux_d.clf_keys k1f
          on k1f.link_id = rw.link_id;
               


---------------------------------------------------------------------
--  Выполнить проверки:
-- Записать расхождения в журнал CUSTOM_LOG с типом ERR
---------------------------------------------------------------------

--1) Не найдены суррогатные ключи (ошибка, если empty>0 ): 
 --сторона FROM
 select 
    t_from,
    crit_id, 
    sum(
        case   when 
                not (u7m_id_from_by_inn is not null 
                or  coalesce(length(inn_from),0) not in (10,12)
                or  inn_from like '00%')
                and  
                    not (
                    u7m_id_from_by_crm is not null
                    or id_from is not null
                    )
                 and t_from ='CLU'
              or 
                f7m_id_from is null 
                and t_from ='CLF' 
            then 1
            else 0 
    end) empty ,
    count(*) total
from t_team_k7m_aux_p.lk_lkc_with_keys
where id_from is not null or coalesce(length(inn_from)) in (10,12)
group by        t_from, crit_id
having empty>0
        
--сторона TO
 select 
    t_to,
    crit_id, 
    sum(
        case   when 
                not (u7m_id_to_by_inn is not null 
                or  coalesce(length(inn_to),0) not in (10,12)
                or  inn_to like '00%')
                and  
                    not (
                    u7m_id_to_by_crm is not null
                    or id_to is not null
                    )
                 and t_to ='CLU'
              or 
                f7m_id_from is null 
                and t_to ='CLF' 
            then 1
            else 0 
    end) empty ,
    count(*) total
from t_team_k7m_aux_p.lk_lkc_with_keys
where id_to is not null or coalesce(length(inn_to)) in (10,12)
group by        t_to, crit_id
having empty>0


-- Дубли (не должно быть)
select link_id, count(*) 
from t_team_k7m_aux_p.lk_lkc_with_keys
group by        link_id
having count(*)>1
 
--Конфликт в ключах(Не должно быть):
 --сторона FROM
 select crit_id, count(*) 
from t_team_k7m_aux_p.lk_lkc_with_keys
where 
        u7m_id_from_by_inn <> u7m_id_from_by_crm 
group by         crit_id

 --сторона FROM
 select crit_id, count(*) 
from t_team_k7m_aux_p.lk_lkc_with_keys
where 
        u7m_id_to_by_inn <> u7m_id_to_by_crm 
group by         crit_id

------------КОНЕЦ ПРОВЕРОК--------------
drop table t_team_k7m_aux_p.lk_lkc
create table t_team_k7m_aux_p.lk_lkc
stored as parquet
as
select
   cast(row_number() over (order by x.link_id ) as string) lkc_id,
   x.link_id,
   CRIT_ID,
   FLAG_NEW             ,
   LINK_PROB,
   QUANTITY             ,
   U7M_ID_FROM,
   T_FROM,
   post_nm_from,
   U7M_ID_TO,
   T_TO   
from (   
    select 
       c.link_id,
       c.CRIT_ID              ,
       c.FLAG_NEW             ,
       c.LINK_PROB            ,
       c.QUANTITY             ,
       case 
       when
        T_FROM = 'CLF' then f7m_id_from
        else
       cast(coalesce(c.u7m_id_from_by_crm,c.u7m_id_from_by_inn) as string) 
       end U7M_ID_FROM          ,
       c.T_FROM               ,
       c.post_nm_from,
       cast(coalesce(c.u7m_id_to_by_crm,c.u7m_id_to_by_inn) as string) as U7M_ID_TO,
       c.T_TO                  
    from t_team_k7m_aux_p.lk_lkc_with_keys c
     ) x
where     U7M_ID_FROM is not null
    and U7M_ID_TO is not null;
         
     
--Результат в   LK/LKC  
    create table t_team_k7m_pa_p.lk
    stored as parquet
    as      
    select 
            cast( row_number() over (order by U7M_ID_FROM, U7M_ID_TO) as string) lk_id,
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO,
            FLAG_NEW,
            'EXEC_ID from ETL_EXEC!' exec_id --Получить из системы логирования
    from 
     (
        select
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO,
            max(FLAG_NEW) FLAG_NEW
        from t_team_k7m_aux_p.lk_lkc
        group by 
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO
        )     x
   
    drop table t_team_k7m_pa_p.lkc ;
   
------------------------------------------- 
create table t_team_k7m_pa_p.lkc
    stored as parquet
    as   
     select
        x.lkc_id,
        x.lk_id,
        x.crit_id,
        'EXEC_ID from ETL_EXEC!' exec_id, --Получить из системы логирования
        x.LINK_PROB            ,
        x.QUANTITY,
		x.post_nm_from
     FROM (
      select 
        lk.lkc_id,
        l.lk_id,
        lk.crit_id,
        lk.LINK_PROB            ,
        lk.QUANTITY,
        lk.post_nm_from
        row_number() over (partition by l.lk_id,lk.crit_id order by lk.LINK_PROB desc,lk.QUANTITY desc,   lk.lkc_id ) rn
     from 
        t_team_k7m_aux_p.lk_lkc lk
        left join  t_team_k7m_pa_p.lk l
        on concat(l.T_FROM,cast(l.U7M_ID_FROM as string),l.T_TO ,cast(l.U7M_ID_TO as string) ) = concat(lk.T_FROM,cast(lk.U7M_ID_FROM as string),lk.T_TO ,cast(lk.U7M_ID_TO as string) )
     ) x
     where x.rn=1;
--------------------------
--  Выполнить проверку:
-- Записать расхождения в журнал CUSTOM_LOG с типом ERR
---------------------------------------------------------------------

--Нарушена целостность связи LK-LKC
select crit_id, count(*) 
from t_team_k7m_pa_p.lkc
where lk_id is null
group by         crit_id

--FROM равен TO
select  count(*) 
from t_team_k7m_pa_p.lk
where U7M_ID_FROM = U7M_ID_TO

------Конец проверок