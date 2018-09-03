
--Подготавливаем перечень неполных BO (и ключей которых нехватает)
create table custom_cb_k7m.bo_incomplete
stored as parquet
as
select 
    core.bo_id,
    core.u7m_id,
    core.key
from (
    select 
        k.bo_id,
        k.u7m_id,
        s.key
    from  custom_cb_k7m.bo_keys k
        cross join custom_cb_k7m_aux.dict_mandatory_keys_out s
    where s.obj = 'BO'    
    ) core 
    left join  custom_cb_k7m.bo
    on core.bo_id =bo.bo_id
        and core.u7m_id =bo.u7m_id
        and core.key =bo.key
where bo.key is null        

--ФИЛЬТР
--В результирующую таблицу попадут только ПОЛНЫЕ BO
select *
from (
select * from bo_gen
union all
select * from bo_bbo
union all
select * from bo_lgd
union all
select * from bo_skr
) b
where   not exists (
    select bo_id,u7m_id
    from custom_cb_k7m.bo_incomplete bi
    where bi.bo_id=b.bo_id
        and bi.u7m_id = b.u7m_id
    )