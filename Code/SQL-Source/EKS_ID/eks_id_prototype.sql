--Таблица с Соответствием ЕКС_ID по ИНН  для CLU
-- Ключко Павел 2018-03-02, 
--  перевод на CLU_KEYS 2018-04-02

-- Описание:
-- Данный скрипт готовит агрегат, который будет использован при формировании CLU (поля EKS_DUPLICATE_CNT и EKS_ID).
-- Результат в таблице CLU_to_eks_id может содержать >1 записи на ИНН.
-- Для определения значения полей EKS_DUPLICATE_CNT и EKS_ID в CLU требуется брать поля с значением priority=1.
-- Прошу обратить внимание: '2018-02-01 00:00:00' -- это дата расчета, в витрине ее надо заменить на соответствующий параметр из EXEC_STTS


--Бизнес-Требования (согласовала Елена Шилова 26.02.2018):
--1.По списку Базис (Заемщики):
-- 
--Доп.фильтры:
--исключение филиалов в названии (отделение, филиал и проч.) 
--анализ КПП  на коды филиалов (коды, 02,03,43  -филиалы) 
--ID ЕКС (MAX), при наличии дублей. 
--Обязательное наличие договора ДБО
--Тербанк ВКО = Тербанк р.счета
-- 
-- 
--2.По CLU в части Поручителей.
--Вводные: 
--Обязательно CRM ID для всех ЮЛ (в т.ч. Поручителей) 
--наличие Договора НЕ обязательно 
--наличие р.с. НЕ обязательно 
-- 
--Фильтры для Поручителей:
--1.ИНН
--2. КПП фильтр (02,03,43-филиалы)
--3. наименование (искл. филиалы)
--4. Приоритет – наличие договора ДБО
--     Если ДБО нет, то Р.с
--5. ID ЕКС (MAX) при наличии дублей.
--Если есть дубли, то фиксирует признак "количество дублей в ЕКС"
  

 
drop table  t_team_k7m_aux_p.clu_extended_basis; 
drop table  t_team_k7m_aux_p.CLU_to_eks_id;
drop table  t_team_k7m_aux_p.clu_base;
/

--Перечень ИД ЮЛ, которые входят в расширенный базис
create table t_team_k7m_aux_p.clu_extended_basis
stored as parquet
as
select 
    u7m_id,
    case when max(is_basis)=1 then 'Y'
    else 'N'
    end FLAG_BASIS_CLIENT
from  (
    select 
        u7m_id_from as u7m_id,
        0 as is_basis
    from t_team_k7m_pa_d.lk
    where t_from ='CLU'
    union all
    select 
        u7m_id_to as u7m_id,
        0 as is_basis
    from t_team_k7m_pa_d.lk
    where t_to ='CLU'    
    union all        
    select  
        bs.u7m_id,
        1 as is_basis
    from
    t_team_k7m_aux_p.clu_keys  bs
    join t_team_k7m_aux_d.basis_client bb 
    on bb.org_inn_crm_num = bs.inn
    ) x
group by u7m_id
/

--1.Базис
create table t_team_k7m_aux_p.CLU_to_eks_id  stored as parquet as
select 
  u7m_id,
  eks_id,
  count(*) over (partition by x.inn) EKS_DUPLICATE_CNT,
  row_number() over (partition by x.inn order by x.eks_id desc) priority,
  inn,
  kpp,
  tb_vko_crm_name,
  TB_CODE,
  acc_example,
  bal2_max,
  bal2_min,
  FLAG_BASIS_CLIENT
from (
select 
  max(bs.inn) inn,
  max(c.c_kpp) kpp,
  max(bb.tb_vko_crm_name) tb_vko_crm_name,
  max(SUBSTR(b.c_local_code,1,2)) TB_CODE,
  max(a.c_main_v_id) acc_example,
  max(substr(a.c_main_v_id,1,5)) bal2_max,
  max(substr(a.c_main_v_id,1,5)) bal2_min,
  eb.FLAG_BASIS_CLIENT,
  bs.u7m_id,
  c.id eks_id
from
    t_team_k7m_aux_d.clu_extended_basis eb
    join 
    t_team_k7m_aux_d.clu_keys  bs
    on eb.u7m_id = bs.u7m_id
    join t_team_k7m_aux_d.basis_client bb 
    on bb.org_inn_crm_num = bs.inn
    JOIN 
    t_team_k7m_aux_d.rdm_tb_mapping M   --Ручная таблица 
    ON M.crm_tb_name = bb.tb_vko_crm_name
    join 
    t_team_k7m_stg.eks_Z_CLIENT c
    on bs.inn = c.c_inn
    join t_team_k7m_stg.eks_z_ac_fin a 
    on  c.id  = coalesce(a.c_client_r,a.c_client_v)
    join t_team_k7m_stg.eks_Z_solClient s
    on c.id   = s.c_client 
    join t_team_k7m_stg.eks_z_product  p 
    on p.id =  s.id   
    join t_team_k7m_stg.eks_z_branch b
    on a.c_filial = b.id  
where 
   CAST ('2018-02-01 00:00:00'   AS TIMESTAMP)   between a.c_date_op and coalesce(a.c_date_close,CAST ('2018-02-01 00:00:00'   AS TIMESTAMP))
    and CAST ('2018-02-01 00:00:00'   AS TIMESTAMP)   between p.c_date_begin and coalesce(p.c_date_close,CAST ('2018-02-01 00:00:00'   AS TIMESTAMP))
    and substr(a.c_main_v_id,1,5) between '40500' and '40807'
    and bs.status ='ACTIVE'
    and eb.FLAG_BASIS_CLIENT ='Y'
    AND case when M.tb_code='00' then  '38' else M.tb_code end =  case when SUBSTR(b.c_local_code,1,2)='00' then  '38' else SUBSTR(b.c_local_code,1,2) end 
    and upper(c.c_name) not like '%ФИЛИАЛ%'
    and upper(c.c_name) not like '%ОТДЕЛЕНИЕ%'
    and upper(c.c_name) not like '%УЧАСТОК%'  
    and substr(c.c_kpp,5,2) not in ('02','03','04','05','31','32','43','44','45')      
group by 
    bs.u7m_id,
    c.id  ,
    eb.FLAG_BASIS_CLIENT
) x
/

--Связи
insert into t_team_k7m_aux_p.CLU_to_eks_id
select 
  u7m_id,
  eks_id,
  count(*) over (partition by x.inn) EKS_DUPLICATE_CNT,
  row_number() over (partition by  x.inn order by  x.is_dbo, x.is_acc, x.eks_id desc) priority,
  inn,
  kpp,
    cast(null as string) tb_vko_crm_name,
  null TB_CODE,
  acc_example,
  concat('is_dbo=',is_dbo)  bal2_max,
  concat('is_acc=',is_acc) bal2_min,
  FLAG_BASIS_CLIENT
from (
select 
  max(bs.inn) inn,
  max(c.c_kpp) kpp,
  max(a.c_main_v_id) acc_example,
  max(substr(a.c_main_v_id,1,5)) bal2_max,
  max(substr(a.c_main_v_id,1,5)) bal2_min,
  eb.FLAG_BASIS_CLIENT,
  bs.u7m_id,
  c.id eks_id,
  min(case 
    when p.id is null then '1' 
    else '0'
  end) is_dbo,
  min(case 
    when a.id is null then '1' 
    else '0'
  end) is_acc
from
    t_team_k7m_aux_p.clu_extended_basis eb
    join 
    t_team_k7m_aux_p.clu_keys  bs
    on eb.u7m_id = bs.u7m_id
    left join t_team_k7m_aux_d.basis_client bb 
    on bb.org_inn_crm_num = bs.inn    
    join  
    t_team_k7m_stg.eks_Z_CLIENT c
    on bs.inn = c.c_inn
    left join
        (
        select
            ai.id,
            ai.c_main_v_id,
            coalesce(ai.c_client_r,ai.c_client_v) c_client
        from t_team_k7m_stg.eks_z_ac_fin ai 
        where substr(ai.c_main_v_id,1,5) between '40500' and '40807'   
            and CAST ('2018-02-01 00:00:00'   AS TIMESTAMP)   between ai.c_date_op and coalesce(ai.c_date_close,CAST ('2018-02-01 00:00:00'   AS TIMESTAMP))
        ) a
    on c.id = a.c_client 
    left join t_team_k7m_stg.eks_Z_solClient s
    on c.id  = s.c_client 
    left join   
        (
        select p.id
        from     t_team_k7m_stg.eks_z_product p
        where CAST ('2018-02-01 00:00:00'   AS TIMESTAMP)   between p.c_date_begin and coalesce(p.c_date_close,CAST ('2018-02-01 00:00:00'   AS TIMESTAMP))
        ) p  
    on p.id =  s.id  
where  
    bs.status ='ACTIVE'
    and bb.org_inn_crm_num is null
    and eb.FLAG_BASIS_CLIENT ='N'
    and upper(c.c_name) not like '%ФИЛИАЛ%'
    and upper(c.c_name) not like '%ОТДЕЛЕНИЕ%'
    and upper(c.c_name) not like '%УЧАСТОК%'  
    and substr(c.c_kpp,5,2) not in ('02','03','04','05','31','32','43','44','45')      
group by 
    bs.U7M_id,
    c.id,
    eb.FLAG_BASIS_CLIENT        
) x
/




---Основа для CLU
create table t_team_k7m_aux_p.clu_base
stored as parquet 
as
select 
    cast(k.u7m_id as string) u7m_id,
    k.crm_id,
    k.inn,
    eb.FLAG_BASIS_CLIENT,
    e.eks_id,
    e.eks_duplicate_cnt  
from  
    t_team_k7m_aux_p.clu_extended_basis eb
    join 
    t_team_k7m_aux_p.clu_keys  k
    on eb.u7m_id = k.u7m_id
    left join (
        select 
            u7m_id, 
            eks_id,
            eks_duplicate_cnt,
            tb_vko_crm_name
        from t_team_k7m_aux_p.CLU_to_eks_id
        where priority = 1 ) e
    on e.u7m_id = k.u7m_id
where k.status = 'ACTIVE'
 
/


--Проверки(2 шт.) Писать ошибку в custom_log если найдены записи:
--1.
SELECT 
    INN,
    SUM(SGN) SGN
FROM (
    SELECT INN, -1 SGN
    FROM t_team_k7m_aux_p.clu_base
    where flag_basis_client = 'Y'
    UNION ALL 
    SELECT 
        org_inn_crm_num,
        1 SGN
    FROM t_team_k7m_aux_d.basis_client bb 
    ) X
GROUP BY INN
HAVING 
    SUM(SGN)<>0
--2.Внимание!Требуется согласованное состояние clu_keys и lk/lkc
SELECT 
    u7m_id,
    SUM(SGN) SGN
FROM (
    SELECT u7m_id, -1 SGN
    FROM t_team_k7m_aux_p.clu_base
    UNION ALL 
    SELECT 
        k.u7m_id,
        1 SGN
    FROM t_team_k7m_aux_p.clu_extended_basis bb 
        join  t_team_k7m_aux_p.clu_keys k
        on k.u7m_id = bb.u7m_id
    where k.status = 'ACTIVE'        
    ) X
GROUP BY u7m_id
HAVING 
    SUM(SGN)<>0  
    



--Сверка с прототипом
SELECT
    U7M_ID,
    SUM(EKS_ID)
FROM (
select U7M_ID, EKS_ID 
from t_team_k7m_aux_p.clu_base
UNION ALL
select U7M_ID, -EKS_ID EKS_ID
from t_team_k7m_aux_d.clu_base
) X
GROUP BY U7M_ID
HAVING SUM(EKS_ID)<>0
LIMIT 100

--Общий отчет
SELECT 
    flag_basis_client,
    count(*) cnt,
    count(eks_id) cnt_eks_id
FROM  t_team_k7m_aux_p.clu_base   X
GROUP BY flag_basis_client


