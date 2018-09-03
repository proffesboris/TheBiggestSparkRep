--Поиск неклиентов поручителей

--ВНИМАНИЮ РАЗРАБОТЧИКА
--Константа DATE'2018-02-01' - это параметр Дата расчета K7M!

--Ниже в SQL вводятся 2 параметра
-- 1 PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
-- 2 PARAM "Попадание в топ по оборотам"
-- 3 PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал  
--Необходимо завести их в  таблице (Борис Медведев создает)
        create table aux.ETL_TASK_PARAM
        (
             Proc_cd      string,
             param_name string,
             param_val string,
             param_desc string)
--заполняем ее так:           
        --1)PARAM 
            -- Proc_cd       = SET
            -- param_name    = TURNOVER_PART 
            --param_val =      0.025
            --param_desc = "Доля оборотов потенц. поручителя в общих оборотах контрагента"
        --2)PARAM 
            -- Proc_cd       = SET
            -- param_name    = TURNOVER_TOP 
            --param_val =      10
            --param_desc = "Попадание в топ по оборотам"
        --3)PARAM 
            -- Proc_cd       = SET
            -- param_name    = TURN_OFF_Q_STABILITY
            -- param_val     = 1
            -- param_desc = "1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал"            
---Далее в коде заменить хардкод на использование этих параметров, обратите внимание, что параметры имеют тип string и их надо преобразовывать к double или int для сравнения!!  




drop table  t_team_k7m_aux_p.sep_link_crit_dual

--Подготовка связей 1
create table t_team_k7m_aux_p.sep_link_crit_dual
stored as parquet
as
 select
    bor_obj,
    bor_u7m_id,
    rel_obj,
    rel_u7m_id,    
    max(ben_flag) ben_flag
 from (    
    select 
        lk.t_from bor_obj,
        lk.u7m_id_from bor_u7m_id,
        lk.t_to rel_obj,
        lk.u7m_id_to rel_u7m_id,
        case when lkc.crit_id= '5.1.7ben' then 1 else 0 end as ben_flag
    from 
        t_team_k7m_stg.rdm_link_criteria_mast as c
        join 
        t_team_k7m_pa_p.lkc as lkc
        on c.code = lkc.crit_id
        join t_team_k7m_pa_p.lk as lk
        on lk.lk_id = lkc.lk_id
    where c.set_flag = 1
        and lk.u7m_id_from is not null
        and lk.u7m_id_to is not null
    union all
    select 
        lk.t_to bor_obj,
        lk.u7m_id_to bor_u7m_id,
        lk.t_from rel_obj,
        lk.u7m_id_from rel_u7m_id,
        case when lkc.crit_id= '5.1.7ben' then 1 else 0 end as ben_flag
    from 
        t_team_k7m_stg.rdm_link_criteria_mast as c
        join 
        t_team_k7m_pa_p.lkc as lkc
        on c.code = lkc.crit_id
        join t_team_k7m_pa_p.lk as lk
        on lk.lk_id = lkc.lk_id
    where c.set_flag = 1
        and lk.u7m_id_from is not null
        and lk.u7m_id_from is not null    
    ) x
group by 
    bor_obj,
    bor_u7m_id,
    rel_obj,
    rel_u7m_id
    
--Подготовка связей 2
drop table t_team_k7m_aux_p.sep_bc_link_crit_dual
create table t_team_k7m_aux_p.sep_bc_link_crit_dual
stored as parquet
as
select 
    d.bor_obj,
    d.bor_u7m_id, 
    b.inn bor_inn,
    d.rel_obj,
    d.rel_u7m_id,
    r.inn rel_inn,
    coalesce(d.ben_flag,0) as ben_flag  
from  t_team_k7m_pa_d.clu b
    join t_team_k7m_aux_p.sep_link_crit_dual d
    on b.u7m_id = d.bor_u7m_id
    join t_team_k7m_pa_d.clu r
    on r.u7m_id = d.rel_u7m_id
where b.flag_basis_client = 'Y'
    and d.bor_obj = 'CLU'
    and d.rel_obj = 'CLU'
--Пока связи устанавливаем только с Юрлицами (вопрос Сергею Шешкусу)

create table t_team_k7m_aux_p.sep_cnt_neighbours
stored as parquet
as
select 
    bor_obj,
    bor_u7m_id,
    count(*) cnt_neighbours
from t_team_k7m_aux_p.sep_bc_link_crit_dual
group by  
    bor_obj,
    bor_u7m_id
     

--Подготовка связей по транзакциям

drop table t_team_k7m_AUX_P.SET_Prov_base
create table t_team_k7m_AUX_P.SET_Prov_base
stored as parquet
as
select 
tr.c_date_prov,
inn_st,inn_sec,ktdt,
tr.c_sum
from 
 t_team_k7m_stg.z_main_kras_new  tr 
where   inn_st<>inn_sec
    and tr.ypred in ('оплата по договору')
    and tr.c_sum<>0
    and to_date(tr.c_date_prov)<=DATE'2018-02-01'--!!!!! Дата расчета K7M!



drop table t_team_k7m_aux_p.sep_surety_7M_r4_basis_client

create table t_team_k7m_aux_p.sep_surety_7M_r4_basis_client
stored as parquet
as
select b.u7m_id as bor_u7m_id, tr.inn_sec as rel_inn,
    coalesce(sum(
                  case when months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<=12 --!!!!! Дата расчета K7M!
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr           

    ,coalesce(sum(
                  case when months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 3
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_1q
    ,coalesce(sum(
                  case when  months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 6
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 3
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_2q

    ,coalesce(sum(
                  case when  months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 9
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 6
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_3q
                  
    ,coalesce(sum(
                  case when  months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 12
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 9
                          and ktdt=0
                        then tr.c_sum else 0 end
                  ),0) as oboroty_kr_4q

    ,coalesce(sum(
                  case when months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<=12
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db   
                  
                
       ,coalesce(sum(
                  case when  months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<=3
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_1q              
       ,coalesce(sum(
                  case when   months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 6
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 3
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_2q     
           ,coalesce(sum(
                  case when months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 9
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 6
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_3q               
             ,coalesce(sum(
                  case when  months_between(DATE'2018-02-01',to_date(tr.c_date_prov))<= 12
                          and months_between(DATE'2018-02-01',to_date(tr.c_date_prov))> 9
                          and ktdt=1
                        then tr.c_sum else 0 end
                  ),0) as oboroty_db_4q                

    ,coalesce(max(
                  case when ktdt=1
                        then months_between(DATE'2018-02-01',to_date(tr.c_date_prov)) end
                  ),0) as oboroty_db_last_tr_m   

    ,coalesce(max(
                  case when  ktdt=0
                        then months_between(DATE'2018-02-01',to_date(tr.c_date_prov)) end
                  ),0) as oboroty_kr_last_tr_m  

    ,coalesce(max(months_between(DATE'2018-02-01',to_date(tr.c_date_prov))),0) as oboroty_last_tr_m  
from t_team_k7m_pa_d.clu as b
    left join  t_team_k7m_AUX_P.SET_Prov_base tr 
    on tr.inn_st = b.inn
where b.flag_basis_client = 'Y'
group by b.u7m_id, tr.inn_sec

  
--Общие суммы по заемщику
drop table t_team_k7m_aux_p.sep_basis_all_sum_tr
create table t_team_k7m_aux_p.sep_basis_all_sum_tr
stored as parquet
as
select bor_u7m_id,
    sum(oboroty_kr) as total_oboroty_kr,
    sum(oboroty_kr_1q) as total_oboroty_kr_1q,
    sum(oboroty_kr_2q) as total_oboroty_kr_2q,
    sum(oboroty_kr_3q) as total_oboroty_kr_3q,
    sum(oboroty_kr_4q) as total_oboroty_kr_4q,
    sum(oboroty_db) as total_oboroty_db,
    sum(oboroty_db_1q) as total_oboroty_db_1q,
    sum(oboroty_db_2q) as total_oboroty_db_2q,
    sum(oboroty_db_3q) as total_oboroty_db_3q,
    sum(oboroty_db_4q) as total_oboroty_db_4q,
    max(oboroty_kr_last_tr_m) as max_oboroty_kr_last_tr_m,
    max(oboroty_db_last_tr_m) as max_oboroty_db_last_tr_m,
    max(oboroty_last_tr_m) as max_oboroty_last_tr_m
from t_team_k7m_aux_p.sep_surety_7M_r4_basis_client
group by bor_u7m_id
    

--Добавим обороты с контрагентами из группы
drop table t_team_k7m_aux_p.sep_surety_7M_r4_gr_ben
create table t_team_k7m_aux_p.sep_surety_7M_r4_gr_ben
stored as parquet
as
select 
    c.bor_u7m_id as u7m_id,
    c.rel_inn,
    A.rel_inn a_rel_inn,
    a.rel_u7m_id,
    oboroty_kr,
    oboroty_kr_1q,
    oboroty_kr_2q,
    oboroty_kr_3q,
    oboroty_kr_4q,
    oboroty_db,
    oboroty_db_1q,
    oboroty_db_2q,
    oboroty_db_3q,
    oboroty_db_4q,
    case when A.rel_inn is not NULL then 1 else 0 end as rel_flag,
    coalesce(A.ben_flag,0) as ben_flag,
    d.total_oboroty_kr,
    d.total_oboroty_kr_1q,
    d.total_oboroty_kr_2q,
    d.total_oboroty_kr_3q,
    d.total_oboroty_kr_4q,
    d.total_oboroty_db,
    d.total_oboroty_db_1q,
    d.total_oboroty_db_2q,
    d.total_oboroty_db_3q,
    d.total_oboroty_db_4q,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr    end as oboroty_kr_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_1q end as oboroty_kr_1q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_2q end as oboroty_kr_2q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_3q end as oboroty_kr_3q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_4q end as oboroty_kr_4q_gr,
    case when A.ben_flag = 1 then c.oboroty_kr else 0 end as oboroty_kr_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_1q else 0 end as oboroty_kr_1q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_2q else 0 end as oboroty_kr_2q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_3q else 0 end as oboroty_kr_3q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_4q else 0 end as oboroty_kr_4q_ben,
    case when A.rel_inn is NULL then 0 else c.oboroty_db    end as oboroty_db_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_1q end as oboroty_db_1q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_2q end as oboroty_db_2q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_3q end as oboroty_db_3q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_4q end as oboroty_db_4q_gr,
    case when A.ben_flag = 1 then c.oboroty_db else 0 end as oboroty_db_ben,
    case when A.ben_flag = 1 then c.oboroty_db_1q else 0 end as oboroty_db_1q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_2q else 0 end as oboroty_db_2q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_3q else 0 end as oboroty_db_3q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_4q else 0 end as oboroty_db_4q_ben,
    d.max_oboroty_kr_last_tr_m,
    d.max_oboroty_db_last_tr_m,
    d.max_oboroty_last_tr_m
 from
 t_team_k7m_aux_p.sep_surety_7M_r4_basis_client as C       
    inner join t_team_k7m_aux_p.sep_basis_all_sum_tr as D 
        on d.bor_u7m_id = c.bor_u7m_id
    left join t_team_k7m_aux_p.sep_bc_link_crit_dual as A 
        on 
            A.bor_u7m_id=C.bor_u7m_id 
            and A.rel_inn = C.rel_inn

  --Проставим порядковые номер для оборотов (чтобы потом отбирать по разным правилам клиентов из топ 10)
  --Добавим обороты с контрагентами из группы
  
 
drop table t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord
create table t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord
stored as parquet
as
select 
    a.u7m_id,
    a.rel_inn,
    a.rel_u7m_id,
    a.rel_flag, 
    a.ben_flag,
    a.oboroty_kr   ,a.oboroty_kr_1q   ,a.oboroty_kr_2q   ,a.oboroty_kr_3q   ,a.oboroty_kr_4q,
    a.oboroty_kr_gr,a.oboroty_kr_1q_gr,a.oboroty_kr_2q_gr,a.oboroty_kr_3q_gr,a.oboroty_kr_4q_gr,
    a.oboroty_kr_ben,a.oboroty_kr_1q_ben,a.oboroty_kr_2q_ben,a.oboroty_kr_3q_ben,a.oboroty_kr_4q_ben
    ,a.oboroty_kr_gr   /a.total_oboroty_kr    as oboroty_kr_gr_rate
    ,a.oboroty_kr_1q_gr/a.total_oboroty_kr_1q as oboroty_kr_1q_gr_rate
    ,a.oboroty_kr_2q_gr/a.total_oboroty_kr_2q as oboroty_kr_2q_gr_rate
    ,a.oboroty_kr_3q_gr/a.total_oboroty_kr_3q as oboroty_kr_3q_gr_rate
    ,a.oboroty_kr_4q_gr/a.total_oboroty_kr_4q as oboroty_kr_4q_gr_rate
    ,a.oboroty_kr_ben   /a.total_oboroty_kr    as oboroty_kr_ben_rate
    ,a.oboroty_kr_1q_ben/a.total_oboroty_kr_1q as oboroty_kr_1q_ben_rate
    ,a.oboroty_kr_2q_ben/a.total_oboroty_kr_2q as oboroty_kr_2q_ben_rate
    ,a.oboroty_kr_3q_ben/a.total_oboroty_kr_3q as oboroty_kr_3q_ben_rate
    ,a.oboroty_kr_4q_ben/a.total_oboroty_kr_4q as oboroty_kr_4q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr desc)     as rn_oboroty_kr,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q desc)  as rn_oboroty_kr_1q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q desc)  as rn_oboroty_kr_2q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q desc)  as rn_oboroty_kr_3q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q desc)  as rn_oboroty_kr_4q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_gr/a.total_oboroty_kr       desc) as rn_oboroty_kr_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q_gr/a.total_oboroty_kr_1q desc) as rn_oboroty_kr_1q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q_gr/a.total_oboroty_kr_2q desc) as rn_oboroty_kr_2q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q_gr/a.total_oboroty_kr_3q desc) as rn_oboroty_kr_3q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q_gr/a.total_oboroty_kr_4q desc) as rn_oboroty_kr_4q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_ben/a.total_oboroty_kr       desc) as rn_oboroty_kr_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q_ben/a.total_oboroty_kr_1q desc) as rn_oboroty_kr_1q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q_ben/a.total_oboroty_kr_2q desc) as rn_oboroty_kr_2q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q_ben/a.total_oboroty_kr_3q desc) as rn_oboroty_kr_3q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q_ben/a.total_oboroty_kr_4q desc) as rn_oboroty_kr_4q_ben_rate,
    a.oboroty_db   ,a.oboroty_db_1q   ,a.oboroty_db_2q   ,a.oboroty_db_3q   ,a.oboroty_db_4q,
    a.oboroty_db_gr,a.oboroty_db_1q_gr,a.oboroty_db_2q_gr,a.oboroty_db_3q_gr,a.oboroty_db_4q_gr,
    a.oboroty_db_ben,a.oboroty_db_1q_ben,a.oboroty_db_2q_ben,a.oboroty_db_3q_ben,a.oboroty_db_4q_ben,
    a.oboroty_db_gr   /a.total_oboroty_db    as oboroty_db_gr_rate,
    a.oboroty_db_1q_gr/a.total_oboroty_db_1q as oboroty_db_1q_gr_rate,
    a.oboroty_db_2q_gr/a.total_oboroty_db_2q as oboroty_db_2q_gr_rate,
    a.oboroty_db_3q_gr/a.total_oboroty_db_3q as oboroty_db_3q_gr_rate,
    a.oboroty_db_4q_gr/a.total_oboroty_db_4q as oboroty_db_4q_gr_rate,
    a.oboroty_db_ben   /a.total_oboroty_db    as oboroty_db_ben_rate,
    a.oboroty_db_1q_ben/a.total_oboroty_db_1q as oboroty_db_1q_ben_rate,
    a.oboroty_db_2q_ben/a.total_oboroty_db_2q as oboroty_db_2q_ben_rate,
    a.oboroty_db_3q_ben/a.total_oboroty_db_3q as oboroty_db_3q_ben_rate,
    a.oboroty_db_4q_ben/a.total_oboroty_db_4q as oboroty_db_4q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db desc)     as rn_oboroty_db,
    row_number() over (partition by  a.u7m_id order by a.oboroty_db_1q desc)  as rn_oboroty_db_1q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q desc)  as rn_oboroty_db_2q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q desc)  as rn_oboroty_db_3q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q desc)  as rn_oboroty_db_4q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_gr/a.total_oboroty_db desc) as rn_oboroty_db_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_1q_gr/a.total_oboroty_db_1q desc) as rn_oboroty_db_1q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q_gr/a.total_oboroty_db_2q desc) as rn_oboroty_db_2q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q_gr/a.total_oboroty_db_3q desc) as rn_oboroty_db_3q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q_gr/a.total_oboroty_db_4q desc) as rn_oboroty_db_4q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_ben/a.total_oboroty_db desc) as rn_oboroty_db_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_1q_ben/a.total_oboroty_db_1q desc) as rn_oboroty_db_1q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q_ben/a.total_oboroty_db_2q desc) as rn_oboroty_db_2q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q_ben/a.total_oboroty_db_3q desc) as rn_oboroty_db_3q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q_ben/a.total_oboroty_db_4q desc) as rn_oboroty_db_4q_ben_rate,
    max_oboroty_kr_last_tr_m,
    max_oboroty_db_last_tr_m,
    max_oboroty_last_tr_m
from t_team_k7m_aux_p.sep_surety_7M_r4_gr_ben a

drop table t_team_k7m_aux_p.sep_surety_7M_r4_t2
create table t_team_k7m_aux_p.sep_surety_7M_r4_t2
stored as parquet
as
select a.u7m_id, 
 kr.oboroty_kr,kr.oboroty_kr_1q   
,kr.oboroty_kr_2q   
,kr.oboroty_kr_3q   
,kr.oboroty_kr_4q
,kr_ben.oboroty_kr as kr_ben_oboroty_kr
,kr_ben.oboroty_kr_1q as kr_ben_oboroty_kr_1q
,kr_ben.oboroty_kr_2q as kr_ben_oboroty_kr_2q
,kr_ben.oboroty_kr_3q as kr_ben_oboroty_kr_3q
,kr_ben.oboroty_kr_4q as kr_ben_oboroty_kr_4q
,kr.oboroty_kr_gr
,kr.oboroty_kr_1q_gr
,kr.oboroty_kr_2q_gr
,kr.oboroty_kr_3q_gr
,kr.oboroty_kr_4q_gr
,kr_ben.oboroty_kr_ben
,kr_ben.oboroty_kr_1q_ben
,kr_ben.oboroty_kr_2q_ben
,kr_ben.oboroty_kr_3q_ben
,kr_ben.oboroty_kr_4q_ben
,kr.oboroty_kr_gr_rate
,kr.oboroty_kr_1q_gr_rate
,kr.oboroty_kr_2q_gr_rate
,kr.oboroty_kr_3q_gr_rate
,kr.oboroty_kr_4q_gr_rate
,kr_ben.oboroty_kr_ben_rate
,kr_ben.oboroty_kr_1q_ben_rate
,kr_ben.oboroty_kr_2q_ben_rate
,kr_ben.oboroty_kr_3q_ben_rate
,kr_ben.oboroty_kr_4q_ben_rate
,kr.rn_oboroty_kr   
,kr.rn_oboroty_kr_1q 
,kr.rn_oboroty_kr_2q
,kr.rn_oboroty_kr_3q
,kr.rn_oboroty_kr_4q    
,kr.rn_oboroty_kr_gr_rate
,kr.rn_oboroty_kr_1q_gr_rate
,kr.rn_oboroty_kr_2q_gr_rate
,kr.rn_oboroty_kr_3q_gr_rate
,kr.rn_oboroty_kr_4q_gr_rate
,kr_ben.rn_oboroty_kr_ben_rate
,kr_ben.rn_oboroty_kr_1q_ben_rate
,kr_ben.rn_oboroty_kr_2q_ben_rate
,kr_ben.rn_oboroty_kr_3q_ben_rate
,kr_ben.rn_oboroty_kr_4q_ben_rate
,db.oboroty_db   
,db.oboroty_db_1q   
,db.oboroty_db_2q   
,db.oboroty_db_3q   
,db.oboroty_db_4q
,db_ben.oboroty_db as db_ben_oboroty_db
,db_ben.oboroty_db_1q as db_ben_oboroty_db_1q
,db_ben.oboroty_db_2q as db_ben_oboroty_db_2q
,db_ben.oboroty_db_3q as db_ben_oboroty_db_3q
,db_ben.oboroty_db_4q as db_ben_oboroty_db_4q
,db.oboroty_db_gr
,db.oboroty_db_1q_gr
,db.oboroty_db_2q_gr
,db.oboroty_db_3q_gr
,db.oboroty_db_4q_gr
,db_ben.oboroty_db_ben
,db_ben.oboroty_db_1q_ben
,db_ben.oboroty_db_2q_ben
,db_ben.oboroty_db_3q_ben
,db_ben.oboroty_db_4q_ben
,db.oboroty_db_gr_rate
,db.oboroty_db_1q_gr_rate
,db.oboroty_db_2q_gr_rate
,db.oboroty_db_3q_gr_rate
,db.oboroty_db_4q_gr_rate
,db_ben.oboroty_db_ben_rate
,db_ben.oboroty_db_1q_ben_rate
,db_ben.oboroty_db_2q_ben_rate
,db_ben.oboroty_db_3q_ben_rate
,db_ben.oboroty_db_4q_ben_rate
,db.rn_oboroty_db   
,db.rn_oboroty_db_1q 
,db.rn_oboroty_db_2q
,db.rn_oboroty_db_3q
,db.rn_oboroty_db_4q    
,db.rn_oboroty_db_gr_rate
,db.rn_oboroty_db_1q_gr_rate
,db.rn_oboroty_db_2q_gr_rate
,db.rn_oboroty_db_3q_gr_rate
,db.rn_oboroty_db_4q_gr_rate 
,db_ben.rn_oboroty_db_ben_rate
,db_ben.rn_oboroty_db_1q_ben_rate
,db_ben.rn_oboroty_db_2q_ben_rate
,db_ben.rn_oboroty_db_3q_ben_rate
,db_ben.rn_oboroty_db_4q_ben_rate,
case when kr.oboroty_kr_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента" 
           and kr.rn_oboroty_kr <= 10 -- PARAM "Попадание в топ по оборотам"
     then kr.rel_inn end as surety_inn_kr_gr   
    ,case when kr.oboroty_kr_1q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента" 
           and kr.rn_oboroty_kr_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_1q_flag   
    ,case when kr.oboroty_kr_2q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_2q_flag  
    ,case when kr.oboroty_kr_3q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_3q_flag    
    ,case when kr.oboroty_kr_4q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_4q_flag     
    ,case when kr_ben.oboroty_kr_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr <= 10 -- PARAM "Попадание в топ по оборотам"
     then kr_ben.rel_inn end as surety_inn_kr_ben   
    ,case when kr_ben.oboroty_kr_1q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_1q_flag   
    ,case when kr_ben.oboroty_kr_2q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_2q <= 10  -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_2q_flag   
    ,case when kr_ben.oboroty_kr_3q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_3q_flag     
    ,case when kr_ben.oboroty_kr_4q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_4q_flag  
    ,case when db.oboroty_db_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db <= 10 -- PARAM "Попадание в топ по оборотам"
     then db.rel_inn end as surety_inn_db_gr   
    ,case when db.oboroty_db_1q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_1q_flag   
    ,case when db.oboroty_db_2q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_2q_flag  
    ,case when db.oboroty_db_3q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_3q_flag    
    ,case when db.oboroty_db_4q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_4q_flag,
     case when db_ben.oboroty_db_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db <= 10 -- PARAM "Попадание в топ по оборотам"
     then db_ben.rel_inn end as surety_inn_db_ben,
     case when db_ben.oboroty_db_1q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_1q_flag,
     case when db_ben.oboroty_db_2q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_2q_flag,
     case when db_ben.oboroty_db_3q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_3q_flag    
    ,case when db_ben.oboroty_db_4q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_4q_flag,
     kr.max_oboroty_kr_last_tr_m,
     db.max_oboroty_db_last_tr_m,
     kr.max_oboroty_last_tr_m,
     kr_ben.max_oboroty_kr_last_tr_m as max_oboroty_kr_ben_last_tr_m,
     db_ben.max_oboroty_db_last_tr_m as max_oboroty_db_ben_last_tr_m,
     kr_ben.max_oboroty_last_tr_m as max_oboroty_ben_last_tr_m
from t_team_k7m_pa_d.clu as a
-- Вместо "t_corp_risks.surety_7M_r4_oboroty_ord"
-- ниже необходимо использовать таблицу "surety_7M_r4_oboroty_ord_cut_br_kr" без услвоия nd "kr.rn_oboroty_kr_gr_rate = 1"
-- аналогично с kr_ben
    left join t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord as kr     
    on 
        A.u7m_id=kr.u7m_id 
        and kr.rn_oboroty_kr_gr_rate = 1  
    left join t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord as kr_ben 
    on 
        A.u7m_id=kr_ben.u7m_id 
        and kr_ben.rn_oboroty_kr_ben_rate = 1
-- Вместо "t_corp_risks.surety_7M_r4_oboroty_ord"
-- ниже необходимо использовать таблицу "surety_7M_r4_oboroty_ord_cut_br_db" без услвоия nd "kr.rn_oboroty_db_gr_rate = 1"
-- аналогично с db_ben
    left join t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord as db 
    on 
        A.u7m_id=db.u7m_id 
        and db.rn_oboroty_db_gr_rate = 1
    left join t_team_k7m_aux_p.sep_surety_7M_r4_oboroty_ord as db_ben 
    on 
        A.u7m_id=db_ben.u7m_id 
        and db_ben.rn_oboroty_db_ben_rate = 1
where a.flag_basis_client = 'Y'   



create table t_team_k7m_aux_p.sep_surety_7M_for_non_client_gr_ben
stored as parquet
as
select 
    b.u7m_id,
    kr_gr.surety_inn_kr_gr,
    kr_ben.surety_inn_kr_ben,
    db_gr.surety_inn_db_gr,
    db_ben.surety_inn_db_ben
from t_team_k7m_pa_d.clu as b
left join (
            select u7m_id, surety_inn_kr_gr from t_team_k7m_aux_p.sep_surety_7M_r4_t2
            where surety_inn_kr_gr is not null 
             (
                1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал  
                (kr_gr_1q_flag = 1 
                and (
                    (max_oboroty_last_tr_m > 9 and (kr_gr_1q_flag+kr_gr_2q_flag+kr_gr_3q_flag+kr_gr_4q_flag)>=3)
                        or
                    (max_oboroty_last_tr_m > 6 and max_oboroty_last_tr_m <= 9  and (kr_gr_1q_flag+kr_gr_2q_flag+kr_gr_3q_flag) = 3)
                        or
                    (max_oboroty_last_tr_m > 3 and max_oboroty_last_tr_m <= 6  and (kr_gr_1q_flag+kr_gr_2q_flag) = 2)
                        or
                    (max_oboroty_last_tr_m > 0 and max_oboroty_last_tr_m <= 3  and kr_gr_1q_flag = 1)
                    ))
                  )  
                    
) as kr_gr on kr_gr.u7m_id = b.u7m_id

left join 
    (
            select u7m_id, surety_inn_kr_ben 
            from t_team_k7m_aux_p.sep_surety_7M_r4_t2
            where surety_inn_kr_ben is not null 
              and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (
                and kr_ben_1q_flag = 1 
                and (
                    (max_oboroty_ben_last_tr_m > 9 and (kr_ben_1q_flag+kr_ben_2q_flag+kr_ben_3q_flag+kr_ben_4q_flag)>=3)
                        or
                    (max_oboroty_ben_last_tr_m > 6 and max_oboroty_ben_last_tr_m <= 9  and (kr_ben_1q_flag+kr_ben_2q_flag+kr_ben_3q_flag) = 3)
                        or
                    (max_oboroty_ben_last_tr_m > 3 and max_oboroty_ben_last_tr_m <= 6  and (kr_ben_1q_flag+kr_ben_2q_flag) = 2)
                        or
                    (max_oboroty_ben_last_tr_m > 0 and max_oboroty_ben_last_tr_m <= 3  and kr_ben_1q_flag = 1)
                    )
                 )
                 )   

    ) as kr_ben 
    on kr_ben.u7m_id = b.u7m_id
left join (
            select u7m_id, surety_inn_db_gr 
            from t_team_k7m_aux_p.sep_surety_7M_r4_t2
            where surety_inn_db_gr is not null
            and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (                          
                 db_gr_1q_flag = 1 
                and (
                    (max_oboroty_last_tr_m > 9 and (db_gr_1q_flag+db_gr_2q_flag+db_gr_3q_flag+db_gr_4q_flag)>=3)
                        or
                    (max_oboroty_last_tr_m > 6 and max_oboroty_last_tr_m <= 9  and (db_gr_1q_flag+db_gr_2q_flag+db_gr_3q_flag) = 3)
                        or
                    (max_oboroty_last_tr_m > 3 and max_oboroty_last_tr_m <= 6  and (db_gr_1q_flag+db_gr_2q_flag) = 2)
                        or
                    (max_oboroty_last_tr_m > 0 and max_oboroty_last_tr_m <= 3  and db_gr_1q_flag = 1)
                    )
                   )) 
                    
) as db_gr on db_gr.u7m_id = b.u7m_id

left join (
            select u7m_id, surety_inn_db_ben 
            from t_team_k7m_aux_p.sep_surety_7M_r4_t2
            where surety_inn_db_ben is not null
            and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (                 
             
                db_ben_1q_flag = 1 
                and (
                    (max_oboroty_ben_last_tr_m > 9 and (db_ben_1q_flag+db_ben_2q_flag+db_ben_3q_flag+db_ben_4q_flag)>=3)
                        or
                    (max_oboroty_ben_last_tr_m > 6 and max_oboroty_ben_last_tr_m <= 9  and (db_ben_1q_flag+db_ben_2q_flag+db_ben_3q_flag) = 3)
                        or
                    (max_oboroty_ben_last_tr_m > 3 and max_oboroty_ben_last_tr_m <= 6  and (db_ben_1q_flag+db_ben_2q_flag) = 2)
                        or
                    (max_oboroty_ben_last_tr_m > 0 and max_oboroty_ben_last_tr_m <= 3  and db_ben_1q_flag = 1)
                    )
                   ))   

) as db_ben 
on db_ben.u7m_id = b.u7m_id
where b.flag_basis_client = 'Y'   

drop table t_team_k7m_aux_p.sep_surety_7M_for_non_client_sur_init

create table t_team_k7m_aux_p.sep_surety_7M_for_non_client_sur_init --=surety_7M_for_non_client_final !
stored as parquet
as
select distinct u7m_id, sur_inn, ben_flag from (
    select u7m_id
          ,coalesce(surety_inn_kr_ben,surety_inn_kr_gr) as sur_inn
          ,case when surety_inn_kr_ben is not null then 1 else 0 end as ben_flag
    from t_team_k7m_aux_p.sep_surety_7M_for_non_client_gr_ben
    union all
    select u7m_id,
        coalesce(surety_inn_db_ben,surety_inn_db_gr) as sur_inn,
        case when surety_inn_db_ben is not null then 1 else 0 end as ben_flag
    from t_team_k7m_aux_p.sep_surety_7M_for_non_client_gr_ben
) x
where sur_inn is not null


