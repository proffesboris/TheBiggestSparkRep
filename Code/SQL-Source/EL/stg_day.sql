--*****Промежуточные сущности на дату ****-------
--nohup hive -v -hiveconf DT1=2018-02-25 -f stg_day.sql >result_day20180225.txt 
set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

	
--Связь счет-кред.договор	
drop table t_team_k7m_aux_p.k7m_cred_to_acc;
create table t_team_k7m_aux_p.k7m_cred_to_acc stored as parquet as    
select 
    coalesce(c.c_high_level_cr,c.id) cred_id,
    ac.id acc_id,
    max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
from  
    t_team_k7m_aux_p.k7m_Z_hoz_Op_Acc oc
    join  t_team_k7m_aux_p.k7m_z_ac_fin ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2    
    join t_team_k7m_aux_p.k7m_Z_product p
    on oc.collection_id = p.C_ARRAY_DOG_ACC        
    join   t_team_k7m_aux_p.k7m_z_all_cred   c
    on c.id = p.id
where  oc.C_NAME_ACCOUNT in (
            2049610,
            2049634,
            213649287,
            1189819940,
            5674430852596,
            114471177439,
            114471175621,
            114471175623,
            1189819943,
            1189819973,
            2049586,
            2049588)
group by 
    coalesce(c.c_high_level_cr,c.id),
    ac.id ;
					
					
--Остатки по кредиту (ОД+НЛ)					
drop table t_team_k7m_aux_p.k7m_cred_bal;
create table t_team_k7m_aux_p.k7m_cred_bal stored as parquet as    
 select 
        p.id cred_id,
        ac.id acc_id,
        r.C_BALANCE_LCL,
        r.C_BALANCE,
        max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
 from 
    t_team_k7m_aux_p.k7m_Z_hoz_Op_Acc oc
    join  t_team_k7m_aux_p.k7m_z_ac_fin ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2    
    join t_team_k7m_aux_p.k7m_acc_rest r
    on ac.c_arc_move = r.c_arc_move
    join t_team_k7m_aux_p.k7m_Z_product p
    on oc.collection_id = p.C_ARRAY_DOG_ACC    
    where 
      r.oper_dt = date'${hiveconf:DT1}'
     and   oc.C_NAME_ACCOUNT in (
            2049610,
            2049634,
            213649287,
            1189819940,
            5674430852596,
            114471177439,
            114471175621,
            114471175623,
            1189819943,
            1189819973,
            2049586,
            2049588)
        and r.C_BALANCE_LCL<>0			
group by p.id,
        ac.id,
        r.C_BALANCE_LCL,
        r.C_BALANCE;
-- Кредит верхнего уровня
insert into t_team_k7m_aux_p.k7m_cred_bal
select
        crh.c_high_level_cr cred_id,
        ac.id acc_id,
        r.C_BALANCE_LCL,
        r.C_BALANCE,
        max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
 from 
    t_team_k7m_aux_p.k7m_Z_hoz_Op_Acc oc
    join  t_team_k7m_aux_p.k7m_z_ac_fin ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2    
    join t_team_k7m_aux_p.k7m_acc_rest r
    on ac.c_arc_move = r.c_arc_move
    join t_team_k7m_aux_p.k7m_Z_product p
    on oc.collection_id = p.C_ARRAY_DOG_ACC
    join t_team_k7m_aux_p.k7m_z_all_cred crh
    on p.id = crh.id
    where r.oper_dt = date'${hiveconf:DT1}'
     and oc.C_NAME_ACCOUNT in (
            '2049610',
            '2049634',
            '213649287',
            '1189819940',
            '5674430852596',
            '114471177439',
            '114471175621',
            '114471175623',
            '1189819943',
            '1189819973',
            '2049586',
            '2049588')
        and r.C_BALANCE_LCL<>0
    group by crh.c_high_level_cr,
        ac.id,
        r.C_BALANCE_LCL,
        r.C_BALANCE;



--core_internal_eks.z_tip_acc where c_cod in ('ACC_DEBTS_CR_PREV','ACC_DEBTS_CR','ACCOUNT_PREV','ACCOUNT','VNB_ACCOUNT','VNB_UNUSED_LINE_PREV','VNB_UNUSED_LINE')
drop table t_team_k7m_aux_p.k7m_cred_bal_total;
create table t_team_k7m_aux_p.k7m_cred_bal_total stored as parquet as    
select 
    cred_id,
    sum(abs(C_BALANCE_LCL)) C_BALANCE_LCL    ,
    sum(abs(C_BALANCE)) C_BALANCE,
    sum(abs(OD_LCL)) OD_LCL    ,
    sum(abs(OD)) OD,
    sum(abs(LIM_LCL)) LIM_LCL    ,
    sum(abs(LIM)) LIM
from  (
    select 
        cred_id,
        acc_id,
        max(C_BALANCE_LCL) C_BALANCE_LCL,
        max(C_BALANCE) C_BALANCE,
        max(
            case 
                when C_NAME_ACCOUNT ='2049586' then C_BALANCE_LCL
                else cast(0 as decimal(27,2))
                end
        ) OD_LCL,
        max(
            case 
                when C_NAME_ACCOUNT ='2049586' then C_BALANCE
                else cast(0 as decimal(27,2))
                end
        ) OD,
        max(
            case 
                when C_NAME_ACCOUNT ='2049588' then C_BALANCE_LCL
                else cast(0 as decimal(27,2))
                end
        ) LIM_LCL,
        max(
            case 
                when C_NAME_ACCOUNT ='2049588' then C_BALANCE
                else cast(0 as decimal(27,2))
                end
        ) LIM
    from t_team_k7m_aux_p.k7m_cred_bal c 
    group by
        cred_id,
        acc_id
    ) x
group by cred_id;


-- 0a. Переключатель Рын-баланс:
drop table t_team_k7m_aux_p.k7m_zalog_switch;
create table t_team_k7m_aux_p.k7m_zalog_switch stored as parquet as
select 
	 p.collection_id,
	 max(p.c_bool) c_bool --1-баланс, иначе марк
from  
	 t_team_k7m_aux_p.k7m_z_property_grp pg
    INNER JOIN t_team_k7m_aux_p.k7m_z_properties  p
	ON  p.c_group_prop = pg.id
        AND pg.c_code = 'MARKET_VB_ACC'
where  unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp))*1000 BETWEEN p.C_DATE_BEG AND NVL( p.C_DATE_END, 100000000000000 )
group by 	p.collection_id
;		

--Сумма залога
drop table t_team_k7m_aux_p.k7m_z_zalog_sum;
create table t_team_k7m_aux_p.k7m_z_zalog_sum stored as parquet as
select 
    z.id,
    z.collection_id,
    z.c_acc_zalog,
    z.c_user_zalog,
    ac.c_main_v_id zalog_acc,
    z.C_PART_TO_LOAN,
    max(case when
        coalesce(s.c_bool,'x') = '1' then abs(r.C_BALANCE_LCL)
    else coalesce(ms.c_summ ,abs(r.C_BALANCE_LCL))
    end) zalog_sum,
    max(r.C_BALANCE_LCL) C_BALANCE_LCL ,
    max(ms.c_summ ) c_summ  ,
    max(s.c_bool) c_bool
--    z.c_market_sum,
    --c_properties    
from t_team_k7m_aux_p.k7m_z_zalog z
    left join t_team_k7m_aux_p.k7m_zalog_switch s
    on s.collection_id = z.c_properties    
        join  t_team_k7m_aux_p.k7m_z_ac_fin ac
    on ac.id = z.c_acc_zalog
    join t_team_k7m_aux_p.k7m_acc_rest r
    on ac.c_arc_move = r.c_arc_move
    left join t_team_k7m_aux_p.k7m_Z_ZALOG_MARKET_SUM ms
    on ms.collection_id =z.c_market_sum 
where (ms.collection_id is null
       or  unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp))*1000 BETWEEN ms.C_DATE_BEG AND coalesce( ms.C_DATE_END, 100000000000000 ))
       and r.c_balance_lcl<>0
group by 
    z.id,
    z.collection_id,
    z.c_acc_zalog,
    ac.c_main_v_id,
    z.c_user_zalog,
    z.C_PART_TO_LOAN
having max(case when
        coalesce(s.c_bool,'x') = '1' then abs(r.C_BALANCE_LCL)
    else coalesce(ms.c_summ ,abs(r.C_BALANCE_LCL))
    end) <>0
	;

--Залог - кредит
-- drop table t_team_k7m_aux_p.k7m_zalog_to_cred;
-- create table t_team_k7m_aux_p.k7m_zalog_to_cred
-- (
-- class_id string,
-- cred_id string,
-- zalog_id string,
-- cred_client_id string,
-- zalog_client_id string,
-- C_PART decimal(17,2),
-- C_PRC_ATTR string
-- )
-- stored as parquet;
 truncate table t_team_k7m_aux_p.k7m_zalog_to_cred;

-- --22.01.2018 - BIBS-1434, Комментарий от Пустынникова, что больше не используется старая схема
-- insert into t_team_k7m_aux_p.k7m_zalog_to_cred
-- select 
    -- c.class_id,
    -- c.id cred_id,
    -- z.id zalog_id,
    -- c.c_client cred_client_id,
    -- z.c_user_zalog zalog_client_id,
    -- 100 C_PART,
    -- '1' C_PRC_ATTR
-- from 
   -- t_team_k7m_aux_p.k7m_z_all_cred c
   -- join
   -- t_team_k7m_aux_p.k7m_z_zalog z
   -- on c.c_zalog =z.collection_id
-- group by c.class_id,c.id,z.id,c.c_client, z.c_user_zalog
-- ;

--новая схема
insert into t_team_k7m_aux_p.k7m_zalog_to_cred
select 
    DISTINCT 
    p.class_id,
    p.id cred_id,
    z.id zalog_id,
    p.c_client cred_client_id,
    z.c_user_zalog zalog_client_id,
     cast(cast( pz.C_PART as double) as decimal(17,2)) ,
    pz.C_PRC_ATTR
from 
   t_team_k7m_aux_p.k7m_Z_ZALOG z
   JOIN t_team_k7m_aux_p.k7m_PART_TO_LOAN pz 
   ON z.C_PART_TO_LOAN = pz.COLLECTION_ID 
   JOIN t_team_k7m_aux_p.k7m_z_all_cred p
   on pz.c_product = p.id;
   
---Явно указанное распределение
drop table t_team_k7m_aux_p.k7m_zalog_explisit_disp;
create table t_team_k7m_aux_p.k7m_zalog_explisit_disp stored as parquet as
select 
    zc.class_id,
    zc.cred_id,
    zc.zalog_id,
    zc.cred_client_id,
    zc.zalog_client_id,
    zs.zalog_acc,
    case when zc.C_PRC_ATTR = '1'
        and zc.c_part<=100 then 
            (zc.c_part/100)*zs.zalog_sum
        when zc.C_PRC_ATTR = '0'    then
            zc.c_part
        else null
        end zalog_disp_sum
from t_team_k7m_aux_p.k7m_zalog_to_cred zc
    join t_team_k7m_aux_p.k7m_z_zalog_sum zs
    on zc.zalog_id = zs.id;

--Найдем нераспределенную часть залога
drop table t_team_k7m_aux_p.k7m_zalog_undisp;
create table t_team_k7m_aux_p.k7m_zalog_undisp stored as parquet as
select 
    zalog_id,
    sum(zalog_sum) undisp_zalog_sum
from (
select id zalog_id,zalog_sum  from t_team_k7m_aux_p.k7m_z_zalog_sum
union all
select zalog_id,-zalog_disp_sum  zalog_sum from t_team_k7m_aux_p.k7m_zalog_explisit_disp
) x
group by zalog_id
having sum(zalog_sum)>0;
	
--Распределение методом алокации
drop table t_team_k7m_aux_p.k7m_zalog_implicit_disp;
create table t_team_k7m_aux_p.k7m_zalog_implicit_disp stored as parquet as
select 
 cred_id,
 zalog_id,
 undisp_zalog_sum,
 cred_sum,
 total_cred_sum,
 cast((undisp_zalog_sum*cred_sum)/total_cred_sum as decimal(17,2)) dispensed_sum
from ( 
select 
        d.cred_id,
        d.zalog_id,
        --z.undisp_zalog_sum,
        cast(c.C_BALANCE_LCL as double)  cred_sum,
        cast(sum(c.C_BALANCE_LCL) over ( partition by d.zalog_id) as double) total_cred_sum,
        cast(z.undisp_zalog_sum as double) undisp_zalog_sum
from t_team_k7m_aux_p.k7m_zalog_undisp z
    join t_team_k7m_aux_p.k7m_zalog_explisit_disp d
    on z.zalog_id = d.zalog_id
    join t_team_k7m_aux_p.k7m_cred_bal_total c 
    on c.cred_id = d.cred_id
where  d.zalog_disp_sum is  null  
    and C_BALANCE_LCL>0
) x;



--Полностью распределенный залог
drop table t_team_k7m_aux_p.k7m_zalog_dispensed;
create table t_team_k7m_aux_p.k7m_zalog_dispensed stored as parquet as    
select 
    ze.class_id,
    ze.cred_id,
    ze.zalog_id,
    ze.cred_client_id,
    ze.zalog_client_id,
    coalesce(ze.zalog_disp_sum,zi.dispensed_sum) final_dispensed_sum,
    ze.zalog_disp_sum e_zalog_disp_sum,
    zi.dispensed_sum i_zalog_disp_sum,
    zi.cred_sum,
    zi.undisp_zalog_sum,
    ze.zalog_acc
from t_team_k7m_aux_p.k7m_zalog_explisit_disp ze
    left join t_team_k7m_aux_p.k7m_zalog_implicit_disp zi
    on ze.cred_id = zi.cred_id
        and ze.zalog_id =zi.zalog_id
where  coalesce(ze.zalog_disp_sum,zi.dispensed_sum) > 0;


drop table t_team_k7m_aux_p.k7m_fok_OFR_prepared;
create table t_team_k7m_aux_p.k7m_fok_OFR_prepared stored as orc as 
select 
    inn,
    max(rep_dt) rep_dt,
    max_period,
    sum(vyruchka) vyruchka,
    sum(total_expenses) total_expenses
from (
    select 
       inn,
        yr,
        qt,
        cast( concat(yr,'-',lpad(cast(cast(qt as smallint)*3 as string),2,'0'),'-01') as date) rep_dt,
        period,
        vyruchka - lag(vyruchka,1,0) over (partition by inn,yr order by qt) vyruchka,
        total_expenses -lag(total_expenses,1,0) over (partition by inn,yr order by qt) total_expenses,
        lag(vyruchka,1,0) over (partition by inn,yr order by qt) prev_vyruchka,
        lag(total_expenses,1,0) over (partition by inn,yr order by qt) prev_total_expenses,
        max(period) over  (partition by inn) max_period
    from (
    select 
        inn,
        yr,
        qt,
           case 
                    when measure = 'тыс.' then 1000
                    when measure = 'млн.' then 1000000
                end*cast(vyruchka as double) as vyruchka,
           case 
            when measure = 'тыс.' then 1000
            when measure = 'млн.' then 1000000
            end*cast(total_expenses as double) as total_expenses         ,
            cast(yr as int)*4+qt period
    from t_team_k7m_aux_p.k7m_fok_OFR 
    where concat(yr,'-',lpad(cast(cast(qt as smallint)*3 as string),2,'0'),'-28') <'${hiveconf:DT1}' --YYYY-MM-DD
    --where inn ='0101006023'
    ) x
    ) y
where   period between   max_period-3 and max_period
    and not (prev_vyruchka+prev_total_expenses=0 and qt>1 )
group by     
    inn,
    max_period
having count(*) =4;
            

--Свод по отчетности: ОФР Сводный
drop table t_team_k7m_aux_p.k7m_OFR_final;
create table t_team_k7m_aux_p.k7m_OFR_final stored as orc as 
select
        ul_inn,
        vyruchka revenue,
        total_expenses,
        from_dt,
        to_dt,
        source
from (
    select 
        ul_inn,
        vyruchka,
        total_expenses,
        from_dt,
        to_dt,
        source,
        row_number() over (partition by ul_inn order by rep_dt desc) rn
    from (
        select 
            ul_inn, 
            cast(concat(yr,'-12-25') as date) rep_dt,
            cast(concat(yr,'-01-01') as date) from_dt,
            cast(concat(yr,'-12-31') as date) to_dt,
            vyruchka,
            total_expenses          ,
            'I' source
        from    (
            select 
                ul_inn,
                yr,
                vyruchka,
                total_expenses,
                row_number() over ( partition by ul_inn order by yr desc) rni
            from t_team_k7m_aux_p.k7m_integrum_OFR 
            where concat(yr,'-12-31')<'${hiveconf:DT1}'  
                and concat(cast(cast(yr as int)+2 as string),'-12-31')>'${hiveconf:DT1}'  
                --Берем только актуальные данные 
            ) xi
        where rni = 1
        union all
        select 
            inn ul_inn, 
            rep_dt,
            add_months(rep_dt,-4*3+1) from_dt,
            last_day(rep_dt) to_dt,
            vyruchka,
            total_expenses          ,
            'F' source
        from t_team_k7m_aux_p.k7m_fok_OFR_prepared
        where last_day(rep_dt) > add_months(date'${hiveconf:DT1}',-4*3)
        --Берем только актуальные данные 
        ) x
    ) y
where rn = 1;           
