set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

--Поcчитать  нормировочный коэффициент
-- 1)

drop table t_team_k7m_aux_p.k7m_EL_5222_Norm_YULK;
create table t_team_k7m_aux_p.k7m_EL_5222_Norm_YULK stored as parquet as
select 
    org_inn ,
    abs_amt/base_amt normk,
    case when abs_amt/base_amt between 0.1 and 3 then abs_amt else 0 end  abs_amt, ---2018-04-06 Коррекция Никиты. Если меньше  0.1 (CUTOFF_LOW) - это значит клиент через нас и не работает, если >3(CUTOFF_HIGH) это какая-то ненужная фигня наподобие УК.
    case when base_amt<1000000 then 0 else base_amt end base_amt, ---2018-04-06 Коррекция Никиты. Вынести как константу в коде!!! Дезавуируем отчетность менее млн. как ничтожную
    source
from (    
select 
    b.org_inn ,
    max(ofr.source) source,
    max(cast(ofr.total_expenses as double))  base_amt ,
    suM(cast(bprov.c_sum_nt as double)) abs_amt 
from     ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_development.basis_client
    where org_inn_crm_num is not null) b
    join  t_team_k7m_aux_p.k7m_OFR_final ofr
    on b.org_inn =  ofr.ul_inn  
    join    t_corp_risks.pdeks_in_25042018  bprov  --t_team_cynomys.yia_transact_out_2019_uf
    on bprov.inn_st=b.org_inn
where bprov.ktdt=1 --Клиенты базиса в Дебете
    and   c_date_prov  between  ofr.from_dt and ofr.to_dt
    and bprov.ypred in ('оплата по договору','эквайринг', 'инкассация','лизинг','аренда') 
group by b.org_inn ) x
where base_amt>0 and abs_amt>0 and abs_amt<=base_amt;

-- Добавим те ИНН, по которым не было проводок, но есть отчетность Интегрум (ФОК)
insert into  t_team_k7m_aux_p.k7m_EL_5222_Norm_YULK 
select 
    b.org_inn ,
    null normk,
    null abs_amt,
    case when ofr.total_expenses<1000000 then 0 else ofr.total_expenses end base_amt, ---2018-04-06 Коррекция Никиты. Вынести как константу в коде!!! Дезавуируем отчетность менее млн. как ничтожную
    ofr.source
from     ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_development.basis_client
    where org_inn_crm_num is not null) b
    join  t_team_k7m_aux_p.k7m_OFR_final ofr
    on b.org_inn =  ofr.ul_inn  
    left join t_team_k7m_aux_p.k7m_EL_5222_Norm_YULK y
    on ofr.ul_inn = y.org_inn
where ofr.ul_inn is null;


-- 2)
drop table t_team_k7m_aux_p.k7m_EL_5222_Norm_YULD;
create table t_team_k7m_aux_p.k7m_EL_5222_Norm_YULD stored as parquet as
select 
    org_inn ,
    abs_amt/base_amt normk,
    case when abs_amt/base_amt between 0.1 and 3 then abs_amt else 0 end  abs_amt, ---2018-04-06 Коррекция Никиты. Если меньше 0.1 - это значит клиент через нас и не работает, если >3 это какая-то ненужная фигня наподобие УК
    case when base_amt<1000000 then 0 else base_amt end base_amt, ---2018-04-06 Коррекция Никиты. Вынести как константу в коде!!! Дезавуируем отчетность менее млн. как ничтожную
    source
from (    
select 
    b.org_inn ,
    max(ofr.source) source,
    max(cast(ofr.revenue as double))  base_amt ,
    suM(cast(bprov.c_sum_nt as double)) abs_amt 
from     ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_development.basis_client
    where org_inn_crm_num is not null) b
    join  t_team_k7m_aux_p.k7m_OFR_final ofr
    on b.org_inn =  ofr.ul_inn  
    join     t_corp_risks.pdeks_in_25042018 bprov
    on bprov.inn_st=b.org_inn
where bprov.ktdt=0 --Клиенты базиса в Кредите
    and   c_date_prov  between  ofr.from_dt and ofr.to_dt
    and bprov.ypred in ('оплата по договору','эквайринг', 'инкассация','лизинг','аренда') 
group by b.org_inn ) x
where base_amt>0 and abs_amt>0 and abs_amt<=base_amt;

-- Добавим те ИНН, по которым не было проводок, но есть отчетность Интегрум (ФОК)
insert into  t_team_k7m_aux_p.k7m_EL_5222_Norm_YULD 
select 
    b.org_inn ,
    null normk,
    null abs_amt,
    case when ofr.revenue<1000000 then 0 else ofr.revenue end base_amt, -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем отчетность менее млн. как ничтожную
    ofr.source
from     ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_development.basis_client
    where org_inn_crm_num is not null) b
    join  t_team_k7m_aux_p.k7m_OFR_final ofr
    on b.org_inn =  ofr.ul_inn  
    left join t_team_k7m_aux_p.k7m_EL_5222_Norm_YULD y
    on ofr.ul_inn = y.org_inn
where ofr.ul_inn is null;

--Суммарные обороты за 12 месяцев
drop table t_team_k7m_aux_p.k7m_EL_5222_12m;
create table t_team_k7m_aux_p.k7m_EL_5222_12m stored as parquet as
select 
      case 
        when x.ktdt=0 --Клиенты базиса в Кредите
        then 'YULD' else 'YULK'
    end typ_contr,  
    x.ktdt,
    x.org_inn ,
    x.s12m_amt  
from (
select 
  --  case 
    --    when bprov.ktdt=0 --Клиенты базиса в Кредите
        --then 'YULD' else 'YULK'
    --end c_tp,        
    bprov.ktdt,
    b.org_inn ,
    suM(cast(bprov.c_sum_nt as double)) s12m_amt 
from     ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_development.basis_client
    where org_inn_crm_num is not null) b
    --join  t_team_k7m_aux_p.k7m_OFR_final ofr
    --on b.org_inn =  ofr.ul_inn  
    join     t_corp_risks.pdeks_in_25042018  bprov
    on bprov.inn_st=b.org_inn
where c_date_prov  between  cast( add_months(date'${hiveconf:DT1}',-12) as string) and '${hiveconf:DT1}'
    and bprov.ypred in ('оплата по договору','эквайринг', 'инкассация','лизинг','аренда') 
    and cast(bprov.c_sum_nt as double)>0
group by
     bprov.ktdt,        
    b.org_inn) x;   



drop table t_team_k7m_aux_p.k7m_EL_5222_prep;
create table t_team_k7m_aux_p.k7m_EL_5222_prep stored as orc as
select 
    'YULK' typ_contr,
    bprov.inn_sec from_inn,
    b.org_inn to_inn,
    max(b.norm_base)  base_amt,
    max(b.norm_case)  base_case,
    suM(cast(bprov.c_sum_nt as double)) abs_amt
from     ( 

    select 
        distinct 
            i.org_inn_crm_num org_inn,
            case 
                when coalesce(k.base_amt,0) < 1 and m.s12m_amt> 1000000   then    m.s12m_amt  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= 1000000 then 0 -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then k.base_amt
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between 0.8 and 1.2 then m.s12m_amt*(k.base_amt/ k.abs_amt)  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <0.1 or m.s12m_amt/k.abs_amt >  3) then k.base_amt  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>1.2 then  1.2* k.base_amt -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<0.8 and m.s12m_amt/k.abs_amt > 0 then 0.8 * k.base_amt -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < 0.05 then k.base_amt
                else   m.s12m_amt
            end    norm_base,
        case 
                when coalesce(k.base_amt,0) < 1 and m.s12m_amt> 1000000   then    'case1'  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= 1000000 then 'case2' -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then 'case3'
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between 0.8 and 1.2 then 'case4'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <0.1 or m.s12m_amt/k.abs_amt >  3) then 'case5'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>1.2 then  'case6' -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<0.8 and m.s12m_amt/k.abs_amt > 0 then 'case7' -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < 0.05 then 'case8'
                else   'error'
            end       norm_case
    from t_team_k7m_development.basis_client  i   
        join t_team_k7m_aux_p.k7m_EL_5222_12m m
        on i.org_inn_crm_num = m.org_inn
        left join t_team_k7m_aux_p.k7m_EL_5222_Norm_YULK k
        on i.org_inn_crm_num = k.org_inn
    where i.org_inn_crm_num is not null
        and m.typ_contr ='YULK'
    ) b
    join    t_corp_risks.pdeks_in_25042018 bprov
    on bprov.inn_st=b.org_inn
where bprov.ktdt=1 --Клиенты базиса в Дебете
    and   c_date_prov  between  cast( add_months(date'${hiveconf:DT1}',-12) as string) and '${hiveconf:DT1}'
    and bprov.inn_sec!=bprov.inn_st
    and bprov.ypred in ('оплата по договору','эквайринг', 'инкассация','лизинг','аренда') 
group by bprov.inn_sec ,
    b.org_inn ;
  
    
    
    
    
insert into t_team_k7m_aux_p.k7m_EL_5222_prep 
select 
    'YULD' typ_contr,
    bprov.inn_sec from_inn,
    b.org_inn to_inn,
    max(b.norm_base)  base_amt,
    max(b.norm_case)  base_case,
    suM(cast(bprov.c_sum_nt as double)) abs_amt
from     ( 
      select 
        distinct 
            i.org_inn_crm_num org_inn,
            case 
                when coalesce(k.base_amt,0) < 1 and m.s12m_amt> 1000000   then    m.s12m_amt  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= 1000000 then 0 -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then k.base_amt
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between 0.8 and 1.2 then m.s12m_amt*(k.base_amt/ k.abs_amt)  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <0.1 or m.s12m_amt/k.abs_amt >  3) then k.base_amt  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>1.2 then  1.2* k.base_amt -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<0.8 and m.s12m_amt/k.abs_amt > 0 then 0.8 * k.base_amt -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < 0.05 then k.base_amt
                else   m.s12m_amt
            end    norm_base,
        case 
                when coalesce(k.base_amt,0) < 1 and m.s12m_amt> 1000000   then    'case1'  -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) < 1 and  m.s12m_amt <= 1000000 then 'case2' -- 2018-04-06 Коррекция Никиты. 1000000 Вынести как константу revenue_down_limit в коде!!! Дезавуируем обороты менее млн. как ничтожные
                when coalesce(k.base_amt,0) > 1 and  k.abs_amt < 1 then 'case3'
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt between 0.8 and 1.2 then 'case4'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and   (m.s12m_amt/k.abs_amt <0.1 or m.s12m_amt/k.abs_amt >  3) then 'case5'  -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate, 1.2 - high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt>1.2 then  'case6' -- 2018-04-06 Коррекция Никиты. 1.2 константа  high_limit_rate
                when coalesce(k.base_amt,0) > 1 and  m.s12m_amt/k.abs_amt<0.8 and m.s12m_amt/k.abs_amt > 0 then 'case7' -- 2018-04-06 Коррекция Никиты. 0.8 константа low_limit_rate
                when coalesce(k.base_amt,0) > 1 and   m.s12m_amt/k.abs_amt < 0.05 then 'case8'
                else   'error'
            end       norm_case
    from t_team_k7m_development.basis_client i   
        join t_team_k7m_aux_p.k7m_EL_5222_12m m
        on i.org_inn_crm_num = m.org_inn
        left join t_team_k7m_aux_p.k7m_EL_5222_Norm_YULD k
        on i.org_inn_crm_num = k.org_inn
    where i.org_inn_crm_num is not null
        and m.typ_contr ='YULD'
    ) b
    join    t_corp_risks.pdeks_in_25042018 bprov
    on bprov.inn_st=b.org_inn
where bprov.ktdt=0 --Клиенты базиса в Кредите
    and  bprov.c_date_prov  between  cast( add_months(date'${hiveconf:DT1}',-12) as string) and '${hiveconf:DT1}'
    and bprov.inn_sec!=bprov.inn_st
    and bprov.ypred in ('оплата по договору','эквайринг', 'инкассация','лизинг','аренда') 
group by bprov.inn_sec ,
    b.org_inn     ;        
        
        
drop table t_team_k7m_aux_p.k7m_EL_5222;
create table t_team_k7m_aux_p.k7m_EL_5222 stored as orc as
select 
    date'${hiveconf:DT1}' report_dt,
    '' from_cid,
    from_inn,
    '' to_cid,
    to_inn,
 concat('5.2.2.2',
    case 
        when   quantity>=0.25 
     
        then ''
    else 
        '_g'
     end
    ) crit_code,   
    case 
        when   quantity>=0.25 
        then 50
    else        
    10
    end probability,
    quantity,
    typ_contr source,
    base_amt ,
    abs_amt absolute_value_amt

from (
       select 
    typ_contr,
    from_inn,
    to_inn,
    base_amt,
    abs_amt,
    abs_amt/base_amt quantity,
    row_number() over (partition by  from_inn,    to_inn  order by abs_amt/base_amt )     rn
from 
        t_team_k7m_aux_p.k7m_EL_5222_prep         
where   base_amt>0
    ) x
where rn = 1;  
    
        
        
        