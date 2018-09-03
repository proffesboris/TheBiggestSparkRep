set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;



drop table t_team_k7m_aux_p.k7m_EL_5213_issues;
create table t_team_k7m_aux_p.k7m_EL_5213_issues stored as orc as
select 
    c.c_client,
    cl.c_inn,
    max(c.c_ft_credit) c_ft_credit,
    coalesce(c.c_high_level_cr,c.id) cred_id,
    i.issue_dt,
    sum(c_summa*coalesce(crs.course,1))  issue_amt,
    max(crs.course) course
from t_team_k7m_aux_p.k7m_cred_bal_total b
    join t_team_k7m_aux_p.k7m_z_all_cred c
    on c.id = b.cred_id
    join 
        (
        select distinct collection_id,c_date issue_dt,     c_summa
        from t_team_k7m_aux_p.k7m_z_fact_oper_issues
        where oper_type ='ISSUE'
            and    c_date<=unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp)   )*1000  
               and    c_date>=unix_timestamp(cast(add_months(date'${hiveconf:DT1}',-5*3) as timestamp)   )*1000  
            ) i
    on i.collection_id = c.c_list_pay
    join t_team_k7m_aux_p.k7m_z_client cl
    on cl.id = c.c_client
    join t_team_k7m_aux_p.k7m_cur_courses crs
    on c.c_ft_credit = crs.id
where   i.issue_dt  between  crs.c_date_recount and crs.till_c_date_recount
group by
     c.c_client,
     cl.c_inn,
     i.issue_dt,
     coalesce(c.c_high_level_cr,c.id);
     
drop table t_team_k7m_aux_p.k7m_EL_5213_prep;
create table t_team_k7m_aux_p.k7m_EL_5213_prep stored as orc as
select 
    bprov.inn_sec from_inn,
    issues.c_inn to_inn,
    issues.cred_id,
    issues.issue_amt,
    issues.issue_dt,
    issues.c_ft_credit,
    bprov.ypred  predicted_value,
    bprov.c_date_prov,
    bprov.id prov_id,
    cast(bprov.c_sum_nt as double) * cast(issues.issue_amt/sum(issues.issue_amt)  over (partition by bprov.id) as double) abs_amt
  from    
    t_team_k7m_aux_p.k7m_EL_5213_issues issues
    join  t_team_k7m_stg.z_main_kras_new bprov
    on bprov.inn_st = issues.c_inn
where bprov.ktdt=1 --Клиенты базиса в Дебете
    and   unix_timestamp(cast(c_date_prov as timestamp))*1000 between  issues.issue_dt and issues.issue_dt+3600.*24.*1000.*30.
    and bprov.ypred in ('выдача займа','беспроцентный займ') ;

            
---Результат    
drop table t_team_k7m_aux_p.k7m_EL_5213;
create table t_team_k7m_aux_p.k7m_EL_5213 stored as orc as
select 
    date'${hiveconf:DT1}' report_dt,
    '' from_cid,
    from_inn,
    '' to_cid,
    to_inn,
 concat('5.2.1.3',
    case 
        when   quantity>=0.25 
     
        then ''
    else 
        '_g'
     end
    ) crit_code,   
    100 probability,
    quantity,
    cred_id source,
    issue_amt base_amt,
    abs_amt absolute_value_amt
from (
    select 
            from_inn,
            to_inn,
            cred_id,
            issue_dt,
            issue_amt,
            abs_amt,
            quantity,
            row_number() over (partition by from_inn,to_inn order by quantity desc) rn
            
    from (
        select 
            from_inn,
            to_inn,
            cred_id,
            issue_dt,
            sum(abs_amt) abs_amt,
            issue_amt,
            sum(abs_amt)/issue_amt quantity
        from t_team_k7m_aux_p.k7m_EL_5213_prep 
        where issue_amt>0
        group by 
            from_inn,
            to_inn,
            cred_id,
            issue_dt,
            issue_amt
            ) x
     ) x
    join      ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_aux_p.basis_client
    where org_inn_crm_num is not null) b
    on b.org_inn =  x.to_inn      
where rn=1     ;
     
