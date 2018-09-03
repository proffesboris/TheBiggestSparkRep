set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


--Результат по критерию 5.2.1.2
drop table t_team_k7m_aux_p.k7m_EL_5211;
create table t_team_k7m_aux_p.k7m_EL_5211 stored as orc as
select 
    date'${hiveconf:DT1}' report_dt,
    zalog_class_id from_cid,
    zalog_inn  from_inn,
    cred_class_id to_cid,
    cred_inn to_inn,
    concat('5.2.1.1',(case when ta1600 is not  null and dispensed_sum/ta1600<0.1 then '_g' else '' end)) crit_code,
    case when ta1600 is null then 10 else 100 end probability,
    dispensed_sum/ta1600 quantity,
	source,
	ta1600 base_amt,
	dispensed_sum absolute_value_amt
from (
select 
    cc.class_id cred_class_id,
    cc.c_inn cred_inn,
    cz.class_id zalog_class_id,
    cz.c_inn zalog_inn,
    max(f.ta1600) ta1600,
	max(f.source) source,
    sum(d.final_dispensed_sum) dispensed_sum
from t_team_k7m_aux_p.k7m_zalog_dispensed   d
    join
    t_team_k7m_aux_p.k7m_Z_CLIENT    cc
    on cc.id=d.cred_client_id
	join 	 ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_aux_p.basis_client
    where org_inn_crm_num is not null) b
	on b.org_inn =  cc.c_inn 
    join
    t_team_k7m_aux_p.k7m_Z_CLIENT    cz
    on cz.id=d.zalog_client_id
    left join 
        (select 
        ul_inn inn,ta1600,source
        from 
        t_team_k7m_aux_p.k7m_rep1600_final
        where dt>=add_months(date'${hiveconf:DT1}',-5*3) and source ='F'
	  or dt>=add_months(date'${hiveconf:DT1}',-24*2) and source ='I'
        )    f
    on cz.c_inn  = f.inn
group by 
  cc.class_id,
    cc.c_inn,
    cz.class_id,
    cz.c_inn 
) x
where zalog_inn <> cred_inn    
        and zalog_class_id <>'CL_PRIV'
;
