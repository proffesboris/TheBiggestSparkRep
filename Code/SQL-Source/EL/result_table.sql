drop table t_team_k7m_aux_p.k7m_EL; 
create table t_team_k7m_aux_p.k7m_EL stored as parquet  as
select distinct 
    e.*
from (
    select cast(report_dt as timestamp) report_dt, from_inn, to_inn, crit_code, probability,quantity,source,base_amt,absolute_value_amt
    from t_team_k7m_aux_p.k7m_EL_5211
    union all
    select cast(report_dt as timestamp) report_dt, from_inn, to_inn, crit_code, probability,quantity,source,base_amt,absolute_value_amt
    from t_team_k7m_aux_p.k7m_EL_5212_5214
    union all
    select  cast(report_dt as timestamp) report_dt, from_inn, to_inn, crit_code, probability,quantity,source,base_amt,absolute_value_amt
    from t_team_k7m_aux_p.k7m_EL_5213
    union all
    select  cast(report_dt as timestamp) report_dt, from_inn, to_inn, crit_code, probability,quantity,source,base_amt,absolute_value_amt
    from t_team_k7m_aux_p.k7m_EL_5222
    )    e
where e.to_inn<>e.from_inn
    and length(e.from_inn) in (10,12)
    and e.from_inn <>'7707083893'
    and e.from_inn not like '00%';        
