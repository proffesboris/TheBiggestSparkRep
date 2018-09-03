insert into t_team_cynomys.k7m_EL3 
select distinct 
    e.*
from (
    select * from t_team_k7m_prototypes.k7m_EL_5211
    union all
    select * from t_team_k7m_prototypes.k7m_EL_5212_5214
    union all
    select * from t_team_k7m_prototypes.k7m_EL_5213
    union all
    select * from t_team_k7m_prototypes.k7m_EL_5222
    
    )    e
   left semi join t_team_cynomys.gsl_target_all  l
    on l.inn = e.to_inn 
        and to_date(l.dateto) = e.report_dt
where e.to_inn<>e.from_inn
    and length(e.from_inn) in (10,12)
    and e.from_inn <>'7707083893';        