insert into t_team_cynomys.k7m_EL 
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
   left semi join team_business_services.tmp_nz_list l
    on l.inn = e.to_inn ;        