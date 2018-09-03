-- ELTE
insert into t_team_k7m_pa_d.bo 
select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        'ELTE' as key,
        null as value_c,
        case
            when c.country_cd = 'RUS' or bo.value_c = c.currency_cd then 0
            else cast(coalesce(sc.pte, 0) * coalesce(sc.lgte, 0) as decimal(16,4))
        end as value_n,
        null as value_d
  from
        t_team_k7m_pa_d.bo_keys a
        join t_team_k7m_pa_d.clu b on a.u7m_id = b.u7m_id
        join t_team_k7m_aux_p.amr_country_ckp c on b.reg_country = c.country_cd
        join t_team_k7m_pa_d.bo bo on (bo.bo_id = a.bo_id and bo.key = 'CURRENCY')
        left join (
            select
                    u7m_id,
                    max(case when key = 'PTE' then value_n else null end) as pte,
                    max(case when key = 'LGTE' then value_n else null end) as lgte
              from
                    t_team_k7m_pa_d.score
             group by
                    u7m_id
        ) sc on sc.u7m_id = b.u7m_id
 
-- K_DISCOUNT 
insert into t_team_k7m_pa_d.bo 
select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        'K_DISCOUNT' as key,
        null as value_c,
        case
            when abs(c.max_term - c.min_term) > 0 then 
				round(c.kmin + ((c.kmax - c.kmin)*(coalesce(bo.value_n, c.min_term) - c.min_term))/(c.max_term - c.min_term), 2)
            else 1
        end as value_n,
        null as value_d
  from
        t_team_k7m_pa_d.bo_keys a
        join t_team_k7m_pa_d.bo bo on (bo.bo_id = a.bo_id and bo.key = 'DUR_U')
        t_team_k7m_aux_p.amr_time_discount c  on (bo.value_n >= c.min_term and bo.value_n < c.max_term )

-- ELp_COEF   
insert into t_team_k7m_pa_d.bo 
select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        'ELp_COEF' as key,
        null as value_c,
        bo.value_n,
        null as value_d
  from
        t_team_k7m_pa_d.bo_keys a
        join t_team_k7m_pa_d.bo bo on bo.bo_id = a.bo_id
 where
        bo.key = 'CCF_C_OFFLINE' 

-- SKR        
insert into t_team_k7m_pa_d.bo 
select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        'SKR_OFFLINE' as key,
        null as value_c,
        greatest(0.01, round((b.value_n * bo.lgd + bo.elte) * bo.elp_coef * bo.k_discount, 2)) as value_n,
        null as value_d
  from
        t_team_k7m_pa_d.bo_keys a
		join t_team_k7m_pa_d.score b on (b.u7m_id = a.u7m_id and b.key = 'PD_OFFLINE_PRICE')
        left join (
            select
                    bo_id,
                    max(case when key = 'ELTE' then value_n else null end) as elte,
                    max(case when key = 'LGD_OFFLINE_PRICE' then value_n else null end) as lgd,
                    max(case when key = 'ELp_COEF' then value_n else null end) as elp_coef,
                    max(case when key = 'K_DISCOUNT' then value_n else null end) as k_discount
              from
                    t_team_k7m_pa_d.bo
             group by
                    bo_id
        ) bo on bo.bo_id = a.bo_id,
        