 --Отдельный этап. Выполняется ПЕРЕД ключами и служит основой для ключей.
 
 drop table t_team_k7m_aux_p.lk_lkc_raw_unfiltered;
 drop table t_team_k7m_aux_p.lk_lkc_raw;
   

    create table t_team_k7m_aux_p.lk_lkc_raw_unfiltered
    stored as parquet
     as
     --Эк.связи. Пока только CLU
     select 
        e.crit_code as crit_id,
        0 as   flag_new,
        cast(e.probability  as double) as link_prob,
        cast(e.quantity as double) quantity,
        CONCAT('EL_',cast(e.loc_id as string)) link_id,
        'CLU' as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM, 
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from t_team_k7m_aux_d.k7m_EL  e
     union all
     --СБ-связи. Только CLU
     select 
        e.crit_code as crit_id,
        0 as   flag_new,
        cast(e.probability  as double) as link_prob,
        cast(e.quantity as double) quantity,
        CONCAT('SB_',cast(e.loc_id as string)) link_id,
        'CLU' as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM, 
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from t_team_k7m_aux_d.k7m_SBL  e
     union all
     --exsgen CLU
     select 
        distinct
        e.crit_id,
        0 as   flag_new,
        cast(100 as double) as link_prob,
        cast(1 as double) as quantity,
        CONCAT('EX_',cast(e.loc_id as string)) link_id,
        e.from_obj as T_FROM,
        cast(null as string) as ID_FROM,
        e.from_inn as INN_FROM, 
        e.to_obj as T_TO,
        cast(null as string) as ID_TO,
        e.to_inn as INN_TO,
        cast(null as string) as post_nm_from
     from t_team_k7m_aux_d.exsgen  e --ПОПРАВИТЬ РЕАЛИЗАЦИЮ
     union all
     --k7m_eks_eiogen CLF (from)
     select 
        e.crit as crit_id,
        0 as   flag_new,
        cast(e.link_prob*100  as double)  as link_prob,
        cast(1 as double) quantity,
        CONCAT('EK_',cast(e.loc_id as string)) link_id,
        'CLF' as T_FROM,
        cast(null as string) as ID_FROM,
        e.inn_benef as INN_FROM, 
        'CLU' as T_TO,
        cast(null as string) as ID_TO,
        e.inn_org as INN_TO,
        e.position as post_nm_from
    from
            t_team_k7m_aux_d.k7m_eks_eiogen  e
        inner join t_team_k7m_aux_d.basis_client b
        on b.org_inn_crm_num = e.inn_org
     union all 
     --k7m_km CLF (from)
     select 
        'km' as crit_id,
        1 as   flag_new,
        cast(100  as double)   as link_prob,
        cast(1 as double) quantity,
        CONCAT('KM_',cast(e.loc_id as string)) link_id,
        'CLF' as T_FROM,
        e.km_id as ID_FROM,
        cast(null as string) as INN_FROM, 
        'CLU' as T_TO,
        e.crm_org_id as ID_TO,
        cast(null as string) as INN_TO,
        cast(null as string) as post_nm_from
    from
        t_team_k7m_aux_d.k7m_km   e
        inner join t_team_k7m_aux_d.basis_client b
            on b.org_crm_id = e.crm_org_id     
     union all
     -- t_team_k7m_aux_d.k7m_crmlk CLU
     select 
            'KM' as crit_id,
            1 as   flag_new,
            cast(100  as double)   as link_prob,
            cast(1 as double) quantity,
            CONCAT('VK_',cast(v.loc_id as string)) link_id,
            'CLF' as T_FROM,
            v.crm_id as ID_FROM,
            cast(null as string) as INN_FROM, 
            'CLU' as T_TO,
            b.org_crm_id as ID_TO,
            b.org_inn_crm_num  as INN_TO,
        cast(null as string) as post_nm_from
    from  
        t_team_k7m_aux_d.k7m_vko v
        inner join t_team_k7m_aux_d.basis_client b
    on v.org_crm_id = b.org_crm_id
     union all
     --UL 
      select 
        u.criterion  as crit_id,
        case when   u.criterion='ben_crm' then          1         else 0 end as   flag_new ,
        cast(100  as double)  as link_prob,
        quantity,
        CONCAT('UL_',cast(u.loc_id as string)) link_id,
        case when u.criterion in ('5.1.5','5.1.5g','5.1.6tp','5.1.7','EIOGen','ben_crm' ) 
               then 'CLF'
               else 'CLU'
        end  as T_FROM,
        u.crm_id1 as ID_FROM,
        u.inn1 as INN_FROM, 
        'CLU' as T_TO,
        u.crm_id2 as ID_TO,
        u.inn2 as INN_TO  ,
        u.post as post_nm_from
     from t_team_k7m_aux_d.MRT3_GSL_FINAL u ;


create table t_team_k7m_aux_p.lk_lkc_raw
    stored as parquet
     as  
     select 
        f.crit_id,
        f.flag_new,
        f.link_prob,
        f.quantity,
        f.link_id,
        f.T_FROM,
        f.ID_FROM,
        f.INN_FROM, 
        f.post_nm_from,
        f.T_TO,
        f.ID_TO,
        f.INN_TO
    from t_team_k7m_aux_p.lk_lkc_raw_unfiltered  f
        left join t_team_k7m_stg.rdm_link_criteria_mast d 
        on lower(f.crit_id) = d.code
    where ((f.quantity <= nvl(d.max_val,1000000)) and (f.quantity >= nvl( d.min_val,-1)) or f.quantity is null)
      and (length(f.INN_FROM) = 10 or length(f.INN_FROM) = 12 or f.INN_FROM is null and f.T_FROM = 'CLF')
      and (length(f.INN_TO) = 10 or length(f.INN_TO) = 12)
      and f.INN_FROM not like '000%'
      and f.INN_TO not like '000%'
      and f.INN_FROM <> '7707083893'
      and f.INN_TO <> '7707083893'
      and f.crit_id not in ('EIOGEN_EKS','EIOGen');

---Добавить EIO по расширенному базису всех ЮЛ
insert into t_team_k7m_aux_p.lk_lkc_raw
    select 
        s.crit_id,
        s.flag_new,
        s.link_prob,
        s.quantity,
        s.link_id,
        s.T_FROM,
        s.ID_FROM,
        s.INN_FROM, 
        s.post_nm_from,
        s.T_TO,
        s.ID_TO,
        s.INN_TO
    from (
    select distinct 
        INN_TO as INN
    from t_team_k7m_aux_p.lk_lkc_raw
    where T_FROM = 'CLU'
        and INN_TO is not null
        ) x
    join 
    t_team_k7m_aux_p.lk_lkc_raw_unfiltered    s   
    on x.inn = s.INN_TO
  where (length(s.INN_FROM) = 10 or length(s.INN_FROM) = 12 or s.INN_FROM is null and s.T_FROM = 'CLF')
      and (length(s.INN_TO) = 10 or length(s.INN_TO) = 12)
      and s.INN_FROM not like '000%'
      and s.INN_TO not like '000%'
      and s.INN_FROM <> '7707083893'
      and s.INN_TO <> '7707083893'
      and s.crit_id in ('EIOGEN_EKS','EIOGen')  ;  
    
