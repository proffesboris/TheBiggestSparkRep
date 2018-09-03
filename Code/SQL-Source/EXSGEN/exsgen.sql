 drop table t_team_k7m_aux_p.exsgen_prep
 drop table  t_team_k7m_aux_p.exsgen


create table  t_team_k7m_aux_p.exsgen_prep
  stored as parquet
 as 
   select distinct 
    concat('exsgen_set',
    case when zal.id is not null and acc.c_main_v_id not like '91414%' then '_zal' else '_sur' end)   crit_id,
    guarantor.id as from_eks_id, 
    guarantor.c_inn from_inn,
    guarantor.c_name from_name,
    guarantor.class_id from_class,
    b.org_crm_id to_crm_id, 
    b.org_inn_crm_num    to_inn,
    b.org_crm_name      to_name,
    c.id cred_id
 from
    t_team_k7m_aux_d.basis_client b
    inner join     t_team_k7m_stg.eks_z_client bc
        on bc.c_inn = b.org_inn_crm_num
    inner join t_team_k7m_stg.eks_z_pr_cred AS c
        on c.C_CLIENT = bc.id      
    inner join t_team_k7m_stg.eks_z_part_to_loan pl 
        on pl.c_product= c.id 
    inner join t_team_k7m_stg.eks_Z_zalog zal 
        on zal.c_part_to_loan = pl.collection_id
    inner join t_team_k7m_stg.eks_z_ac_fin as acc 
        on zal.c_acc_zalog = acc.id
    inner join internal_eks_ibs.z_client as guarantor ---  TODO перевести на t_team_k7m_stg.eks_z_client 
        on zal.c_user_zalog = guarantor.id
  where        bc.id   <> guarantor.id
    and not (guarantor.class_id<>'CL_PRIV' and guarantor.c_inn = b.org_inn_crm_num )
  
 
--Результирующая таблица, вход для LK/LKC 
 
  create table  t_team_k7m_aux_p.exsgen
  stored as parquet
 as
    select 
        row_number() over (order by x.from_inn,x.to_inn) loc_id,
        x.*
    from (
    select 
        p.crit_id,
        p.cred_id,
        'CLU' from_obj,
        p.from_eks_id, 
        p.from_inn,
        p.from_name,
        cast(null as timestamp) from_date_of_birth,
        cast(null as string) from_doc_num,
        'CLU' to_obj,
        p.to_crm_id, 
        p.to_inn,
        p.to_name
  from t_team_k7m_aux_p.exsgen_prep p
  where p.from_class <>'CL_PRIV'
  union all
  select
        p.crit_id,
        p.cred_id,
        'CLF' from_obj,
        p.from_eks_id, 
        p.from_inn,
        p.from_name,
        cp.c_date_pers from_date_of_birth,
        concat(cp.c_doc_ser,cp.c_doc_num) from_doc_num,
        'CLU' to_obj,
        p.to_crm_id, 
        p.to_inn,
        p.to_name
   from 
    t_team_k7m_aux_p.exsgen_prep p
    inner join t_team_k7m_stg.eks_z_cl_priv cp
        on cp.id= from_eks_id
  where p.from_class ='CL_PRIV'
      ) x
    