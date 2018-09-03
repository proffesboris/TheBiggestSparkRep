set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;


drop table t_team_k7m_aux_p.k7m_payments_for_5212;
create table t_team_k7m_aux_p.k7m_payments_for_5212 stored as parquet as
select 
    coalesce(c.c_high_level_cr,c.id) cred_id,
    max(c.c_client) client_id,
    coalesce(min(
        case when i.oper_type ='ISSUE' then
        c_date
        else null
        end
        ),
    unix_timestamp(cast(add_months(date'${hiveconf:DT1}',-5*3)as timestamp)   )*1000       
    )  issue_dt,
    sum(
        case when i.oper_type !='ISSUE' then
        c_summa
        else null
        end
        ) payment_amt         
from t_team_k7m_aux_p.k7m_z_all_cred c
    join 
    t_team_k7m_aux_p.k7m_z_fact_oper_issues i 
    on c.c_list_pay = i.collection_id
where i.c_date <= unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp)   )*1000    
    and i.c_date>=unix_timestamp(cast(add_months(date'${hiveconf:DT1}',-5*3)as timestamp)   )*1000    
group by     coalesce(c.c_high_level_cr,c.id)
having 
sum(
        case when i.oper_type !='ISSUE' then
        c_summa
        else null
        end
        )>0;        

drop table t_team_k7m_aux_p.k7m_link_for_5212a;
create table t_team_k7m_aux_p.k7m_link_for_5212a stored as parquet as
select 
 cred_id,
    acc_id,
    issue_dt,
    payment_amt,
    cred_class_id,
    cred_inn,
   zalog_class_id,
   case when zalog_inn <> cred_inn  or  zalog_inn is null or zalog_class_id <>'CL_PRIV' then  zalog_inn else '' end zalog_inn
from (
select distinct
    p.cred_id,
    ca.acc_id,
    p.issue_dt,
    p.payment_amt,
    cc.class_id cred_class_id,
    cc.c_inn cred_inn,
   cz.class_id zalog_class_id,
    cz.c_inn zalog_inn
from (
    select *
    from     t_team_k7m_aux_p.k7m_zalog_dispensed 
    where zalog_acc like '91414%' ---Поручительства
     )   d
    right join    t_team_k7m_aux_p.k7m_payments_for_5212 p
    on p.cred_id = d.cred_id
    join t_team_k7m_aux_p.k7m_cred_to_acc ca
    on ca.cred_id = p.cred_id
    join
    t_team_k7m_aux_p.k7m_Z_CLIENT    cc
    on cc.id=p.client_id
    join      ( 
    select distinct org_inn_crm_num org_inn from t_team_k7m_aux_p.basis_client
    where org_inn_crm_num is not null) b
    on b.org_inn =  cc.c_inn 
    left join
    t_team_k7m_aux_p.k7m_Z_CLIENT    cz
    on cz.id=d.zalog_client_id

) x;
        

---core_internal_eks.z_main_docum падал. Пришлось брать размеченные транзакции, т.к. их меньше. Возможно в целевом алгоритме надо тоже их брать.
drop table t_team_k7m_aux_p.k7m_z_main_docum;
create table t_team_k7m_aux_p.k7m_z_main_docum stored as parquet as 
select id, unix_timestamp(c_date_prov)*1000 c_date_prov,inn_st prov_kt_inn,inn_sec prov_dt_inn,C_SUM,C_SUM_PO,C_SUM_NT,c_acc_kt, c_nazn, c_num_dt,c_num_kt
from t_team_k7m_stg.z_main_kras_new     
where c_date_prov>=cast(add_months(date'${hiveconf:DT1}',-5*3)as timestamp)   
    --and c_kl_kt_0 =1
    and    ktdt=0 --Клиенты базиса в Кредите
    and inn_st<>inn_sec --плательщик не равен получателю
;          

--Справочный агрегат. Для детализациия расчета       
drop table t_team_k7m_aux_p.k7m_EL5212a_detail;
create table t_team_k7m_aux_p.k7m_EL5212a_detail stored as parquet as         
select 
    a.cred_id,
    a.issue_dt,
    b.id prov_id,
    b.c_acc_kt ,
    a.zalog_class_id ,
    b.prov_kt_inn,
    b.prov_dt_inn,--Важно!  Берем исправленный ИНН алгоритм:
                            --c_kl_dt_2_inn заполняется в 98% случаев если c_kl_dt_0 =1.если, например, C_KL_DT#0=2, то мы должны брать ИНН из поля c_kl_dt_2_inn, 
                            --а если C_KL_dT#0=1, то необходимо по ссылке     c_acc_dt найти счет ac_fin coalesce(c_client_r,c_client_v), далее найти клиента по счету и получит ИНН из z_clients.
                            ---Часто проставляют ИНН Сбербанка если C_KL_dT#0=1. Хотя счет клиентский
    a.cred_class_id ,
    a.cred_inn,
    a.zalog_inn,
    A.payment_amt  fact_SUM,
    C_SUM_PO AS PROV_SUM,
    b.c_nazn
from t_team_k7m_aux_p.k7m_link_for_5212a a
    join t_team_k7m_aux_p.k7m_z_main_docum b ---Вернуть core_internal_eks.z_main_docum 
    on a.acc_id = b.c_acc_kt 
 where b.c_date_prov>=a.issue_dt
    and b.c_date_prov<=  unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp)   )*1000
    --and b.c_kl_kt_0 =1
    and b.prov_kt_inn<>b.prov_dt_inn --плательщик не равен получателю        
;         
        
drop table t_team_k7m_aux_p.k7m_EL5212a_prep;
create table t_team_k7m_aux_p.k7m_EL5212a_prep stored as parquet as 
select x.cred_id,
    x.issue_dt,
    x.zalog_class_id ,
    x.cred_class_id ,
    x.to_inn  ,
    x.from_inn ,
    x.crit_code,
    x.fact_SUM,
    x.PROV_SUM,
    x.PROV_SUM/x.fact_SUM as quantity
from (
select 
    a.cred_id,
    a.issue_dt,
    a.zalog_class_id ,
    case when a.zalog_inn=b.prov_dt_inn then '5.2.1.2' else  '5.2.1.4' end crit_code,
    b.prov_dt_inn to_inn,--Важно!  c_kl_dt_2_inn заполняется в 98% случаев если c_kl_dt_0 =1.если, например, C_KL_DT#0=2, то мы должны брать ИНН из поля c_kl_dt_2_inn, 
                            --а если C_KL_dT#0=1, то необходимо по ссылке     c_acc_dt найти счет ac_fin coalesce(c_client_r,c_client_v), далее найти клиента по счету и получит ИНН из z_clients.
    a.cred_class_id ,
    a.cred_inn from_inn,
    CAST(A.payment_amt AS double) fact_SUM,
    sum(CAST( C_SUM_PO AS double)) AS PROV_SUM
from t_team_k7m_aux_p.k7m_link_for_5212a a
    join t_team_k7m_aux_p.k7m_z_main_docum b ---Вернуть core_internal_eks.z_main_docum 
    on a.acc_id = b.c_acc_kt 
 where b.c_date_prov>=a.issue_dt
    and b.c_date_prov<=  unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp)   )*1000
    --and b.c_kl_kt_0 =1
    and b.prov_kt_inn<>b.prov_dt_inn --плательщик не равен получателю
    and b.prov_kt_inn<>'7707083893'
    and b.prov_dt_inn<>'7707083893'
    and substr(b.c_num_dt,1,5) not between '44000' and '45999'
group by 
    a.cred_id,
    a.issue_dt,
    a.zalog_class_id ,
    case when a.zalog_inn=b.prov_dt_inn then '5.2.1.2' else  '5.2.1.4' end,
    b.prov_dt_inn  ,
    a.cred_class_id ,
    a.cred_inn ,
    CAST(A.payment_amt AS double) 
) x;
        
        
--Результат по критериям 5.2.1.2,  5.2.1.4
drop table t_team_k7m_aux_p.k7m_EL_5212_5214;
create table t_team_k7m_aux_p.k7m_EL_5212_5214 stored as orc as
SELECT 
    date'${hiveconf:DT1}' report_dt,
    from_cid,
    from_inn,
    to_cid,
    to_inn,
      concat(crit_code,
    case 
        when   quantity>=0.5 
    and  unix_timestamp(cast(date'${hiveconf:DT1}'as timestamp)   )*1000 -issue_dt>= 3*3600*24*30*1000
 or quantity>=1
        then ''
 else 
        case 
            when from_cid ='CL_PRIV' then '_gfl' 
            else '_gul'
        end
end        ) crit_code,
    100 probability,
    quantity,
    issue_dt source,
    fact_SUM base_amt,
    PROV_SUM absolute_value_amt
FROM (    
    select 
        zalog_class_id from_cid,
        from_inn,
        cred_class_id to_cid,
        to_inn,
        issue_dt,
        quantity,
        PROV_SUM,
        fact_SUM,
        cred_id,
        crit_code,
        ROW_NUMBER() OVER (PARTITION BY crit_code,to_inn, from_inn ORDER BY quantity DESC) RN
    from t_team_k7m_aux_p.k7m_EL5212a_prep
) x
WHERE RN=1   ;     
        
