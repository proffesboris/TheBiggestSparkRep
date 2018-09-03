--Ïîäãîòîâêà àðõèâíîé òàáëèöû z_main_docum çà 2016 ãîä 
--TODO îôîðìèòü ñêðèïò äëÿ ïðÿìîãî ðàñ÷åòà íà ÎÄ.
create table t_team_k7m_stg.eks_z_main_docum_arc_2016
stored as parquet
as
select 
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart
from t_team_k7m_prototypes.eks_z_main_docum_arc_2016; --Ð˜Ð¡Ð¢ÐžÐ§ÐÐ˜Ðš ÐžÐ” internal_eks_arc_2016_ibs.z_main_docum. ÐŸÐ¾ÑÑ‚Ð°Ð²Ð»ÐµÐ½Ð¾ Ð”Ð°Ñ‚Ð°ÑÐµÑ€Ð²Ð¸ÑÐ°Ð¼Ð¸.


create table t_team_k7m_prototypes.eks_z_main_docum_od_2016
stored as parquet
as 
select 
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart
from internal_eks_ibs.z_main_docum
where c_date_prov >=date'2016-01-01'
    and c_date_prov <date'2017-01-01';



create table t_team_k7m_prototypes.eks_z_main_docum_2016_prep
(
id decimal(38,12), 
class_id string,
state_id string,
c_acc_dt decimal(38,12), 
c_acc_kt decimal(38,12), 
c_date_prov timestamp, 
c_kl_dt_1_1 decimal(38,12), 
c_kl_dt_1_2 decimal(38,12), 
c_kl_dt_2_1 string, 
c_kl_dt_2_inn string, 
c_kl_dt_2_2 string, 
c_kl_dt_2_3 decimal(38,12), 
c_kl_dt_2_kpp string, 
c_kl_kt_1_1 decimal(38,12), 
c_kl_kt_1_2 decimal(38,12), 
c_kl_kt_2_1 string, 
c_kl_kt_2_inn string, 
c_kl_kt_2_2 string, 
c_kl_kt_2_3 decimal(38,12), 
c_kl_kt_2_kpp string, 
c_nazn string, 
c_sum decimal(17,2), 
c_valuta decimal(38,12), 
c_vid_doc decimal(38,12), 
c_kod_nazn_pay decimal(38,12), 
c_valuta_po decimal(38,12), 
c_sum_po decimal(17,2), 
c_multicurr decimal(38,12), 
c_sum_nt decimal(17,2), 
c_type_mess decimal(38,12), 
c_code_doc decimal(38,12), 
c_num_dt  string, 
c_num_kt  string, 
c_filial decimal(38,12), 
c_depart decimal(38,12), 
r smallint
)
CLUSTERED BY (ID) INTO 256 BUCKETS
STORED AS PARQUET;

insert into t_team_k7m_prototypes.eks_z_main_docum_2016_prep
select 
    id,
    class_id,
    state_id,
    c_acc_dt,
    c_acc_kt,
    c_date_prov,
    c_kl_dt_1_1,
    c_kl_dt_1_2,
    c_kl_dt_2_1,
    c_kl_dt_2_inn,
    c_kl_dt_2_2,
    c_kl_dt_2_3,
    c_kl_dt_2_kpp,
    c_kl_kt_1_1,
    c_kl_kt_1_2,
    c_kl_kt_2_1,
    c_kl_kt_2_inn,
    c_kl_kt_2_2,
    c_kl_kt_2_3,
    c_kl_kt_2_kpp,
    c_nazn,
    c_sum,
    c_valuta,
    c_vid_doc,
    c_kod_nazn_pay,
    c_valuta_po,
    c_sum_po,
    c_multicurr,
    c_sum_nt,
    c_type_mess,
    c_code_doc,
    c_num_dt,
    c_num_kt,
    c_filial,
    c_depart,
    1 r
from t_team_k7m_prototypes.eks_z_main_docum_od_2016;

insert into t_team_k7m_prototypes.eks_z_main_docum_2016_prep
select 
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart,
2 r
from t_team_k7m_prototypes.eks_z_main_docum_arc_2016;







spark.sql(""" 
create table t_team_k7m_prototypes.eks_z_main_docum_2016
stored as parquet
as     
select
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart
from (
select 
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart,
row_number() over (partition by id order by r) rn
from 
t_team_k7m_prototypes.eks_z_main_docum_2016_prep
) y
where rn=1
""") show 199




spark.sql(""" 
create table t_team_k7m_prototypes.eks_z_main_docum_2016
stored as parquet
as     
select
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart,
r
from (
select 
id,
class_id,
state_id,
c_acc_dt,
c_acc_kt,
c_date_prov,
c_kl_dt_1_1,
c_kl_dt_1_2,
c_kl_dt_2_1,
c_kl_dt_2_inn,
c_kl_dt_2_2,
c_kl_dt_2_3,
c_kl_dt_2_kpp,
c_kl_kt_1_1,
c_kl_kt_1_2,
c_kl_kt_2_1,
c_kl_kt_2_inn,
c_kl_kt_2_2,
c_kl_kt_2_3,
c_kl_kt_2_kpp,
c_nazn,
c_sum,
c_valuta,
c_vid_doc,
c_kod_nazn_pay,
c_valuta_po,
c_sum_po,
c_multicurr,
c_sum_nt,
c_type_mess,
c_code_doc,
c_num_dt,
c_num_kt,
c_filial,
c_depart,
row_number() over (partition by id order by r) rn
from 
t_team_k7m_prototypes.eks_z_main_docum_2016_prep
) y
where rn=1
""") show 199
drop table t_team_k7m_stg.eks_z_main_docum_arc_2016;

create table t_team_k7m_stg.eks_z_main_docum_arc_2016 --ÐŸÐµÑ€ÐµÐ½ÐµÑÑ‚Ð¸ Ñ‚Ð°Ð±Ð»Ð¸Ñ†Ñƒ Ð² ÐžÐ” (custom_cb_k7m_stg)
stored as parquet
as 
select * from t_team_k7m_prototypes.eks_z_main_docum_2016;

---ÐšÐžÐÐ•Ð¦

--Ð¡Ð²ÐµÑ€ÐºÐ° Ñ ODS
create table z_main_docum_ods_2016
stored as parquet
as
select 
    cast(id as decimal(38,12)) as id, 
    c_date_prov, 
    from_unixtime(cast(c_date_prov/1000 as int)) c_date_prov_ts,
    cast(c_sum_nt as decimal(17,2)) as  c_sum_nt
from core_internal_eks.z_main_docum	
where ods_opc<>'D'
    and c_date_prov >=unix_timestamp('2016-01-01 00:00:00')*1000
    and c_date_prov <unix_timestamp('2017-01-01 00:00:00')*1000;
    
    
create table t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3
stored as parquet
as
select 
    id, sum(s) s
from (
select id,
    c_sum_nt s 
from t_team_k7m_prototypes.eks_z_main_docum_2016
union all
select id,
    -c_sum_nt s
from t_team_k7m_prototypes.z_main_docum_ods_2016
) x
group by id 
having sum(s)<>0;


create table t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3_detail_OD
 stored as parquet
 as
 select t.*,s
 from t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3 s
 join  t_team_k7m_prototypes.eks_z_main_docum_2016 t
 on t.id = s.id;
 
 
 spark.sql(""" 
create table t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3_detail_ODS
 stored as parquet
 as
 select t.*,s
 from t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3 s
 join  t_team_k7m_prototypes.z_main_docum_ods_2016 t
 on t.id = s.id
""") show 199


spark.sql(""" 
select 
    sign(s),
    count(*)
from t_team_k7m_prototypes.eks_z_main_docum_diff_2016_v3
group by     sign(s)
""") show 199
    

