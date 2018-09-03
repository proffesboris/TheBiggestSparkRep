--Скрипт создания архива z_main_docum за 2016.
--Для запуска силами ЦСПС на ОД.

create table custom_cb_k7m_stg.eks_z_main_docum_arc0_2016
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
from internal_eks_arc_2016_ibs.z_main_docum
where c_date_prov >=date'2016-01-01'
    and c_date_prov <date'2017-01-01';


create table custom_cb_k7m_stg.eks_z_main_docum_od_2016
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



create table custom_cb_k7m_stg.eks_z_main_docum_2016_prep
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

insert into custom_cb_k7m_stg.eks_z_main_docum_2016_prep
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
from custom_cb_k7m_stg.eks_z_main_docum_od_2016;

insert into custom_cb_k7m_stg.eks_z_main_docum_2016_prep
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
from custom_cb_k7m_stg.eks_z_main_docum_arc0_2016;







create table custom_cb_k7m_stg.eks_z_main_docum_2016
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
custom_cb_k7m_stg.eks_z_main_docum_2016_prep
) y
where rn=1;



create table custom_cb_k7m_stg.eks_z_main_docum_arc_2016
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
r,
row_number() over (partition by id order by r) rn
from 
custom_cb_k7m_stg.eks_z_main_docum_2016_prep
) y
where rn=1;
---КОНЕЦ

--Проверка результата. ОБЯЗАТЕЛЬНО
--
--1) Исполнить запрос
--select count(*) cnt,sum(    c_sum_nt ) c_sum_nt from custom_cb_k7m_stg.eks_z_main_docum_arc_2016
--
--2) Сверить результат с эталонными значениями:
--cnt= 3740308867
--sum_nt =15635602130185266.85
--
--3) Если не сошлась любая цифра - сообщить аналитику по K7M (Считать K7M на нрекорректной таблице нельзя).

