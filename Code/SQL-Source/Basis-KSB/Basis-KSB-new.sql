create table user_iyarukov.innimport190417_t17_1 stored as orc
as
select * 
from (select  id
  ,C_INN
  ,odsopc
  ,row_number() over (partition by id order by odsvalidfrom desc, odssequenceid desc ) as rn
  from core_internal_eks.Z_CLIENT  a 
)t
 where t.rn=1 
 and t.C_INN in (Select inn from user_iyarukov.for_transact190417_till2017)
 ;

create table user_iyarukov.innimport190417_t17_2 stored as orc
as 
Select a.* 
from (
                               select  
                                               z_ac_fin.id                                               
                                               ,z_ac_fin.c_client_v
                                               ,from_unixtime(cast(z_ac_fin.odsvalidfrom / 1000 as int)) as odsvalidfrom
                                               ,z_ac_fin.odsopc 
                                               ,z_ac_fin.odssequenceid
                                               --для фильтрации истории добавим счетчик записей
                                               ,row_number() over (partition by id order by from_unixtime(cast(z_ac_fin.odsvalidfrom / 1000 as int)) desc) as rn
                               from core_internal_eks.z_ac_fin
                               where 1=1
                               ) a
where 1=1
--нужна только последняя версия 
and a.rn = 1
and a.c_client_v in (Select id from user_iyarukov.innimport190417_t17_1)
;

create table user_iyarukov.innimport190417_t17_3 stored as orc
as select 
v.id,
'0' as ktdt, 
v.class_id,
v.state_id,
v.c_acc_dt,
v.c_acc_kt,
from_unixtime(cast(v.c_date_prov / 1000 as int)) c_date_prov,
v.c_kl_dt_1_1,
v.c_kl_dt_1_2,
v.c_kl_dt_2_1,
v.c_kl_dt_2_inn,
v.c_kl_dt_2_2,
v.c_kl_dt_2_3,
v.c_kl_dt_2_kpp,
v.c_kl_kt_1_1,
v.c_kl_kt_1_2,
v.c_kl_kt_2_1,
v.c_kl_kt_2_inn,
v.c_kl_kt_2_2,
v.c_kl_kt_2_3,
v.c_kl_kt_2_kpp,
v.c_nazn,
cast(v.c_sum as decimal(23,5)) as c_sum,
v.c_valuta,
v.c_vid_doc,
v.c_kod_nazn_pay,
v.c_valuta_po,
cast(v.c_sum_po as decimal(23,5)) as c_sum_po,
v.c_multicurr,
cast(v.c_sum_nt as decimal(23,5)) as c_sum_nt,
v.c_type_mess,
v.c_code_doc,
v.c_num_dt,
v.c_num_kt,
v.c_filial,
v.c_depart,
v.odsvalidfrom,
v.odsopc,
v.odssequenceid,
case when a.id is not null then a.c_client_v
     else -100
end as c_client_v

from core_internal_eks.z_main_docum_v  v
left join user_iyarukov.innimport190417_t17_2 a on v.c_acc_kt=a.id

where v.c_acc_kt in (select distinct id from user_iyarukov.innimport190417_t17_2) 
or 
v.C_KL_KT_2_INN in (Select inn from user_iyarukov.for_transact190417_till2017)
      
UNION ALL

select 
v.id,
'1' as ktdt, 
v.class_id,
v.state_id,
v.c_acc_dt,
v.c_acc_kt,
from_unixtime(cast(v.c_date_prov / 1000 as int)) c_date_prov,
v.c_kl_dt_1_1,
v.c_kl_dt_1_2,
v.c_kl_dt_2_1,
v.c_kl_dt_2_inn,
v.c_kl_dt_2_2,
v.c_kl_dt_2_3,
v.c_kl_dt_2_kpp,
v.c_kl_kt_1_1,
v.c_kl_kt_1_2,
v.c_kl_kt_2_1,
v.c_kl_kt_2_inn,
v.c_kl_kt_2_2,
v.c_kl_kt_2_3,
v.c_kl_kt_2_kpp,
v.c_nazn,
cast(v.c_sum as decimal(23,5)) as c_sum,
v.c_valuta,
v.c_vid_doc,
v.c_kod_nazn_pay,
v.c_valuta_po,
cast(v.c_sum_po as decimal(23,5)) as c_sum_po,
v.c_multicurr,
cast(v.c_sum_nt as decimal(23,5)) as c_sum_nt,
v.c_type_mess,
v.c_code_doc,
v.c_num_dt,
v.c_num_kt,
v.c_filial,
v.c_depart,
v.odsvalidfrom,
v.odsopc,
v.odssequenceid,
case when a.id is not null then a.c_client_v
     else -100
end as c_client_v

from core_internal_eks.z_main_docum_v  v
left join user_iyarukov.innimport190417_t17_2 a on v.c_acc_dt=a.id

  where v.c_acc_dt in (select distinct id from user_iyarukov.innimport190417_t17_2)
  or v.C_KL_DT_2_INN in (Select inn from user_iyarukov.for_transact190417_till2017) 

;

create table user_iyarukov.innimport190417_t17_4 stored as orc
as 
SELECT A.*
,B.C_CUR_ATTR_CUR_P_2UNIT AS C_VALUTA_LABEL
,C.C_CUR_ATTR_CUR_P_2UNIT AS c_valuta_po_LABEL
,D.C_NAME AS c_vid_doc_LABEL
,E.C_MEMO_NAZN AS c_kod_nazn_pay_LABEL
,g.c_inn as inn_st
,h.c_shortlabel as c_filial_label
,case when c_multicurr=1920418 then 'Область покрытия плательщика'
   when c_multicurr=1920419 then 'Область покрытия  получателя'
   when c_multicurr=1920420 then 'Мультивалютность'
   when c_multicurr=1920421 then 'Область национального покрытия'
   else null
 end as c_multicurr_label
,j.c_name as c_type_mess_label

,row_number() over (partition by a.id,ktdt order by a.odsvalidfrom desc, a.odssequenceid desc, a.odsopc desc) as rn
FROM user_iyarukov.innimport190417_t17_3  A 
LEFT JOIN core_internal_eks.Z_FT_MONEY B ON A.C_VALUTA=B.ID
LEFT JOIN core_internal_eks.Z_FT_MONEY C ON A.c_valuta_po=C.ID
LEFT JOIN core_internal_eks.z_name_paydoc D on A.C_VID_DOC=D.id
LEFT JOIN core_internal_eks.Z_KOD_N_PAY E ON A.c_kod_nazn_pay=E.id
inner join user_iyarukov.innimport190417_t17_1 g on a.c_client_v=g.id
left join core_internal_eks.Z_branch h on a.c_filial=h.id
left join core_internal_eks.SBRF_TYPE_MESS j on a.c_type_mess=j.id
where upper(a.state_id)='PROV' and upper(a.class_id) = 'MAIN_DOCUM'
;


create table user_iyarukov.innimport190417_t17_5 stored as orc
as 
select * from user_iyarukov.innimport190417_t17_4
where rn=1;
 
 

drop table if exists user_iyarukov.dt1_hive010617;
create table user_iyarukov.dt1_hive010617 stored as orc as 
select id,ktdt, C_KL_DT_1_1,C_KL_DT_2_INN
from user_iyarukov.innimport190417_t17_5
where ktdt=0;

drop table if exists user_iyarukov.kt1_hive010617;
create table user_iyarukov.kt1_hive010617 stored as orc as 
select id,ktdt, C_KL_KT_1_1,C_KL_KT_2_INN
from user_iyarukov.innimport190417_t17_5
where ktdt=1;


drop table if exists user_iyarukov.distclient_hive010617;
create table user_iyarukov.distclient_hive010617 stored as orc as 
select distinct idclient from (
select  C_KL_DT_1_1 as idclient from  user_iyarukov.dt1_hive010617
union all
select  C_KL_KT_1_1 as idclient from  user_iyarukov.kt1_hive010617) aa;

drop table if exists user_iyarukov.innsec_hive010617;
create table user_iyarukov.innsec_hive010617 stored as orc
as
select * 
from (select  a.*
	,row_number() over (partition by id order by odsvalidfrom desc, odssequenceid desc) as rn
	from core_internal_eks.Z_CLIENT  a 
	where a.id in (select idclient from user_iyarukov.distclient_hive010617 where idclient is not null)
)t
 where t.rn=1;

drop table if exists user_iyarukov.innimport190417_6; 
create table user_iyarukov.innimport190417_6 stored as orc as 
select a.*,coalesce(a.c_kl_kt_2_INN,b.c_inn) as inn_sec,b.c_name as kl_2_org
from user_iyarukov.innimport190417_t17_5 a
left join user_iyarukov.innsec_hive010617 b on a.c_kl_kt_1_1=b.id 
where a.ktdt=1
union all
select a.*,coalesce(a.c_kl_dt_2_INN,c.c_inn) as inn_sec,c.c_name as kl_2_org
from user_iyarukov.innimport190417_t17_5 a
left join user_iyarukov.innsec_hive010617 c on a.c_kl_dt_1_1=c.id 
where a.ktdt=0 
;