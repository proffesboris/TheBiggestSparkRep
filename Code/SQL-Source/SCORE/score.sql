/*шаг1 - создание таблицы key*/
create table t_team_k7m_pa_d.score_keys as 
select distinct
        pd.exec_id,
        cast(NULL as bigint) as okr_id,
        pd.u7m_id
  from
        t_team_k7m_pa_d.pdout pd
        join t_team_k7m_pa_d.clu bc on (pd.u7m_id = bc.u7m_id)     
 where
		bc.flag_basis_client = 'Y'


/*шаг2 - создание таблицы key-value
нужно взять t_team_k7m_pa_d.pdout
развернуть ее в key-value вид с ключом {exec_id, okr_id, u7m_id}
по столбцам (score_st, score, ws1..ws100, factor1..factor100, cindex_st, cindex, pd_standalone, pd_offline, pd_offline_price, pd_offline_rezerv,
    calibr_param1, calibr_param2, calibr_param3, calibr_param_price1, calibr_param_price2, calibr_param_price3, calibr_param_st1, calibr_param_st2, calibr_param_st3, 
    rating_offline, rating_offline_price, rating_offline_rezerv)

Для этого нужно вызвать функцию преобразования в key-value вид, написанную Костиковым К. с параметрами:
	схема исходной таблицы: t_team_k7m_pa_d
	название исходной таблицы: pdout
	схема выходной таблицы: t_team_k7m_pa_d
	название выходной таблицы: pdout_kv
какой jar-ник нужно дернуть - обсудить с Костиковым К.

Результат пересечь с созданной выше score_keys, создать таблицу score: 

create table t_team_k7m_pa_d.score stored as parquet as
select
		a.exec_id,
		a.okr_id,
		a.obj_id as u7m_id,
		a.key,
		a.value_c,
		a.value_n,
		a.value_d
  from
		t_team_k7m_pa_d.pdout_kv a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
		
*/

/*шаг3 - добавление параметров SKE*/
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_BASE' as key,
		cast(coalesce(a.ske_base, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_base is not NULL

insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_D_0' as key,
		cast(coalesce(a.ske_d_0, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_d_0 is not NULL		

insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'ACTREPORT_FLAG' as key,
		cast(a.actreport_flag as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.actreport_flag is not NULL

insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_DebtCredit0' as key,
		cast(coalesce(a.ske_debtcredit0, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_debtcredit0 is not NULL
		
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_DebtLoan0' as key,
		cast(coalesce(a.ske_debtloan0, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_debtloan0 is not NULL
		
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_DebtLeasing0' as key,
		cast(coalesce(a.ske_debtleasing0, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_debtleasing0 is not NULL
		
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_DebtLoanTransact' as key,
		cast(coalesce(a.ske_dloans, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_dloans is not NULL
		
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
		a.exec_id,
		a.okr_id,
		a.u7m_id,
		'SKE_DebtLeasingTransact' as key,
		cast(coalesce(a.ske_dlising, 0) as decimal(16,4)) as value_n
  from
		t_team_k7m_pa_d.limitout a
		join t_team_k7m_pa_d.score_keys b on (a.u7m_id = b.u7m_id)
 where
		a.ske_dlising is not NULL
		
/*шаг4 - добавление risk_segment*/
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_c)
select
        src.exec_id,
        src.okr_id,
        src.u7m_id,
        'RISK_SEGMENT_OFFLINE' as key,
        rs.risk_segment_offline as value_c
  from
        t_team_k7m_pa_d.score_keys src
        left join t_team_k7m_pa_d.proxyrisksegmentstandalone rs on (src.u7m_id = rs.u7m_id)
 where
		rs.risk_segment_offline is not NULL

/*шаг5 - добавление параметров te*/
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select 
        src.exec_id,
        src.okr_id,
        src.u7m_id,
        'PTE' as key,
        cast(ckp.pte_val as decimal(16, 4)) as value_n
  from
        t_team_k7m_pa_d.score_keys src
        join t_team_k7m_pa_d.clu clu on (clu.u7m_id = src.u7m_id)
        join t_team_k7m_aux_d.amr_country_ckp ckp on (clu.reg_country = ckp.country_cd)
where
        coalesce(lgte_val, -1) between 0 and 1 and
        coalesce(pte_val, -1) between 0 and 1 
union all
select 
        src.exec_id,
        src.okr_id,
        src.u7m_id,
        'LGTE' as key,
        cast(ckp.lgte_val as decimal(16, 4)) as value_n
  from
        t_team_k7m_pa_d.score_keys src
        join t_team_k7m_pa_d.clu clu on (clu.u7m_id = src.u7m_id)
        join t_team_k7m_aux_d.amr_country_ckp ckp on (clu.reg_country = ckp.country_cd)
where
        coalesce(lgte_val, -1) between 0 and 1 and
        coalesce(pte_val, -1) between 0 and 1 

/*шаг6 - добавление RAROC*/
insert into t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
select
        src.exec_id,
        src.okr_id,
        src.u7m_id,
        'K_RWA' as key,
        1 as value_n
  from
        t_team_k7m_pa_d.score_keys src

/*шаг7 - добавление показателей ProxyLGD (LGD_OFFLINE)*/
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'acrecbc_2_qa' as key,
		CAST(a.acrecbc_2_qa as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.acrecbc_2_qa is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'brwisyng_3_sm' as key,
		CAST(a.brwisyng_3_sm as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.brwisyng_3_sm is not NULL;	
		
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'cas2eqr_2_qa' as key,
		CAST(a.cas2eqr_2_qa as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.cas2eqr_2_qa is not NULL;		
		
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'curliqr_2' as key,
		CAST(a.curliqr_2 as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.curliqr_2 is not NULL;		

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'noncur_assets_y' as key,
		CAST(a.noncur_assets_y as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.noncur_assets_y is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'dummy_opkflag' as key,
		CAST(a.dummy_opkflag as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.dummy_opkflag is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'rexp2dbt_QA' as key,
		CAST(a.rexp2dbt_QA as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.rexp2dbt_QA is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'rg2avsbr_sm2_QA' as key,
		CAST(a.rg2avsbr_sm2_QA as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.rg2avsbr_sm2_QA is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'stl2as_2_QA' as key,
		CAST(a.stl2as_2_QA as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.stl2as_2_QA is not NULL;

insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'cr2as_2_QA' as key,
		CAST(a.cr2as_2_QA as DECIMAL(16,4)) as value_n 
  FROM 
		t_team_k7m_pa_d.score_keys src
		join t_team_k7m_aux_d.ProxyLGDin_STG_14_00_CALC_CUST a on a.u7m_id = src.u7m_id
 where
		a.cr2as_2_QA is not NULL;
		
/*шаг 8 - добавление MAX_TRANS_DATE*/
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_d)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'MAX_TRANS_DATE' as key,
		CAST(add_months(/*run_dt дата расчета из etl_exec_stts */, 3) as timestamp) as value_d 
  FROM 
		t_team_k7m_pa_d.score_keys src;

/*шаг 9 - добавление PNPO_FLAG*/
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'PNPO_FLAG' as key,
		CAST(coalesce(ch.check_flg, 0) as DECIMAL(16,4)) as value_n
  FROM 
		t_team_k7m_pa_d.score_keys src
		left join (
			SELECT
					a.u7m_id,
					max(case when a.check_flg = 1 then 1 else 0 end) as check_flg
			  FROM
					t_team_k7m_pa_d.clu_check a
			 WHERE
					a.check_name in ('OVERP_EKS', 'OVERP_CRM')
			 GROUP BY
					a.u7m_id
		) ch on src.u7m_id = ch.u7m_id;

/*шаг 10 - добавление FLAG_OVER_EXIST*/
insert INTO t_team_k7m_pa_d.score (exec_id, okr_id, u7m_id, key, value_n)
SELECT 
		src.exec_id,
		src.okr_id,
		src.u7m_id,
		'FLAG_OVER_EXIST' as key,
		CAST(case when a.check_flg = 1 then 1 else 0 end as DECIMAL(16,4)) as value_n
  FROM 
		t_team_k7m_pa_d.score_keys src
		left join t_team_k7m_pa_d.clu_check a on src.u7m_id = a.u7m_id and a.check_name = 'OVERE';
	