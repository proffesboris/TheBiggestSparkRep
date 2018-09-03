drop table t_team_k7m_aux_d.dict_mandatory_keys_out;
create table t_team_k7m_aux_d.dict_mandatory_keys_out (
obj string,
block_nm string,
key string
)
stored as parquet;


INSERT INTO t_team_k7m_aux_d.dict_mandatory_keys_out
select 'BO' obj , 'BO' block_nm, 'DUR_L' key union all 
select 'BO' obj , 'BO' block_nm, 'DUR_U' key union all 
select 'BO' obj , 'BO' block_nm, 'EXP_L' key union all 
select 'BO' obj , 'BO' block_nm, 'EXP_U' key union all 
select 'BO' obj , 'BO' block_nm, 'CREDIT_MODE' key union all 
select 'BO' obj , 'BO' block_nm, 'AVP_L' key union all 
select 'BO' obj , 'BO' block_nm, 'AVP_U' key union all 
select 'BO' obj , 'BO' block_nm, 'PRIV_L' key union all 
select 'BO' obj , 'BO' block_nm, 'PRIV_U' key union all 
select 'BO' obj , 'BO' block_nm, 'BALLOON_L' key union all 
select 'BO' obj , 'BO' block_nm, 'BALLOON_U' key union all 
select 'BO' obj , 'BO' block_nm, 'PD_L' key union all 
select 'BO' obj , 'BO' block_nm, 'PD_U' key union all 
select 'BO' obj , 'BO' block_nm, 'LGD_L' key union all 
select 'BO' obj , 'BO' block_nm, 'LGD_U' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'ANetCCO_to_EAD' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'ufn_flag' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'brtclwner3_ok_1_nd' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'cnedvokr_nd' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'collosn5' key union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'maxcolgivrate_osn_1_nd' key union all 
---select 'BO' obj , 'SKR_OFFLINE' block_nm, 'TURNOVER_VOL' key union all  РЕШЕНО НЕ СЧИТАТЬ ОБЯЗАТЕЛЬНЫМ 16.05.2018
select 'BO' obj , 'SKR_OFFLINE' block_nm, 'ELTE' key union all 
select 'BO' obj , 'SKR_OFFLINE' block_nm, 'K_DISCOUNT' key union all 
select 'BO' obj , 'SET' block_nm, 'SET_ID' key union all 
select 'BO' obj , 'SKR_OFFLINE' block_nm, 'ELp_COEF' key  union all 
select 'BO' obj , 'LGD_OFFLINE' block_nm, 'das2dbt_3_QA_OFFLINE' key ; --Добавили 18.05.2018


INSERT INTO t_team_k7m_aux_d.dict_mandatory_keys_out
select 'SCORE' obj , 'RISK_SEGMENT_OFFLINE' block_nm, 'RISK_SEGMENT_OFFLINE' key union all 
select 'SCORE' obj , 'PD_OFFLINE' block_nm, 'RATING_OFFLINE' key union all 
select 'SCORE' obj , 'PD_OFFLINE' block_nm, 'RATING_OFFLINE_PRICE' key union all 
select 'SCORE' obj , 'PD_OFFLINE' block_nm, 'RATING_OFFLINE_REZERV' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'acrecbc_2_qa' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'brwisyng_3_sm' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'cas2eqr_2_qa' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'curliqr_2' key union all 
--select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'maxcolgivrate_osn_1_nd' key union all  РЕШЕНО НЕ СЧИТАТЬ ОБЯЗАТЕЛЬНЫМ 17.05.2018
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'noncur_assets_y' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'dummy_opkflag' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'rexp2dbt_QA' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'rg2avsbr_sm2_QA' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'stl2as_2_QA' key union all 
select 'SCORE' obj , 'LGD_OFFLINE' block_nm, 'cr2as_2_QA' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'SKE_BASE' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'SKE_D_0' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'ACTREPORT_FLAG' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'SKE_DebtCredit0' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'SKE_DebtLoanTransact' key union all 
select 'SCORE' obj , 'SKE_OFFLINE' block_nm, 'SKE_DebtLeasingTransact' key;
