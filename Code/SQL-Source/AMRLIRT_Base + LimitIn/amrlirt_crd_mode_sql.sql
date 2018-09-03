create table t_team_k7m_aux_p.amr_crd_mode as (
select
        crd_mode_cd,
        crd_risk_flag,
        type_cd,
        loan_sort_num,
        cast(replace(ccfm_val, ',', '.') as decimal(38,18)) as ccfm_val,
        cast(replace(ccfm_dt_val, ',', '.') as decimal(38,18)) as ccfm_dt_val,
        cast(replace(ki_val, ',', '.') as decimal(38,18)) as ki_val,
        cast(replace(kf_val, ',', '.') as decimal(38,18)) as kf_val
  from
        (
            SELECT 
                    mtr.name,
                    max(case when mtc.name = 'CreditMode' then mtv.value end) as crd_mode_cd,
                    max(case when mtc.name = 'CreditRiskFlag' then mtv.value end) as crd_risk_flag,
                    max(case when mtc.name = 'Type' then mtv.value end) as type_cd,
                    max(case when mtc.name = 'LoanSort' then mtv.value end) as loan_sort_num,
                    max(case when mtc.name = 'CCFm' then mtv.value end) as ccfm_val,
                    max(case when mtc.name = 'CCFm_dt' then mtv.value end) as ccfm_dt_val,
                    max(case when mtc.name = 'Ki' then mtv.value end) as ki_val,
                    max(case when mtc.name = 'Kf' then mtv.value end) as kf_val
              FROM 
                    t_team_k7m_stg.amr_model_tables mt
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtc on (mtc.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtr on (mtr.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_models_tables_values mtv on (mtc.mt_row_column_id = mtv.mtv_id_col and mtr.mt_row_column_id = mtv.mtv_id_row)
                    JOIN t_team_k7m_stg.amr_data_types dt on dt.data_type_id = mtv.data_type_id
             where
                    mt.code = 'CommonTBL_CreditModeDecode' and
                    mt.is_active = 'Y' and
                    mt.model_id is NULL 
             group by
                    mtr.name  
        )
);


      


