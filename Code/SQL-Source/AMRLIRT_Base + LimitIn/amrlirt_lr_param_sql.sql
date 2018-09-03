create table t_team_k7m_aux_p.amr_lr_param as (
select
        param_name,
        start_dt,
        end_dt,
        cast(replace(param_val, ',', '.') as decimal(38,18)) as param_val
  from
        (
            SELECT 
                    mtr.name,
                    max(case when mtc.name = 'Name' then mtv.value end) as param_name,
                    max(case when mtc.name = 'StartDate' then mtv.value end) as start_dt,
                    max(case when mtc.name = 'EndDate' then mtv.value end) as end_dt,
                    max(case when mtc.name = 'Value' then mtv.value end) as param_val
              FROM 
                    t_team_k7m_stg.amr_model_tables mt
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtc on (mtc.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtr on (mtr.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_models_tables_values mtv on (mtc.mt_row_column_id = mtv.mtv_id_col and mtr.mt_row_column_id = mtv.mtv_id_row)
                    JOIN t_team_k7m_stg.amr_data_types dt on dt.data_type_id = mtv.data_type_id
             where
                    mt.code = 'CommonTBL_LR2017_Parameters' and
                    mt.is_active = 'Y' and
                    mt.model_id is NULL   
             group by
                    mtr.name  
        )
);