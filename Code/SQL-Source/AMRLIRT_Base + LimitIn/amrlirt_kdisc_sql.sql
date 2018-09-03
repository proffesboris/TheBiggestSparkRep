create table t_team_k7m_aux_p.amr_time_discount as (
select
        cast(replace(min_term, ',', '.') as decimal(38,18)) as min_term,
        cast(replace(max_term, ',', '.') as decimal(38,18)) as max_term,
        cast(replace(kmin, ',', '.') as decimal(38,18)) as kmin,
        cast(replace(kmax, ',', '.') as decimal(38,18)) as kmax
  from
        (
            SELECT 
                    mtr.name,
                    max(case when mtc.name = 'MaxTerm' then mtv.value end) as max_term,
                    max(case when mtc.name = 'MinTerm' then mtv.value end) as min_term,
                    max(case when mtc.name = 'Kmax' then mtv.value end) as kmax,
                    max(case when mtc.name = 'Kmin' then mtv.value end) as kmin
              FROM 
                    t_team_k7m_stg.amr_model_tables mt
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtc on (mtc.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtr on (mtr.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_models_tables_values mtv on (mtc.mt_row_column_id = mtv.mtv_id_col and mtr.mt_row_column_id = mtv.mtv_id_row)
                    JOIN t_team_k7m_stg.amr_data_types dt on dt.data_type_id = mtv.data_type_id
             where
                    mt.code = 'CommonTBL_KTimeDisc' and
                    mt.is_active = 'Y' and
                    mt.model_id is NULL   
             group by
                    mtr.name  
        )
);
