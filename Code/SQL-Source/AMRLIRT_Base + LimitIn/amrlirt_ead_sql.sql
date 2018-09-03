create table t_team_k7m_aux_p.amr_ead_prd as (
select
        prd_name,
        prd_stts,
        prd_lim,
        replace(ccfp_val, ',', '.') as ccfp_val,
        replace(ccfp_other_val, ',', '.') as ccfp_other_val,
        replace(ccfm_val, ',', '.') as ccfm_val,
        replace(ccfm_dt_val, ',', '.') as ccfm_dt_val 
  from
        (
            SELECT   
                    mtr.name,
                    max(case when mtc.name = 'ProductName' then mtv.value end) as prd_name,
                    max(case when mtc.name = 'Product_Status' then mtv.value end) as prd_stts,
                    max(case when mtc.name = 'ProductLimit' then mtv.value end) as prd_lim,
                    max(case when mtc.name = 'CCFp' then mtv.value end) as ccfp_val,
                    max(case when mtc.name = 'CCFp_other' then mtv.value end) as ccfp_other_val,
                    max(case when mtc.name = 'CCFm' then mtv.value end) as ccfm_val,
                    max(case when mtc.name = 'CCFm_dt' then mtv.value end) as ccfm_dt_val
              FROM 
                    t_team_k7m_stg.amr_model_tables mt
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtc on (mtc.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtr on (mtr.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_models_tables_values mtv on (mtc.mt_row_column_id = mtv.mtv_id_col and mtr.mt_row_column_id = mtv.mtv_id_row)
                    JOIN t_team_k7m_stg.amr_data_types dt on dt.data_type_id = mtv.data_type_id
             where
                    mt.code = 'CommonTBL_Alpha_factor_ead' and
                    mt.is_active = 'Y' and
                    mt.model_id is NULL    
             group by
                    mtr.name  
        )
);

