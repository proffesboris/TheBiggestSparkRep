create table t_team_k7m_aux_p.amr_country_ckp as (
select
        country_cd,
        cast(replace(lgte_val, ',', '.') as decimal(38,18)) as lgte_val,
        cast(replace(pte_val, ',', '.') as decimal(38,18)) as pte_val,
        currency_cd
  from
        (
            SELECT   
                    mtr.name,
                    max(case when mtc.name = 'CountryCode' then mtv.value end) as country_cd,
                    max(case when mtc.name = 'LGTEValue' then mtv.value end) as lgte_val,
                    max(case when mtc.name = 'PTEValue' then mtv.value end) as pte_val,
                    max(case when mtc.name = 'CurrencyCode' then mtv.value end) as currency_cd
              FROM 
                    t_team_k7m_stg.amr_model_tables mt
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtc on (mtc.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_mt_rows_columns mtr on (mtr.model_table_id = mt.model_table_id)
                    JOIN t_team_k7m_stg.amr_models_tables_values mtv on (mtc.mt_row_column_id = mtv.mtv_id_col and mtr.mt_row_column_id = mtv.mtv_id_row)
                    JOIN t_team_k7m_stg.amr_data_types dt on dt.data_type_id = mtv.data_type_id
             where
                    mt.code = 'CommonTBL_CountryCKP' and
                    mt.is_active = 'Y' and
                    mt.model_id is NULL    
             group by
                    mtr.name  
        )
 where
        country_cd is not NULL
);
