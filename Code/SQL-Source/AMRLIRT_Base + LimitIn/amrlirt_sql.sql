-- step1. AUX
create table t_team_k7m_aux_p.amr_calc_req as (
select
        org_calc.final_request_id as final_rqst_id,
        org_calc.final_request_dt as final_rqst_dttm,
        org_calc.model_cd,
        org_calc.rating_id as crm_rating_card_id,
        org_calc.calc_request_id as calc_rqst_id,
        rate.code as cust_rating_cd,
        org_calc.crm_cust_id,
        org_calc.org_name as crm_cust_name,
        org_calc.org_inn as cust_inn_num,
        org_calc.org_kpp as cust_kpp_num,
        max(to_date(rd.report_date, 'dd.mm.yyyy')) as rqst_rep_dt,
        max(to_date(rd.start_date_str, 'dd.mm.yyyy')) as rqst_rep_start_dt,
        max(to_date(rd.end_date_str, 'dd.mm.yyyy')) as rqst_rep_end_dt,
		max(case when lower(mp.code) = 'asset_q1' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as asset_q1_amt, 		 
        max(case when lower(mp.code) = 'rev_q1' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as rev_q1_amt,
        max(case when lower(mp.code) = 'rev_q2' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as rev_q2_amt,
        max(case when lower(mp.code) = 'rev_y' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as rev_y_amt,
        max(case when lower(mp.code) = 'calc_ebitda' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as calc_ebitda_amt,
        max(case when lower(mp.code) = 'short_debt_q1' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as short_debt_q1_amt,
        max(case when lower(mp.code) = 'long_debt_q1' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as long_debt_q1_amt,
        max(case when lower(mp.code) = 'cor_debt_q1' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as cor_debt_q1_amt,
        max(case when lower(mp.code) = 'short_debt_q2' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as short_debt_q2_amt,
        max(case when lower(mp.code) = 'long_debt_q2' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as long_debt_q2_amt,
        max(case when lower(mp.code) = 'cor_debt_q2' then cast(replace(sd.parameter_value, ',', '.') as decimal(16,4)) end) as cor_debt_q2_amt, 
        max(case when lower(mp.code) = 'model_type' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS model_type_cd,
        max(case when lower(mp.code) = 'ql1' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql1_val,
        max(case when lower(mp.code) = 'ql2' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql2_val,
        max(case when lower(mp.code) = 'ql3' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql3_val,
        max(case when lower(mp.code) = 'ql4' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql4_val,
        max(case when lower(mp.code) = 'ql5' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql5_val,
        max(case when lower(mp.code) = 'ql6' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql6_val,
        max(case when lower(mp.code) = 'ql7' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql7_val,
        max(case when lower(mp.code) = 'ql8' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql8_val,
        max(case when lower(mp.code) = 'ql9' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql9_val,
        max(case when lower(mp.code) = 'ql10' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql10_val,
        max(case when lower(mp.code) = 'ql11' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql11_val,
        max(case when lower(mp.code) = 'ql12' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql12_val,
        max(case when lower(mp.code) = 'ql13' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql13_val,
        max(case when lower(mp.code) = 'ql14' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql14_val,
        max(case when lower(mp.code) = 'ql15' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql15_val,
        max(case when lower(mp.code) = 'ql16' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql16_val,
        max(case when lower(mp.code) = 'ql17' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql17_val,
        max(case when lower(mp.code) = 'ql18' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql18_val,
        max(case when lower(mp.code) = 'ql19' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql19_val,
        max(case when lower(mp.code) = 'ql20' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql20_val,
        max(case when lower(mp.code) = 'ql21' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql21_val,
        max(case when lower(mp.code) = 'ql22' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql22_val,
        max(case when lower(mp.code) = 'ql23' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql23_val,
        max(case when lower(mp.code) = 'ql24' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql24_val,
        max(case when lower(mp.code) = 'ql25' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql25_val,
        max(case when lower(mp.code) = 'ql26' then coalesce(rd.value_integration_uid, sd.parameter_value) end) AS ql26_val,
        max(case when lower(mp.code) = 'ql11_score' then sd.parameter_value end) as ql11_score_val,
        max(case when lower(mp.code) = 'ql13_score' then sd.parameter_value end) as ql13_score_val,
        max(case when lower(mp.code) = 'ql15_score' then sd.parameter_value end) as ql15_score_val,
        max(case when lower(mp.code) = 'ql16_score' then sd.parameter_value end) as ql16_score_val,
        max(case when lower(mp.code) = 'ql3_score' then sd.parameter_value end) as ql3_score_val,
        max(case when lower(mp.code) = 'ql5_score' then sd.parameter_value end) as ql5_score_val,
        max(case when lower(mp.code) = 'ql9_score' then sd.parameter_value end) as ql9_score_val,
        max(case when lower(mp.code) = 'woe_ql15' then sd.parameter_value end) as woe_ql15_val,
        max(case when lower(mp.code) = 'woe_ql16' then sd.parameter_value end) as woe_ql16_val,
        max(case when lower(mp.code) = 'woe_ql5' then sd.parameter_value end) as woe_ql5_val,
        max(case when lower(mp.code) = 'woe_ql7' then sd.parameter_value end) as woe_ql7_val,
        max(case when lower(mp.code) = 'woe_ql9' then sd.parameter_value end) as woe_ql9_val,
        max(case when lower(mp.code) = 'ws1' or upper(mp.code) like 'DT%_WS1' then sd.parameter_value end) as ws1_val,
        max(case when lower(mp.code) = 'ws2' or upper(mp.code) like 'DT%_WS2' then sd.parameter_value end) as ws2_val,
        max(case when lower(mp.code) = 'ws3' or upper(mp.code) like 'DT%_WS3' then sd.parameter_value end) as ws3_val,
        max(case when lower(mp.code) = 'ws4' or upper(mp.code) like 'DT%_WS4' then sd.parameter_value end) as ws4_val,
        max(case when lower(mp.code) = 'ws5' or upper(mp.code) like 'DT%_WS5' then sd.parameter_value end) as ws5_val,
        max(case when lower(mp.code) = 'ws6' or upper(mp.code) like 'DT%_WS6' then sd.parameter_value end) as ws6_val,
        max(case when lower(mp.code) = 'ws7' or upper(mp.code) like 'DT%_WS7' then sd.parameter_value end) as ws7_val,
        max(case when lower(mp.code) = 'ws7_1' or upper(mp.code) like 'DT%_WS7_1' then sd.parameter_value end) as ws7_1_val,
        max(case when lower(mp.code) = 'ws8' or upper(mp.code) like 'DT%_WS8' then sd.parameter_value end) as ws8_val,
        max(case when lower(mp.code) = 'ws9' or upper(mp.code) like 'DT%_WS9'then sd.parameter_value end) as ws9_val,
        max(case when lower(mp.code) = 'ws10' or upper(mp.code) like 'DT%_WS10' then sd.parameter_value end) as ws10_val,
        max(case when lower(mp.code) = 'ws10_1' or upper(mp.code) like 'DT%_WS10_1' then sd.parameter_value end) as ws10_1_val,
        max(case when lower(mp.code) = 'ws11' or upper(mp.code) like 'DT%_WS11' then sd.parameter_value end) as ws11_val,
        max(case when lower(mp.code) = 'ws12' or upper(mp.code) like 'DT%_WS12' then sd.parameter_value end) as ws12_val,
        max(case when lower(mp.code) = 'ws13' or upper(mp.code) like 'DT%_WS13' then sd.parameter_value end) as ws13_val,
        max(case when lower(mp.code) = 'ws13_1' or upper(mp.code) like 'DT%_WS13_1' then sd.parameter_value end) as ws13_1_val,
        max(case when lower(mp.code) = 'ws14' or upper(mp.code) like 'DT%_WS14' then sd.parameter_value end) as ws14_val,
        max(case when lower(mp.code) = 'ws14_1' or upper(mp.code) like 'DT%_WS14_1' then sd.parameter_value end) as ws14_1_val,
        max(case when lower(mp.code) = 'ws15' or upper(mp.code) like 'DT%_WS15' then sd.parameter_value end) as ws15_val,
        max(case when lower(mp.code) = 'ws16' or upper(mp.code) like 'DT%_WS16' then sd.parameter_value end) as ws16_val,
        max(case when lower(mp.code) = 'ws17' or upper(mp.code) like 'DT%_WS17' then sd.parameter_value end) as ws17_val,
        max(case when lower(mp.code) = 'ws18' or upper(mp.code) like 'DT%_WS18' then sd.parameter_value end) as ws18_val,
        max(case when lower(mp.code) = 'ws18_all' or upper(mp.code) like 'DT%_WS18_ALL' then sd.parameter_value end) as ws18_all_val,
        max(case when lower(mp.code) = 'ws19' or upper(mp.code) like 'DT%_WS19' then sd.parameter_value end) as ws19_val,
        max(case when lower(mp.code) = 'ws20' or upper(mp.code) like 'DT%_WS20' then sd.parameter_value end) as ws20_val,
        max(case when lower(mp.code) = 'ws21' or upper(mp.code) like 'DT%_WS21' then sd.parameter_value end) as ws21_val,
        max(case when lower(mp.code) = 'ws22' or upper(mp.code) like 'DT%_WS22' then sd.parameter_value end) as ws22_val,
        max(case when lower(mp.code) = 'ws23' or upper(mp.code) like 'DT%_WS23' then sd.parameter_value end) as ws23_val,
        max(case when lower(mp.code) = 'ws24' or upper(mp.code) like 'DT%_WS24'then sd.parameter_value end) as ws24_val,
        max(case when lower(mp.code) = 'ws25' or upper(mp.code) like 'DT%_WS25' then sd.parameter_value end) as ws25_val,
        max(case when lower(mp.code) = 'ws26' or upper(mp.code) like 'DT%_WS26' then sd.parameter_value end) as ws26_val,
        max(case when lower(mp.code) = 'ws27' or upper(mp.code) like 'DT%_WS27' then sd.parameter_value end) as ws27_val,
        max(case when lower(mp.code) = 'ws28' or upper(mp.code) like 'DT%_WS28' then sd.parameter_value end) as ws28_val,
        max(case when lower(mp.code) = 'ws29' or upper(mp.code) like 'DT%_WS29' then sd.parameter_value end) as ws29_val,
        max(case when lower(mp.code) = 'ws30' or upper(mp.code) like 'DT%_WS30' then sd.parameter_value end) as ws30_val,
        max(case when lower(mp.code) = 'ws31' or upper(mp.code) like 'DT%_WS31' then sd.parameter_value end) as ws31_val,
        max(case when lower(mp.code) = 'ws32' or upper(mp.code) like 'DT%_WS32' then sd.parameter_value end) as ws32_val,
        max(case when lower(mp.code) = 'ws33' or upper(mp.code) like 'DT%_WS33' then sd.parameter_value end) as ws33_val,
        max(case when lower(mp.code) = 'ws34' or upper(mp.code) like 'DT%_WS34' then sd.parameter_value end) as ws34_val,
        max(case when lower(mp.code) = 'ws35' or upper(mp.code) like 'DT%_WS35' then sd.parameter_value end) as ws35_val,
        max(case when lower(mp.code) = 'ws36' or upper(mp.code) like 'DT%_WS36' then sd.parameter_value end) as ws36_val,
        max(case when lower(mp.code) = 'ws37' or upper(mp.code) like 'DT%_WS37' then sd.parameter_value end) as ws37_val,
        max(case when lower(mp.code) = 'ws38' or upper(mp.code) like 'DT%_WS38' then sd.parameter_value end) as ws38_val,
        max(case when lower(mp.code) = 'ws39' or upper(mp.code) like 'DT%_WS39' then sd.parameter_value end) as ws39_val,
        max(case when lower(mp.code) = 'ws40' or upper(mp.code) like 'DT%_WS40' then sd.parameter_value end) as ws40_val,
        max(case when lower(mp.code) = 'ws41' or upper(mp.code) like 'DT%_WS41' then sd.parameter_value end) as ws41_val,
        max(case when lower(mp.code) = 'ws42' or upper(mp.code) like 'DT%_WS42' then sd.parameter_value end) as ws42_val,
        max(case when lower(mp.code) = 'ws43' or upper(mp.code) like 'DT%_WS43' then sd.parameter_value end) as ws43_val,
        max(case when lower(mp.code) = 'ws44' or upper(mp.code) like 'DT%_WS44' then sd.parameter_value end) as ws44_val,
        max(case when upper(mp.code) = 'DT0700_1' then sd.parameter_value end) as dt0700_1_val,
        max(case when upper(mp.code) = 'DT0700_10' then sd.parameter_value end) as dt0700_10_val,
        max(case when upper(mp.code) = 'DT0700_2' then sd.parameter_value end) as dt0700_2_val,
        max(case when upper(mp.code) = 'DT0700_3' then sd.parameter_value end) as dt0700_3_val,
        max(case when upper(mp.code) = 'DT0700_4' then sd.parameter_value end) as dt0700_4_val,
        max(case when upper(mp.code) = 'DT0700_5' then sd.parameter_value end) as dt0700_5_val,
        max(case when upper(mp.code) = 'DT0700_6' then sd.parameter_value end) as dt0700_6_val,
        max(case when upper(mp.code) = 'DT0700_7' then sd.parameter_value end) as dt0700_7_val,
        max(case when upper(mp.code) = 'DT0700_8' then sd.parameter_value end) as dt0700_8_val,
        max(case when upper(mp.code) = 'DT0700_9' then sd.parameter_value end) as dt0700_9_val,
        max(case when upper(mp.code) = 'DT_1200' then sd.parameter_value end) as dt_1200_val,
        max(case when upper(mp.code) = 'DT0710' then sd.parameter_value end) as dt0710_val,
        max(case when upper(mp.code) = 'DT0100' then sd.parameter_value end) as dt0100_val,
        max(case when upper(mp.code) = 'DT0200' then sd.parameter_value end) as dt0200_val,
        max(case when upper(mp.code) = 'DT0300' then sd.parameter_value end) as dt0300_val,
        max(case when upper(mp.code) = 'DT0400' then sd.parameter_value end) as dt0400_val,
        max(case when upper(mp.code) = 'DT0500' then sd.parameter_value end) as dt0500_val,
        max(case when upper(mp.code) = 'DT0600' then sd.parameter_value end) as dt0600_val,
        max(case when upper(mp.code) = 'DT0800' then sd.parameter_value end) as dt0800_val,
        max(case when upper(mp.code) = 'DT0900' then sd.parameter_value end) as dt0900_val,
        max(case when upper(mp.code) = 'DT1000' then sd.parameter_value end) as dt1000_val,
        max(case when upper(mp.code) = 'DT1100' then sd.parameter_value end) as dt1100_val,
        max(case when upper(mp.code) = 'DT0700' then sd.parameter_value end) as dt0700_val,
        max(case when upper(mp.code) = 'DT1200' then sd.parameter_value end) as dt1200_val,
        max(case when lower(mp.code) = 'rating_price' then sd.parameter_value end) as rating_price_cd,
        max(case when lower(mp.code) = 'rating_rezerv' then sd.parameter_value end) as rating_rezerv_cd,
        max(case when lower(mp.code) = 'rgg' then sd.parameter_value end) as rgg_val,
        max(case when lower(mp.code) = 'rgg_price' then sd.parameter_value end) as rgg_price_val,
        max(case when lower(mp.code) = 'rw' then sd.parameter_value end) as rw_val,
        max(case when lower(mp.code) = 'rw_price' then sd.parameter_value end) as rw_price_val,
        max(case when lower(mp.code) = 'tsqual' then sd.parameter_value end) as tsqual_val,
        max(case when lower(mp.code) = 'tsquant' then sd.parameter_value end) as tsquant_val,
        max(case when lower(mp.code) = 'gr_ext_rating' then sd.parameter_value end) as gr_ext_rating_val,
        max(case when lower(mp.code) = 'gr_int_rating' then sd.parameter_value end) as gr_int_rating_val,
        max(case when lower(mp.code) = 'gr_int_rating_price' then sd.parameter_value end) as gr_int_rating_price_val,
		max(case when lower(mp.code) = 'q101b_rev' then sd.parameter_value end) as q101b_rev_val
  from
        (
            select
                    fin_calc.final_request_id,
                    fin_calc.final_request_dt,
                    fin_calc.model_cd,
                    fin_calc.rating_id,
                    fin_calc.calc_request_id,
                    org.inn as org_inn,
                    org.kpp as org_kpp,
                    CASE WHEN calc_req.organization_id is NULL THEN cbr.code ELSE org.code END as crm_cust_id,
                    CASE WHEN calc_req.organization_id is NULL THEN upper(cbr.name) ELSE upper(org.name) END as org_name,
                    row_number() over (partition by CASE WHEN calc_req.organization_id is NULL THEN cbr.code ELSE org.code END order by fin_calc.final_request_dt desc) rn
              from
                    (
                        SELECT  
                                fin_req.request_id as final_request_id,
                                fin_req.request_dt as final_request_dt,
                                fin_req.model_integration_uid as model_cd,
                                fin_req.rating_identifier as rating_id,
                                MAX(req.request_id) as calc_request_id
                          FROM         
                                t_team_k7m_stg.amr_requests fin_req 
                                INNER JOIN t_team_k7m_stg.amr_requestors reqr ON (fin_req.requestor_id = reqr.requestor_id)
                                INNER JOIN t_team_k7m_stg.amr_requests req ON (fin_req.rating_identifier = req.rating_identifier)
                         WHERE 
                                fin_req.rating_identifier is not NULL AND
                                fin_req.request_type_id = 5 AND 
                                fin_req.is_process_failed = 'N' AND 
                                req.request_type_id IN (2, 3) AND 
                                req.is_process_failed = 'N' AND 
                                req.request_dt <= fin_req.request_dt 									
                         GROUP BY  
                                fin_req.request_id,
                                fin_req.request_dt,
                                fin_req.model_integration_uid,
                                fin_req.rating_identifier
                    ) fin_calc
                    join t_team_k7m_stg.amr_requests calc_req on (fin_calc.calc_request_id = calc_req.request_id)
                    left join t_team_k7m_stg.amr_organizations org on (calc_req.organization_id = org.organization_id)
                    left join t_team_k7m_stg.amr_coborrower_groups cbr on (calc_req.coborrower_group_id = cbr.coborrower_group_id)
             where
                    calc_req.organization_id is not NULL or
                    calc_req.coborrower_group_id is not NULL
        ) org_calc
        JOIN t_team_k7m_stg.amr_pd_rating_final pdf ON pdf.request_id = org_calc.final_request_id
        JOIN t_team_k7m_stg.amr_ratings rate ON pdf.rating_id = rate.rating_id
        JOIN t_team_k7m_stg.amr_stage_data sd ON org_calc.calc_request_id = sd.request_id and sd.stage_id = 3
        JOIN t_team_k7m_stg.amr_model_parameters mp on sd.model_parameter_id = mp.model_parameter_id
        LEFT JOIN t_team_k7m_stg.amr_request_data rd ON sd.model_parameter_id = rd.model_parameter_id AND sd.request_id = rd.request_id
 where
        org_calc.rn = 1
 group by
		org_calc.final_request_id,
        org_calc.final_request_dt,
        org_calc.model_cd,
        org_calc.rating_id,
        org_calc.calc_request_id,
        rate.code,
        org_calc.crm_cust_id,
        org_calc.org_name,
        org_calc.org_inn,
        org_calc.org_kpp
);

-- step2. PA
create table t_team_k7m_pa_d.limitin as (
select 
        clu.u7m_id,
		a.*,
        exp(a.q101b_rev_val) as rev_amt,
        case 
            when a.gr_int_rating_val is NULL or a.gr_int_rating_val in ('0', '-1000', '0,0') then ''
            else a.gr_int_rating_val   
        end as gr_int_rating_s_val,
        b.fin_leas_debt_amt,
        b.fin_loan_debt_amt,
        b.fin_crd_debt_amt
  from 
        t_team_k7m_aux_d.amr_calc_req a 
		inner join t_team_k7m_pa_p.clu clu on (clu.crm_id = a.crm_cust_id)
		left join t_team_k7m_aux_d.fok_fin_stmt_debt b on (a.crm_cust_id = b.crm_cust_id and a.rqst_rep_dt = b.fin_stmt_start_dt)
 where
 		(a.model_cd in ('CC01', 'CC01_M_A', 'CC01_price_ttc', 'CC02', 'CC02_price_ttc', 'CC03', 'CC03_price_ttc', 'CC04', 'CC04_price_ttc', 'CC05', 'CC06', 'CC09', 'CC10', 'CC11', 'CC11_price_ttc',
 		                'CC12', 'CC12_price_ttc', 'CC13', 'CC13_price_ttc', 'CC14_price_ttc', 'CC_IFRS', 'CC_RAS', 'CC_RAS_v2', 'CC_RAS_v3', 'CC_RAS_v4', 'CC_RAS_v5', 'PD_CORP_CC_1_0117_1_0117_OPK', 
 		                'PD_CORP_CC_1_0117_1_0117_R', 'PD_CORP_CC_1_0117_1_0117_R_v2', 'PD_CORP_CC_1_0117_1_0117_R_v3', 'PD_CORP_CC_1_0117_2_0817_OPK_1', 'PD_CORP_CC_1_0117_2_0817_R_1') or 
 		 upper(a.model_cd) like 'PD_CORP_CC%' 
 		) and 
 		a.model_type_cd not in ('CC_OPK_REORG','CC_REORG')
);