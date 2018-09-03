-- запросы написаны над 0 слоем ЛД
-- необходимо его создать, если он отсутствует из таблиц ФОК docs_header, docs_data

-- fok_step_1. Отбор нужных заголовков
create table t_team_k7m_aux_p.fok_docs_header_final_loan as (
select 
        dh.id,
        dh.divid,
        dh.period,
        dh.year,
        max(dd.ch) as fin_stmt_meas_cd
  from 
        t_team_k7m_stg.fok_docs_header dh
        join (
            select  
                    id,
                    -- сортировка для каждого id клиента и периода 1) сначала по статусу: ищем среди подтвержденных, если их нет - среди остальных
                    -- 2) затем по дате изменения: отбираем среди найденных самый новый заголовок
                    row_number() over (partition by formid, divid, year, period order by case when docs_status = 9 then 1 else 0 end desc, modify_date desc) rn
              from
                    t_team_k7m_stg.fok_docs_header
             where
                    formid in ('SB0160')
        ) mv on (dh.id = mv.id and mv.rn = 1) -- соединение по pk, чтобы избежать размножения
        join t_team_k7m_stg.fok_docs_data dd on (dh.id = dd.docs_header_id and dd.field_name = 'MEASURE' and dd.ch in ('млн.', 'тыс.'))
 where
        dh.formid in ('SB0160') 
 group by
        dh.id,
        dh.divid,
        dh.period,
        dh.year        
);

-- step 2. Выбор задолженностей по нужным продуктам и транспонирование    
create table t_team_k7m_aux_p.fok_fin_stmt_detail as (
select
        docs_header_id,
        sum(case when prd = 'Финансовый лизинг' then amount else 0 end) as leas_amt,
        sum(case when prd = 'Займ' then amount else 0 end) as loan_amt,
        sum(case when prd = 'Кредит' then amount else 0 end) as crd_amt
  from
        (
            select 
                    docs_header_id, 
                    copy_path,
                    sum(coalesce(nm, 0)) as amount, 
                    max(ch) as prd
              from 
                    t_team_k7m_stg.fok_docs_data
             where 
                    field_name in ('DN_4.01.3', 'DN_4.01.5', 'DN_1.01.3', 'DN_1.01.5')
             group by docs_header_id, 
                    copy_path
        )
 group by 
        docs_header_id
);   

-- step 3. Сборка финального агрегата для витрины LimitIn.
-- НУЖНО МАТЕРИАЛИЗОВАТЬ В СТЕЙДЖЕ (AUX) 
create table t_team_k7m_aux_p.fok_fin_stmt_debt as (
select
        crm_cust_id,
        fin_stmt_year,
        fin_stmt_period,
        fin_stmt_start_dt,
        case when fin_stmt_meas_cd = 'млн.' then fin_leas_debt_amt * 1000 else fin_leas_debt_amt end as fin_leas_debt_amt,
        case when fin_stmt_meas_cd = 'млн.' then fin_loan_debt_amt * 1000 else fin_loan_debt_amt end as fin_loan_debt_amt,
        case when fin_stmt_meas_cd = 'млн.' then fin_crd_debt_amt * 1000 else fin_crd_debt_amt end as fin_crd_debt_amt
  from
        (
            select
                    dh.divid as crm_cust_id,
                    dh.year as fin_stmt_year,
                    dh.period as fin_stmt_period,
                    dh.fin_stmt_meas_cd,
                    case
                        when dh.period in (1,2,3,4) and year > 1900 then case 
                                                                        when dh.period = 1 then to_date(concat(year,'-01-01'))
                                                                        when dh.period = 2 then to_date(concat(year,'-04-01'))
                                                                        when dh.period = 3 then to_date(concat(year,'-07-01'))
                                                                        when dh.period = 4 then to_date(concat(year,'-10-01'))
                                                                    end
                        else null
                    end as fin_stmt_start_dt,
                    dd.leas_amt as fin_leas_debt_amt,
                    dd.loan_amt as fin_loan_debt_amt,
                    dd.crd_amt as fin_crd_debt_amt
              from
                    t_team_k7m_aux_p.fok_docs_header_final_loan dh
                    join t_team_k7m_aux_p.fok_fin_stmt_detail dd on (dh.id = dd.docs_header_id) 
        )
 where
        fin_stmt_start_dt < unix_timestamp() and
        fin_stmt_start_dt is not NULL and
        fin_leas_debt_amt + fin_loan_debt_amt + fin_crd_debt_amt > 0
); 