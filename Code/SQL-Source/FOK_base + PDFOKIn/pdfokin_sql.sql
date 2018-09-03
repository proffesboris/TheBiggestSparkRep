-- запросы написаны над 0 слоем ЛД
-- необходимо его создать, если он отсутствует из таблиц ФОК docs_header, docs_data

-- fok_step_1. Отбор нужных заголовков
drop table if exists t_team_k7m_aux_d.fok_docs_header_final_rsbu
create table t_team_k7m_aux_d.fok_docs_header_final_rsbu as
select 
        dh.*
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
                    formid in ('SB0121')
        ) mv on (dh.id = mv.id and mv.rn = 1) -- соединение по pk, чтобы избежать размножения
 where
        dh.formid in ('SB0121') and
        exists ( -- есть адекватное обозначение ед. измерения
                    select 
                            1
                      from
                            t_team_k7m_stg.fok_docs_data dd 
                     where
                            dh.id = dd.docs_header_id and 
                            dd.field_name = 'MEASURE' and 
                            dd.ch is not null and
                            dd.ch in ('млн.', 'тыс.')       
        );
        
-- step 2. РСБУ. Выбор всех нужных показателей по отобранным заголовкам и транспонирование  
-- НУЖНО МАТЕРИАЛИЗОВАТЬ В СТЕЙДЖЕ (AUX)  
drop table if exists t_team_k7m_aux_d.fok_fin_stmt_rsbu    
create table t_team_k7m_aux_d.fok_fin_stmt_rsbu as 
select
        crm_cust_id,
        fin_stmt_year,
        fin_stmt_period,
        fin_stmt_start_dt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1100_amt * 1000 else fin_stmt_1100_amt end) as fin_stmt_1100_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1110_amt * 1000 else fin_stmt_1110_amt end) as fin_stmt_1110_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1120_amt * 1000 else fin_stmt_1120_amt end) as fin_stmt_1120_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1130_amt * 1000 else fin_stmt_1130_amt end) as fin_stmt_1130_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1140_amt * 1000 else fin_stmt_1140_amt end) as fin_stmt_1140_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1150_amt * 1000 else fin_stmt_1150_amt end) as fin_stmt_1150_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1160_amt * 1000 else fin_stmt_1160_amt end) as fin_stmt_1160_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1170_amt * 1000 else fin_stmt_1170_amt end) as fin_stmt_1170_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1180_amt * 1000 else fin_stmt_1180_amt end) as fin_stmt_1180_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1190_amt * 1000 else fin_stmt_1190_amt end) as fin_stmt_1190_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1210_amt * 1000 else fin_stmt_1210_amt end) as fin_stmt_1210_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1220_amt * 1000 else fin_stmt_1220_amt end) as fin_stmt_1220_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_0230_amt * 1000 else fin_stmt_0230_amt end) as fin_stmt_0230_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1230_amt * 1000 else fin_stmt_1230_amt end) as fin_stmt_1230_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1240_amt * 1000 else fin_stmt_1240_amt end) as fin_stmt_1240_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1250_amt * 1000 else fin_stmt_1250_amt end) as fin_stmt_1250_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1260_amt * 1000 else fin_stmt_1260_amt end) as fin_stmt_1260_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1200_amt * 1000 else fin_stmt_1200_amt end) as fin_stmt_1200_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1310_amt * 1000 else fin_stmt_1310_amt end) as fin_stmt_1310_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1320_amt * 1000 else fin_stmt_1320_amt end) as fin_stmt_1320_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1340_amt * 1000 else fin_stmt_1340_amt end) as fin_stmt_1340_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1350_amt * 1000 else fin_stmt_1350_amt end) as fin_stmt_1350_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1360_amt * 1000 else fin_stmt_1360_amt end) as fin_stmt_1360_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1370_amt * 1000 else fin_stmt_1370_amt end) as fin_stmt_1370_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1300_amt * 1000 else fin_stmt_1300_amt end) as fin_stmt_1300_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1410_amt * 1000 else fin_stmt_1410_amt end) as fin_stmt_1410_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1420_amt * 1000 else fin_stmt_1420_amt end) as fin_stmt_1420_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1430_amt * 1000 else fin_stmt_1430_amt end) as fin_stmt_1430_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1450_amt * 1000 else fin_stmt_1450_amt end) as fin_stmt_1450_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1400_amt * 1000 else fin_stmt_1400_amt end) as fin_stmt_1400_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1510_amt * 1000 else fin_stmt_1510_amt end) as fin_stmt_1510_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1520_amt * 1000 else fin_stmt_1520_amt end) as fin_stmt_1520_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1530_amt * 1000 else fin_stmt_1530_amt end) as fin_stmt_1530_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1540_amt * 1000 else fin_stmt_1540_amt end) as fin_stmt_1540_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1550_amt * 1000 else fin_stmt_1550_amt end) as fin_stmt_1550_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1500_amt * 1000 else fin_stmt_1500_amt end) as fin_stmt_1500_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1600_amt * 1000 else fin_stmt_1600_amt end) as fin_stmt_1600_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_1700_amt * 1000 else fin_stmt_1700_amt end) as fin_stmt_1700_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2110_amt * 1000 else fin_stmt_2110_amt end) as fin_stmt_2110_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2120_amt * 1000 else fin_stmt_2120_amt end) as fin_stmt_2120_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2210_amt * 1000 else fin_stmt_2210_amt end) as fin_stmt_2210_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2220_amt * 1000 else fin_stmt_2220_amt end) as fin_stmt_2220_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2200_amt * 1000 else fin_stmt_2200_amt end) as fin_stmt_2200_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2320_amt * 1000 else fin_stmt_2320_amt end) as fin_stmt_2320_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2330_amt * 1000 else fin_stmt_2330_amt end) as fin_stmt_2330_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2340_amt * 1000 else fin_stmt_2340_amt end) as fin_stmt_2340_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2350_amt * 1000 else fin_stmt_2350_amt end) as fin_stmt_2350_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2300_amt * 1000 else fin_stmt_2300_amt end) as fin_stmt_2300_amt,
        (case when fin_stmt_meas_cd = 'млн.' then fin_stmt_2400_amt * 1000 else fin_stmt_2400_amt end) as fin_stmt_2400_amt
  from
        (
            select 
                    dh.DIVID as crm_cust_id,
                    dh.year as fin_stmt_year,
                    dh.period as fin_stmt_period,
                    case
                        when dh.period in (1,2,3,4) and year > 1900 then case 
                                                                        when dh.period = 1 then to_date(concat(year,'-01-01'))
                                                                        when dh.period = 2 then to_date(concat(year,'-04-01'))
                                                                        when dh.period = 3 then to_date(concat(year,'-07-01'))
                                                                        when dh.period = 4 then to_date(concat(year,'-10-01'))
                                                                    end
                        else null
                    end as fin_stmt_start_dt,
                    max(case when dd.field_name = 'MEASURE' then dd.ch end) as fin_stmt_meas_cd,
                    max(case when dd.field_name = 'BB1.1100.3' then dd.nm end) as fin_stmt_1100_amt,
                    max(case when dd.field_name = 'BB1.1110.3' then dd.nm end) as fin_stmt_1110_amt,
                    max(case when dd.field_name = 'BB1.1120.3' then dd.nm end) as fin_stmt_1120_amt,
                    max(case when dd.field_name = 'BB1.1130.3' then dd.nm end) as fin_stmt_1130_amt,
                    max(case when dd.field_name = 'BB1.1140.3' then dd.nm end) as fin_stmt_1140_amt,
                    max(case when dd.field_name = 'BB1.1150.3' then dd.nm end) as fin_stmt_1150_amt,
                    max(case when dd.field_name = 'BB1.1160.3' then dd.nm end) as fin_stmt_1160_amt,
                    max(case when dd.field_name = 'BB1.1170.3' then dd.nm end) as fin_stmt_1170_amt,
                    max(case when dd.field_name = 'BB1.1180.3' then dd.nm end) as fin_stmt_1180_amt,
                    max(case when dd.field_name = 'BB1.1190.3' then dd.nm end) as fin_stmt_1190_amt,
                    max(case when dd.field_name = 'BB1.1210.3' then dd.nm end) as fin_stmt_1210_amt,
                    max(case when dd.field_name = 'BB1.1220.3' then dd.nm end) as fin_stmt_1220_amt,
                    max(case when dd.field_name = 'BB1.0230.3' then dd.nm end) as fin_stmt_0230_amt,
                    max(case when dd.field_name = 'BB1.1230.3' then dd.nm end) as fin_stmt_1230_amt,
                    max(case when dd.field_name = 'BB1.1240.3' then dd.nm end) as fin_stmt_1240_amt,
                    max(case when dd.field_name = 'BB1.1250.3' then dd.nm end) as fin_stmt_1250_amt,
                    max(case when dd.field_name = 'BB1.1260.3' then dd.nm end) as fin_stmt_1260_amt,
                    max(case when dd.field_name = 'BB1.1200.3' then dd.nm end) as fin_stmt_1200_amt,
                    max(case when dd.field_name = 'BB1.1310.3' then dd.nm end) as fin_stmt_1310_amt,
                    max(case when dd.field_name = 'BB1.1320.3' then dd.nm end) as fin_stmt_1320_amt,
                    max(case when dd.field_name = 'BB1.1340.3' then dd.nm end) as fin_stmt_1340_amt,
                    max(case when dd.field_name = 'BB1.1350.3' then dd.nm end) as fin_stmt_1350_amt,
                    max(case when dd.field_name = 'BB1.1360.3' then dd.nm end) as fin_stmt_1360_amt,
                    max(case when dd.field_name = 'BB1.1370.3' then dd.nm end) as fin_stmt_1370_amt,
                    max(case when dd.field_name = 'BB1.1300.3' then dd.nm end) as fin_stmt_1300_amt,
                    max(case when dd.field_name = 'BB1.1410.3' then dd.nm end) as fin_stmt_1410_amt,
                    max(case when dd.field_name = 'BB1.1420.3' then dd.nm end) as fin_stmt_1420_amt,
                    max(case when dd.field_name = 'BB1.1430.3' then dd.nm end) as fin_stmt_1430_amt,
                    max(case when dd.field_name = 'BB1.1450.3' then dd.nm end) as fin_stmt_1450_amt,
                    max(case when dd.field_name = 'BB1.1400.3' then dd.nm end) as fin_stmt_1400_amt,
                    max(case when dd.field_name = 'BB1.1510.3' then dd.nm end) as fin_stmt_1510_amt,
                    max(case when dd.field_name = 'BB1.1520.3' then dd.nm end) as fin_stmt_1520_amt,
                    max(case when dd.field_name = 'BB1.1530.3' then dd.nm end) as fin_stmt_1530_amt,
                    max(case when dd.field_name = 'BB1.1540.3' then dd.nm end) as fin_stmt_1540_amt,
                    max(case when dd.field_name = 'BB1.1550.3' then dd.nm end) as fin_stmt_1550_amt,
                    max(case when dd.field_name = 'BB1.1500.3' then dd.nm end) as fin_stmt_1500_amt,
                    max(case when dd.field_name = 'BB1.1600.3' then dd.nm end) as fin_stmt_1600_amt,
                    max(case when dd.field_name = 'BB1.1700.3' then dd.nm end) as fin_stmt_1700_amt,
                    max(case when dd.field_name = 'PU1.2110.3' then dd.nm end) as fin_stmt_2110_amt,
                    max(case when dd.field_name = 'PU1.2120.3' then dd.nm end) as fin_stmt_2120_amt,
                    max(case when dd.field_name = 'PU1.2210.3' then dd.nm end) as fin_stmt_2210_amt,
                    max(case when dd.field_name = 'PU1.2220.3' then dd.nm end) as fin_stmt_2220_amt,
                    max(case when dd.field_name = 'PU1.2200.3' then dd.nm end) as fin_stmt_2200_amt,
                    max(case when dd.field_name = 'PU1.2320.3' then dd.nm end) as fin_stmt_2320_amt,
                    max(case when dd.field_name = 'PU1.2330.3' then dd.nm end) as fin_stmt_2330_amt,
                    max(case when dd.field_name = 'PU1.2340.3' then dd.nm end) as fin_stmt_2340_amt,
                    max(case when dd.field_name = 'PU1.2350.3' then dd.nm end) as fin_stmt_2350_amt,
                    max(case when dd.field_name = 'PU1.2300.3' then dd.nm end) as fin_stmt_2300_amt,
                    max(case when dd.field_name = 'PU1.2400.3' then dd.nm end) as fin_stmt_2400_amt
              from 
                    t_team_k7m_stg.fok_docs_data dd
                    join t_team_k7m_aux_d.fok_docs_header_final_rsbu dh on dd.docs_header_id = dh.id
            group by 
                    dh.DIVID, dh.year, dh.period
        ) sel
 where
        fin_stmt_start_dt is not NULL and
        fin_stmt_start_dt < '2018-04-27' -- здесь нужна бизнес-дата расчета
);        

-- step3. Приведение показателей agr к репрезентативному виду
drop table if exists t_team_k7m_aux_d.fok_fin_stmt_rsbu_agr 
create table t_team_k7m_aux_d.fok_fin_stmt_rsbu_agr as 
select
		crm_cust_id,
        fin_stmt_start_dt,
        (coalesce(fin_stmt_2110_amt, 0) + coalesce(fin_stmt_2110_amt_fx, 0) - coalesce(fin_stmt_2110_amt_nx, 0)) as fin_stmt_2110_amt,
        (coalesce(fin_stmt_2300_amt, 0) + coalesce(fin_stmt_2300_amt_fx, 0) - coalesce(fin_stmt_2300_amt_nx, 0)) as fin_stmt_2300_amt,
        (coalesce(fin_stmt_2200_amt, 0) + coalesce(fin_stmt_2200_amt_fx, 0) - coalesce(fin_stmt_2200_amt_nx, 0)) as fin_stmt_2200_amt,
        (coalesce(fin_stmt_2320_amt, 0) + coalesce(fin_stmt_2320_amt_fx, 0) - coalesce(fin_stmt_2320_amt_nx, 0)) as fin_stmt_2320_amt,
        (coalesce(fin_stmt_2330_amt, 0) + coalesce(fin_stmt_2330_amt_fx, 0) - coalesce(fin_stmt_2330_amt_nx, 0)) as fin_stmt_2330_amt,
        (coalesce(fin_stmt_2340_amt, 0) + coalesce(fin_stmt_2340_amt_fx, 0) - coalesce(fin_stmt_2340_amt_nx, 0)) as fin_stmt_2340_amt,
        (coalesce(fin_stmt_2350_amt, 0) + coalesce(fin_stmt_2350_amt_fx, 0) - coalesce(fin_stmt_2350_amt_nx, 0)) as fin_stmt_2350_amt,
        (coalesce(fin_stmt_2400_amt, 0) + coalesce(fin_stmt_2400_amt_fx, 0) - coalesce(fin_stmt_2400_amt_nx, 0)) as fin_stmt_2400_amt
  from
		(
			select 
					-- В b. хранится текущее значение R(N,X)
					b.crm_cust_id,
                    b.fin_stmt_start_dt,
                    b.fin_stmt_2110_amt,
                    b.fin_stmt_2300_amt,
                    b.fin_stmt_2200_amt,
                    b.fin_stmt_2320_amt,
                    b.fin_stmt_2330_amt,
                    b.fin_stmt_2340_amt,
                    b.fin_stmt_2350_amt,
                    b.fin_stmt_2400_amt,
					-- Это значения R(N,X-1)
					c.fin_stmt_2110_amt as fin_stmt_2110_amt_nx,
                    c.fin_stmt_2300_amt as fin_stmt_2300_amt_nx,
                    c.fin_stmt_2200_amt as fin_stmt_2200_amt_nx,
                    c.fin_stmt_2320_amt as fin_stmt_2320_amt_nx,
                    c.fin_stmt_2330_amt as fin_stmt_2330_amt_nx,
                    c.fin_stmt_2340_amt as fin_stmt_2340_amt_nx,
                    c.fin_stmt_2350_amt as fin_stmt_2350_amt_nx,
                    c.fin_stmt_2400_amt as fin_stmt_2400_amt_nx,
					-- Это значения R(4,X-1)
					fc.fin_stmt_2110_amt as fin_stmt_2110_amt_fx,
                    fc.fin_stmt_2300_amt as fin_stmt_2300_amt_fx,
                    fc.fin_stmt_2200_amt as fin_stmt_2200_amt_fx,
                    fc.fin_stmt_2320_amt as fin_stmt_2320_amt_fx,
                    fc.fin_stmt_2330_amt as fin_stmt_2330_amt_fx,
                    fc.fin_stmt_2340_amt as fin_stmt_2340_amt_fx,
                    fc.fin_stmt_2350_amt as fin_stmt_2350_amt_fx,
                    fc.fin_stmt_2400_amt as fin_stmt_2400_amt_fx
			  from
					t_team_k7m_aux_d.fok_fin_stmt_rsbu b
					left join t_team_k7m_aux_d.fok_fin_stmt_rsbu c on ( b.fin_stmt_year - 1 = c.fin_stmt_year 
                        AND b.fin_stmt_period = c.fin_stmt_period
                        AND b.crm_cust_id = c.crm_cust_id)
					left join t_team_k7m_aux_d.fok_fin_stmt_rsbu fc on ( b.fin_stmt_year - 1 = fc.fin_stmt_year
                        AND fc.fin_stmt_period = 4
                        AND b.crm_cust_id = fc.crm_cust_id)
			 where
					b.fin_stmt_2110_amt > 0 and
					c.fin_stmt_2110_amt > 0 and 
					fc.fin_stmt_2110_amt > 0 and
					fc.fin_stmt_2110_amt >= c.fin_stmt_2110_amt
		) sel
;     

-- step4. Сборка финального агрегата для пересечения с расширенным базисом.
-- НУЖНО МАТЕРИАЛИЗОВАТЬ В PUBLIC AREA (PA)
drop table if exists t_team_k7m_pa_d.pdfokin
create table t_team_k7m_pa_d.pdfokin as 
select
		a.crm_cust_id,
		a.fin_stmt_start_dt,
		clu.inn as inn_num,
		cast(a.fin_stmt_1600_amt as double) as fin_stmt_1600_amt, -- ta
		cast(a.fin_stmt_0230_amt as double) as fin_stmt_0230_amt, -- accounts_receivable
		cast(a.fin_stmt_1250_amt as double) as fin_stmt_1250_amt, --Cash,
		cast(a.fin_stmt_1410_amt as double) as fin_stmt_1410_amt, --Ltcredit,
		cast(a.fin_stmt_1400_amt as double) as fin_stmt_1400_amt, --Ltliab,
		cast(a.fin_stmt_1510_amt as double) as fin_stmt_1510_amt, --Stcredit,
		cast(a.fin_stmt_1500_amt as double) as fin_stmt_1500_amt, --Stliab,
		cast(a.fin_stmt_1300_amt as double) as fin_stmt_1300_amt, --Equity,
		cast(a.fin_stmt_1200_amt as double) as fin_stmt_1200_amt, --Stasset,
		cast(b.fin_stmt_2110_amt as double) as fin_stmt_2110_amt, --Revenue,
		cast(b.fin_stmt_2300_amt as double) as fin_stmt_2300_amt, --EBIT,
		cast(b.fin_stmt_2200_amt as double) as fin_stmt_2200_amt, --OnSaleProfit,
		cast(b.fin_stmt_2330_amt as double) as fin_stmt_2330_amt, --Int2Reciev,
		cast(b.fin_stmt_2320_amt as double) as fin_stmt_2320_amt, --Int2Pay,
		cast(b.fin_stmt_2340_amt as double) as fin_stmt_2340_amt, --IncomeOther,
		cast(b.fin_stmt_2350_amt as double) as fin_stmt_2350_amt, --CostOther,
		cast(b.fin_stmt_2400_amt as double) as fin_stmt_2400_amt, --NetProfit,
		cast(coalesce(b.fin_stmt_2200_amt, 0) + coalesce(b.fin_stmt_2340_amt, 0) - coalesce(b.fin_stmt_2350_amt, 0) as double) as fin_stmt_calc_ebitda--EBITDA
  from 
		t_team_k7m_aux_d.fok_fin_stmt_rsbu a
		inner join t_team_k7m_aux_d.fok_fin_stmt_rsbu_agr b on (a.crm_cust_id = b.crm_cust_id and a.fin_stmt_start_dt = b.fin_stmt_start_dt)
		inner join t_team_k7m_pa_d.clu clu on (clu.crm_id = a.crm_cust_id)
;		
