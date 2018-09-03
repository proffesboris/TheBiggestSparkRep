set DATE = ; -- пример

create table t_team_k7m_stg.int_fin_stmt_rsbu as 
select
		UL_INN as cust_inn_num,
		,PERIOD_START_DT as fin_stmt_start_dt
		,sum( fin_stmt_1100_amt/1000 ) as fin_stmt_1100_amt
		,sum( fin_stmt_1110_amt/1000 ) as fin_stmt_1110_amt
		,sum( fin_stmt_1120_amt/1000 ) as fin_stmt_1120_amt
		,sum( fin_stmt_1130_amt/1000 ) as fin_stmt_1130_amt
		,sum( fin_stmt_1140_amt/1000 ) as fin_stmt_1140_amt
		,sum( fin_stmt_1150_amt/1000 ) as fin_stmt_1150_amt
		,sum( fin_stmt_1160_amt/1000 ) as fin_stmt_1160_amt
		,sum( fin_stmt_1170_amt/1000 ) as fin_stmt_1170_amt
		,sum( fin_stmt_1180_amt/1000 ) as fin_stmt_1180_amt
		,sum( fin_stmt_1190_amt/1000 ) as fin_stmt_1190_amt
		,sum( fin_stmt_1210_amt/1000 ) as fin_stmt_1210_amt
		,sum( fin_stmt_1220_amt/1000 ) as fin_stmt_1220_amt
		,sum( fin_stmt_1230_amt/1000 ) as fin_stmt_1230_amt
		,sum( fin_stmt_1240_amt/1000 ) as fin_stmt_1240_amt
		,sum( fin_stmt_1250_amt/1000 ) as fin_stmt_1250_amt
		,sum( fin_stmt_1260_amt/1000 ) as fin_stmt_1260_amt
		,sum( fin_stmt_1200_amt/1000 ) as fin_stmt_1200_amt
		,sum( fin_stmt_1310_amt/1000 ) as fin_stmt_1310_amt
		,sum( fin_stmt_1320_amt/1000 ) as fin_stmt_1320_amt
		,sum( fin_stmt_1340_amt/1000 ) as fin_stmt_1340_amt
		,sum( fin_stmt_1350_amt/1000 ) as fin_stmt_1350_amt
		,sum( fin_stmt_1360_amt/1000 ) as fin_stmt_1360_amt
		,sum( fin_stmt_1370_amt/1000 ) as fin_stmt_1370_amt
		,sum( fin_stmt_1300_amt/1000 ) as fin_stmt_1300_amt
		,sum( fin_stmt_1410_amt/1000 ) as fin_stmt_1410_amt
		,sum( fin_stmt_1420_amt/1000 ) as fin_stmt_1420_amt
		,sum( fin_stmt_1430_amt/1000 ) as fin_stmt_1430_amt
		,sum( fin_stmt_1450_amt/1000 ) as fin_stmt_1450_amt
		,sum( fin_stmt_1400_amt/1000 ) as fin_stmt_1400_amt
		,sum( fin_stmt_1510_amt/1000 ) as fin_stmt_1510_amt
		,sum( fin_stmt_1520_amt/1000 ) as fin_stmt_1520_amt
		,sum( fin_stmt_1530_amt/1000 ) as fin_stmt_1530_amt
		,sum( fin_stmt_1540_amt/1000 ) as fin_stmt_1540_amt
		,sum( fin_stmt_1550_amt/1000 ) as fin_stmt_1550_amt
		,sum( fin_stmt_1500_amt/1000 ) as fin_stmt_1500_amt
		,sum( fin_stmt_1600_amt/1000 ) as fin_stmt_1600_amt
		,sum( fin_stmt_1700_amt/1000 ) as fin_stmt_1700_amt
		,sum( fin_stmt_2110_amt/1000 ) as fin_stmt_2110_amt
		,sum( fin_stmt_2120_amt/1000 ) as fin_stmt_2120_amt
		,sum( fin_stmt_2210_amt/1000 ) as fin_stmt_2210_amt
		,sum( fin_stmt_2220_amt/1000 ) as fin_stmt_2220_amt
		,sum( fin_stmt_2200_amt/1000 ) as fin_stmt_2200_amt
		,sum( fin_stmt_2320_amt/1000 ) as fin_stmt_2320_amt
		,sum( fin_stmt_2330_amt/1000 ) as fin_stmt_2330_amt
		,sum( fin_stmt_2340_amt/1000 ) as fin_stmt_2340_amt
		,sum( fin_stmt_2350_amt/1000 ) as fin_stmt_2350_amt
		,sum( fin_stmt_2300_amt/1000 ) as fin_stmt_2300_amt
		,sum( fin_stmt_2400_amt/1000 ) as fin_stmt_2400_amt
  from 
		(
			select 
					org.UL_ORG_ID
					,org.UL_INN
					,fs.PERIOD_START_DT
					,max(case when fs.FS_LINE_NUM = '1100' and fs.FS_LINE_CD = 'P11003' then fs.FS_VALUE end) as fin_stmt_1100_amt
					,max(case when fs.FS_LINE_NUM = '1110' and fs.FS_LINE_CD = 'P11103' then fs.FS_VALUE end) as fin_stmt_1110_amt
					,max(case when fs.FS_LINE_NUM = '1120' and fs.FS_LINE_CD = 'P11203' then fs.FS_VALUE end) as fin_stmt_1120_amt
					,max(case when fs.FS_LINE_NUM = '1130' and fs.FS_LINE_CD = 'P11303' then fs.FS_VALUE end) as fin_stmt_1130_amt
					,max(case when fs.FS_LINE_NUM = '1140' and fs.FS_LINE_CD = 'P11403' then fs.FS_VALUE end) as fin_stmt_1140_amt
					,max(case when fs.FS_LINE_NUM = '1150' and fs.FS_LINE_CD = 'P11503' then fs.FS_VALUE end) as fin_stmt_1150_amt
					,max(case when fs.FS_LINE_NUM = '1160' and fs.FS_LINE_CD = 'P11603' then fs.FS_VALUE end) as fin_stmt_1160_amt
					,max(case when fs.FS_LINE_NUM = '1170' and fs.FS_LINE_CD = 'P11703' then fs.FS_VALUE end) as fin_stmt_1170_amt
					,max(case when fs.FS_LINE_NUM = '1180' and fs.FS_LINE_CD = 'P11803' then fs.FS_VALUE end) as fin_stmt_1180_amt
					,max(case when fs.FS_LINE_NUM = '1190' and fs.FS_LINE_CD = 'P11903' then fs.FS_VALUE end) as fin_stmt_1190_amt
					,max(case when fs.FS_LINE_NUM = '1210' and fs.FS_LINE_CD = 'P12103' then fs.FS_VALUE end) as fin_stmt_1210_amt
					,max(case when fs.FS_LINE_NUM = '1220' and fs.FS_LINE_CD = 'P12203' then fs.FS_VALUE end) as fin_stmt_1220_amt
					,max(case when fs.FS_LINE_NUM = '1230' and fs.FS_LINE_CD = 'P12303' then fs.FS_VALUE end) as fin_stmt_1230_amt
					,max(case when fs.FS_LINE_NUM = '1240' and fs.FS_LINE_CD = 'P12403' then fs.FS_VALUE end) as fin_stmt_1240_amt
					,max(case when fs.FS_LINE_NUM = '1250' and fs.FS_LINE_CD = 'P12503' then fs.FS_VALUE end) as fin_stmt_1250_amt
					,max(case when fs.FS_LINE_NUM = '1260' and fs.FS_LINE_CD = 'P12603' then fs.FS_VALUE end) as fin_stmt_1260_amt
					,max(case when fs.FS_LINE_NUM = '1200' and fs.FS_LINE_CD = 'P12003' then fs.FS_VALUE end) as fin_stmt_1200_amt
					,max(case when fs.FS_LINE_NUM = '1310' and fs.FS_LINE_CD = 'P13103' then fs.FS_VALUE end) as fin_stmt_1310_amt
					,max(case when fs.FS_LINE_NUM = '1320' and fs.FS_LINE_CD = 'P13203' then fs.FS_VALUE end) as fin_stmt_1320_amt
					,max(case when fs.FS_LINE_NUM = '1340' and fs.FS_LINE_CD = 'P13403' then fs.FS_VALUE end) as fin_stmt_1340_amt
					,max(case when fs.FS_LINE_NUM = '1350' and fs.FS_LINE_CD = 'P13503' then fs.FS_VALUE end) as fin_stmt_1350_amt
					,max(case when fs.FS_LINE_NUM = '1360' and fs.FS_LINE_CD = 'P13603' then fs.FS_VALUE end) as fin_stmt_1360_amt
					,max(case when fs.FS_LINE_NUM = '1370' and fs.FS_LINE_CD = 'P13703' then fs.FS_VALUE end) as fin_stmt_1370_amt
					,max(case when fs.FS_LINE_NUM = '1300' and fs.FS_LINE_CD = 'P13003' then fs.FS_VALUE end) as fin_stmt_1300_amt
					,max(case when fs.FS_LINE_NUM = '1410' and fs.FS_LINE_CD = 'P14103' then fs.FS_VALUE end) as fin_stmt_1410_amt
					,max(case when fs.FS_LINE_NUM = '1420' and fs.FS_LINE_CD = 'P14203' then fs.FS_VALUE end) as fin_stmt_1420_amt
					,max(case when fs.FS_LINE_NUM = '1430' and fs.FS_LINE_CD = 'P14303' then fs.FS_VALUE end) as fin_stmt_1430_amt
					,max(case when fs.FS_LINE_NUM = '1450' and fs.FS_LINE_CD = 'P14503' then fs.FS_VALUE end) as fin_stmt_1450_amt
					,max(case when fs.FS_LINE_NUM = '1400' and fs.FS_LINE_CD = 'P14003' then fs.FS_VALUE end) as fin_stmt_1400_amt
					,max(case when fs.FS_LINE_NUM = '1510' and fs.FS_LINE_CD = 'P15103' then fs.FS_VALUE end) as fin_stmt_1510_amt
					,max(case when fs.FS_LINE_NUM = '1520' and fs.FS_LINE_CD = 'P15203' then fs.FS_VALUE end) as fin_stmt_1520_amt
					,max(case when fs.FS_LINE_NUM = '1530' and fs.FS_LINE_CD = 'P15303' then fs.FS_VALUE end) as fin_stmt_1530_amt
					,max(case when fs.FS_LINE_NUM = '1540' and fs.FS_LINE_CD = 'P15403' then fs.FS_VALUE end) as fin_stmt_1540_amt
					,max(case when fs.FS_LINE_NUM = '1550' and fs.FS_LINE_CD = 'P15503' then fs.FS_VALUE end) as fin_stmt_1550_amt
					,max(case when fs.FS_LINE_NUM = '1500' and fs.FS_LINE_CD = 'P15003' then fs.FS_VALUE end) as fin_stmt_1500_amt
					,max(case when fs.FS_LINE_NUM = '1600' and fs.FS_LINE_CD = 'P16003' then fs.FS_VALUE end) as fin_stmt_1600_amt
					,max(case when fs.FS_LINE_NUM = '1700' and fs.FS_LINE_CD = 'P17003' then fs.FS_VALUE end) as fin_stmt_1700_amt
					,max(case when fs.FS_LINE_NUM = '2110' and fs.FS_LINE_CD = 'P21103' then fs.FS_VALUE end) as fin_stmt_2110_amt
					,max(case when fs.FS_LINE_NUM = '2120' and fs.FS_LINE_CD = 'P21203' then fs.FS_VALUE end) as fin_stmt_2120_amt
					,max(case when fs.FS_LINE_NUM = '2210' and fs.FS_LINE_CD = 'P22103' then fs.FS_VALUE end) as fin_stmt_2210_amt
					,max(case when fs.FS_LINE_NUM = '2220' and fs.FS_LINE_CD = 'P22203' then fs.FS_VALUE end) as fin_stmt_2220_amt
					,max(case when fs.FS_LINE_NUM = '2200' and fs.FS_LINE_CD = 'P22003' then fs.FS_VALUE end) as fin_stmt_2200_amt
					,max(case when fs.FS_LINE_NUM = '2320' and fs.FS_LINE_CD = 'P23203' then fs.FS_VALUE end) as fin_stmt_2320_amt
					,max(case when fs.FS_LINE_NUM = '2330' and fs.FS_LINE_CD = 'P23303' then fs.FS_VALUE end) as fin_stmt_2330_amt
					,max(case when fs.FS_LINE_NUM = '2340' and fs.FS_LINE_CD = 'P23403' then fs.FS_VALUE end) as fin_stmt_2340_amt
					,max(case when fs.FS_LINE_NUM = '2350' and fs.FS_LINE_CD = 'P23503' then fs.FS_VALUE end) as fin_stmt_2350_amt
					,max(case when fs.FS_LINE_NUM = '2300' and fs.FS_LINE_CD = 'P23003' then fs.FS_VALUE end) as fin_stmt_2300_amt
					,max(case when fs.FS_LINE_NUM = '2400' and fs.FS_LINE_CD = 'P24003' then fs.FS_VALUE end) as fin_stmt_2400_amt
			  from 
					cb_akm_integrum.UL_ORGANIZATION_ROSSTAT as org
					inner join (
						select 
								c.*, 
								row_number() over (partition by UL_ORG_ID, PERIOD_START_DT, FS_LINE_NUM, FS_LINE_CD order by effectiveFrom desc) as rn
						  from 
								cb_akm_integrum.FINANCIAL_STATEMENT_ROSSTAT c
					) as fs on org.UL_ORG_ID = fs.UL_ORG_ID and rn = 1
			 where 
					'${hiveconf:DATE}' >= org.effectiveFrom	and 
					'${hiveconf:DATE}' < org.effectiveTo		
			 group by 
					org.UL_ORG_ID, org.UL_INN, fs.PERIOD_START_DT
		)
 where
		PERIOD_START_DT < '${hiveconf:DATE}'
group by UL_INN, PERIOD_START_DT;

