package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class Integrum7mClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_pa_d_pdintegruminIN = s"${Stg0Schema}.int_UL_ORGANIZATION_ROSSTAT"
  val Node2t_team_k7m_pa_d_pdintegruminIN = s"${Stg0Schema}.int_FINANCIAL_STATEMENT_ROSSTAT"
   val Nodet_team_k7m_pa_d_pdintegruminOUT = s"${MartSchema}.pdintegrumin"
   val Nodet_team_k7m_pa_d_pdintegrumin2OUT = s"${DevSchema}.pdintegrumin"
  val dashboardPath = s"${config.paPath}pdintegrumin"
  val dashboardPathStg = s"${config.auxPath}pdintegrumin"


  override val dashboardName: String = Nodet_team_k7m_pa_d_pdintegruminOUT //витрина
  override def processName: String = "pdintegrumin"

  def DoPdIntegrumIn (dateString: String)
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_pdintegruminOUT).setLevel(Level.WARN)
    logStart()

    val Nodet_team_k7m_pa_d_pdintegruminStg1 = Nodet_team_k7m_pa_d_pdintegrumin2OUT.concat("_Stg1")
    val Nodet_team_k7m_pa_d_pdintegruminStg2 = Nodet_team_k7m_pa_d_pdintegrumin2OUT.concat("_Stg2")

    val createHiveTableStage1 = spark.sql(
      s"""select
         	org.UL_ORG_ID
         	,org.UL_INN
         	,fs.PERIOD_START_DT
         	,max(case when FS_LINE_NUM = '1600' and FS_LINE_CD = 'P16003' then FS_VALUE end) as TA
         	,max(case when FS_LINE_NUM = '2110' and FS_LINE_CD = 'P21103' then FS_VALUE end) as Revenue
         	,max(case when FS_LINE_NUM = '1230' and FS_LINE_CD = 'P12303' then FS_VALUE end) as AccountsReceivables
         	,max(case when FS_LINE_NUM = '1250' and FS_LINE_CD = 'P12503' then FS_VALUE end) as Cash
         	,max(case when FS_LINE_NUM = '1410' and FS_LINE_CD = 'P14103' then FS_VALUE end) as Ltcredit
         	,max(case when FS_LINE_NUM = '1400' and FS_LINE_CD = 'P14003' then FS_VALUE end) as Ltliab
         	,max(case when FS_LINE_NUM = '1510' and FS_LINE_CD = 'P15103' then FS_VALUE end) as Stcredit
         	,max(case when FS_LINE_NUM = '1500' and FS_LINE_CD = 'P15003' then FS_VALUE end) as Stliab
         	,max(case when FS_LINE_NUM = '2300' and FS_LINE_CD = 'P23003' then FS_VALUE end) as EBIT
         	,max(case when FS_LINE_NUM = '2200' and FS_LINE_CD = 'P22003' then FS_VALUE end) as OnSaleProfit
         	,max(case when FS_LINE_NUM = '2320' and FS_LINE_CD = 'P23203' then FS_VALUE end) as Int2Reciev
         	,max(case when FS_LINE_NUM = '2330' and FS_LINE_CD = 'P23303' then FS_VALUE end) as Int2Pay
         	,max(case when FS_LINE_NUM = '2340' and FS_LINE_CD = 'P23403' then FS_VALUE end) as IncomeOther
         	,max(case when FS_LINE_NUM = '2350' and FS_LINE_CD = 'P23503' then FS_VALUE end) as CostOther
         	,max(case when FS_LINE_NUM = '1300' and FS_LINE_CD = 'P13003' then FS_VALUE end) as Equity
         	,max(case when FS_LINE_NUM = '1200' and FS_LINE_CD = 'P12003' then FS_VALUE end) as Stasset
         	,max(case when FS_LINE_NUM = '2400' and FS_LINE_CD = 'P24003' then FS_VALUE end) as NetProfit
         from (
         	select *
         	from $Node1t_team_k7m_pa_d_pdintegruminIN
         	where '${dateString}' >= effectiveFrom
         	and '${dateString}' < effectiveTo
         	) as org
         inner join (
         	select *, row_number() over(partition by UL_ORG_ID, PERIOD_START_DT, FS_LINE_NUM, FS_LINE_CD order by effectiveFrom desc) as rn
         	from $Node2t_team_k7m_pa_d_pdintegruminIN c
         	where '${dateString}' >= effectiveFrom) as fs
         on org.UL_ORG_ID = fs.UL_ORG_ID and rn = 1
         group by org.UL_ORG_ID, org.UL_INN, fs.PERIOD_START_DT
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPathStg}_Stg1")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdintegruminStg1")

    val createHiveTableStage2 = spark.sql(
      s"""
         select
         	 UL_INN
         	,PERIOD_START_DT
         	,sum(TA/1000) 					as TA
         	,sum(Revenue/1000)				as Revenue
         	,sum(AccountsReceivables/1000)	as AccountsReceivables
         	,sum(Cash/1000)					as Cash
         	,sum(Ltcredit/1000)				as Ltcredit
         	,sum(Ltliab/1000)				as Ltliab
         	,sum(Stcredit/1000)				as Stcredit
         	,sum(Stliab/1000)				as Stliab
         	,sum(EBIT/1000)					as EBIT
         	,sum(OnSaleProfit/1000)			as OnSaleProfit
         	,sum(Int2Reciev/1000)			as Int2Reciev
         	,sum(Int2Pay/1000)				as Int2Pay
         	,sum(IncomeOther/1000)			as IncomeOther
         	,sum(CostOther/1000)			as CostOther
         	,sum(Equity/1000)				as Equity
         	,sum(Stasset/1000)				as Stasset
         	,sum(NetProfit/1000)			as NetProfit
         from $Nodet_team_k7m_pa_d_pdintegruminStg1
         group by UL_INN, PERIOD_START_DT
    """
    )
    createHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPathStg}_Stg2")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdintegruminStg2")

    val createHiveTableStage3 = spark.sql(
      s"""
 select   ul_inn
         ,period_start_dt
         ,cast(EBITDA	as double) as ebitda
         ,cast(l1	as double) as l1
         ,cast(l2a	as double) as l2a
         ,cast(l2b	as double) as l2b
         ,cast(l2c	as double) as l2c
         ,cast(l2d	as double) as l2d
         ,cast(l3a	as double) as l3a
         ,cast(p1a	as double) as p1a
         ,cast(p1c	as double) as p1c
         ,cast(p2a	as double) as p2a
         ,cast(p4a	as double) as p4a
         ,cast(p4b	as double) as p4b
         ,cast(p4c	as double) as p4c
         ,cast(f1a	as double) as f1a
         ,cast(f2a	as double) as f2a
         ,cast(f2b	as double) as f2b
         ,cast(f2c	as double) as f2c
         ,cast(f2d	as double) as f2d
         ,cast(f2e	as double) as f2e
         ,cast(f2f	as double) as f2f
         ,cast(f2g	as double) as f2g
         ,cast(f2k	as double) as f2k
         ,cast(f2l	as double) as f2l
         ,cast(TA as  double) as ta
         ,cast(Revenue as  double) as revenue
         ,cast(AccountsReceivables as  double) as accountsreceivables
         ,cast(Cash	as double) as cash
         ,cast(Ltcredit as double) as ltcredit
         ,cast(Ltliab as double) as ltliab
         ,cast(Stcredit	as double) as stcredit
         ,cast(Stliab	as double) as stliab
         ,cast(EBIT as	double) as ebit
         ,cast(OnSaleProfit	as double) as onsaleprofit
         ,cast(Int2Reciev	as double) as int2reciev
         ,cast(Int2Pay	as double) as int2pay
         ,cast(IncomeOther	as double) as incomeother
         ,cast(CostOther	as double) as costother
         ,cast(Equity	as double) as equity
         ,cast(Stasset	as double) as stasset
         ,cast(NetProfit	as double) as netprofit
         ,calc_date
 from (
 select   	UL_INN
         	,'${dateString}' as CALC_DATE
         	,PERIOD_START_DT
         	,TA
         	,Revenue
         	,AccountsReceivables
         	,Cash
         	,Ltcredit
         	,Ltliab
         	,Stcredit
         	,Stliab
         	,EBIT
         	,OnSaleProfit
         	,Int2Reciev
         	,Int2Pay
         	,IncomeOther
         	,CostOther
         	,Equity
         	,Stasset
         	,NetProfit
         	,OnSaleProfit + IncomeOther - CostOther					as EBITDA
         	,case
         		when (stliab=0 and stasset=0) then 0
         		--when stliab=0 then stasset/0.01
         		--when stasset=0 then 0.01/stliab
         		else case when stasset = 0 then 0.01 else stasset end/case when stliab = 0 then 0.01 else stliab end
         		end 												as l1
         	, sign(coalesce(stasset,0.01)-coalesce(stliab,0.01))	as l2a
         	, case
         		when (stasset-stliab<=0) then log(1.01)
         		else log(stasset-stliab)
         		end													as l2b
         	,case
         	    when (stliab=0 and stasset=0) then 0
         		when stasset=0 then (0.01-stliab)/0.01
         		when stliab=0 then (stasset-0.01)/stasset
         		else (stasset-stliab)/stasset
         		end 												as l2c
         	, case
         		when (stasset=0 and stliab=0) then 0
         		when stasset=0 then (0.01-stliab)/stliab
         		when stliab=0 then (stasset-0.01)/0.01
         		else (stasset-stliab)/stliab
         		end													as l2d
         	, case
         		when stliab=0 then cash/0.01
         		else cash/stliab
         		end													as l3a
         	, case
         		when ta=0 then netprofit/0.01
         		else netprofit/ta
         		end													as p1a
         	, case
             when ta=0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/ta
         		end													as p1c
         	, case
         		when equity=0 then netprofit/0.01
         		else netprofit/equity
         		end						 							as p2a
         	,case
         		when revenue=0 then ebit/0.01
         		else ebit/revenue
         		end 										 		as p4a
         	,case
             when revenue=0 then netprofit/0.01
         		else netprofit/revenue
         		end 										 		as p4b
         	, case
             when revenue=0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/revenue
         		end										 			as p4c
         	,case
             when int2pay=0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/int2pay
         		end 												as f1a
         	, case
             when (stliab+ltliab) = 0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/(stliab+ltliab)
         		end													as f2a
         	, case
             when ltliab = 0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/ltliab
         		end													as f2b
         	, case
             when stliab = 0 then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/stliab
         		end													as f2c
         	, case
             when (stliab+ltliab) = 0 then ebit/0.01
         		else ebit/(stliab+ltliab)
         		end													as f2d
         	, case
             when ltliab = 0 then ebit/0.01
         		else ebit/ltliab
         		end										 			as f2e
         	, case
             when stliab = 0 then ebit/0.01
         		else ebit/stliab
         		end													as f2f
         	, case
             when (stliab+ltliab-cash) = 0  then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/(stliab+ltliab-cash)
         		end										 			as f2g
         	, case
             when (ltliab-cash)= 0   then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/(ltliab-cash)
         		end													as f2k
         	, case
             when (stliab - cash)= 0  then (OnSaleProfit + IncomeOther - CostOther)/0.01
         		else (OnSaleProfit + IncomeOther - CostOther)/(stliab-cash)
         		end													as f2l
         from $Nodet_team_k7m_pa_d_pdintegruminStg2)
    """
    )
    createHiveTableStage3
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdintegruminOUT")

    logInserted()
    logEnd()
  }
}

