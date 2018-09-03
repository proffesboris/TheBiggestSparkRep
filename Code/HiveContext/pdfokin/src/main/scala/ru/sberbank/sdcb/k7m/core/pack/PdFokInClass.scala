package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class PdFokInClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_pa_d_pdfokinIN = s"${DevSchema}.fok_fin_stmt_rsbu"
  val Node2t_team_k7m_pa_d_pdfokinIN = s"${DevSchema}.fok_fin_stmt_rsbu_agr"
  val Node3t_team_k7m_pa_d_pdfokinIN = s"${MartSchema}.clu"
   val Nodet_team_k7m_pa_d_pdfokinOUT = s"${MartSchema}.pdfokin"
  val dashboardPath = s"${config.paPath}pdfokin"


  override val dashboardName: String = Nodet_team_k7m_pa_d_pdfokinOUT //витрина
  override def processName: String = "pdfokin"

  def DoPdFokIn ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_pdfokinOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select
         	a.crm_cust_id,
         	cast(a.fin_stmt_start_dt as timestamp) as fin_stmt_start_dt,
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
         	$Node1t_team_k7m_pa_d_pdfokinIN a
         	inner join $Node2t_team_k7m_pa_d_pdfokinIN b on (a.crm_cust_id = b.crm_cust_id and a.fin_stmt_start_dt = b.fin_stmt_start_dt)
         	inner join $Node3t_team_k7m_pa_d_pdfokinIN clu on (clu.crm_id = a.crm_cust_id)
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdfokinOUT")

    logInserted()
    logEnd()
  }
}

