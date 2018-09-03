package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FokFinStmtRsbuAgrClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_pa_d_fok_fin_stmt_rsbu_agrIN = s"${DevSchema}.fok_fin_stmt_rsbu"
   val Nodet_team_k7m_pa_d_fok_fin_stmt_rsbu_agrOUT = s"${DevSchema}.fok_fin_stmt_rsbu_agr"
  val dashboardPath = s"${config.auxPath}fok_fin_stmt_rsbu_agr"


  override val dashboardName: String = Nodet_team_k7m_pa_d_fok_fin_stmt_rsbu_agrOUT //витрина
  override def processName: String = "FOK_BASE"

  def DoFokFinStmtRsbuAgr ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_fok_fin_stmt_rsbu_agrOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select
         	crm_cust_id,
                cast(fin_stmt_start_dt as timestamp) as fin_stmt_start_dt,
                (coalesce(fin_stmt_2110_amt, 0) + coalesce(fin_stmt_2110_amt_fx, 0) - coalesce(fin_stmt_2110_amt_nx, 0)) as fin_stmt_2110_amt,
                (coalesce(fin_stmt_2300_amt, 0) + coalesce(fin_stmt_2300_amt_fx, 0) - coalesce(fin_stmt_2300_amt_nx, 0)) as fin_stmt_2300_amt,
                (coalesce(fin_stmt_2200_amt, 0) + coalesce(fin_stmt_2200_amt_fx, 0) - coalesce(fin_stmt_2200_amt_nx, 0)) as fin_stmt_2200_amt,
                (coalesce(fin_stmt_2320_amt, 0) + coalesce(fin_stmt_2320_amt_fx, 0) - coalesce(fin_stmt_2320_amt_nx, 0)) as fin_stmt_2320_amt,
                (coalesce(fin_stmt_2330_amt, 0) + coalesce(fin_stmt_2330_amt_fx, 0) - coalesce(fin_stmt_2330_amt_nx, 0)) as fin_stmt_2330_amt,
                (coalesce(fin_stmt_2340_amt, 0) + coalesce(fin_stmt_2340_amt_fx, 0) - coalesce(fin_stmt_2340_amt_nx, 0)) as fin_stmt_2340_amt,
                (coalesce(fin_stmt_2350_amt, 0) + coalesce(fin_stmt_2350_amt_fx, 0) - coalesce(fin_stmt_2350_amt_nx, 0)) as fin_stmt_2350_amt,
                (coalesce(fin_stmt_2400_amt, 0) + coalesce(fin_stmt_2400_amt_fx, 0) - coalesce(fin_stmt_2400_amt_nx, 0)) as fin_stmt_2400_amt
          from
         	(select
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
         				$Node1t_team_k7m_pa_d_fok_fin_stmt_rsbu_agrIN b
         				left join $Node1t_team_k7m_pa_d_fok_fin_stmt_rsbu_agrIN c on ( b.fin_stmt_year - 1 = c.fin_stmt_year
                                AND b.fin_stmt_period = c.fin_stmt_period
                                AND b.crm_cust_id = c.crm_cust_id)
         				left join $Node1t_team_k7m_pa_d_fok_fin_stmt_rsbu_agrIN fc on ( b.fin_stmt_year - 1 = fc.fin_stmt_year
                                AND fc.fin_stmt_period = 4
                                AND b.crm_cust_id = fc.crm_cust_id)
         		 where
         				b.fin_stmt_2110_amt > 0 and
         				c.fin_stmt_2110_amt > 0 and
         				fc.fin_stmt_2110_amt > 0 and
         				fc.fin_stmt_2110_amt >= c.fin_stmt_2110_amt
         	) sel
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_fok_fin_stmt_rsbu_agrOUT")

    logInserted()
    logEnd()
  }
}

