package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FokFinStmtRsbuClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_pa_d_fok_fin_stmt_rsbuIN = s"${Stg0Schema}.fok_docs_data"
  val Node2t_team_k7m_pa_d_fok_fin_stmt_rsbuIN = s"${DevSchema}.fok_docs_header_final_rsbu"
   val Nodet_team_k7m_pa_d_fok_fin_stmt_rsbuOUT = s"${DevSchema}.fok_fin_stmt_rsbu"
  val dashboardPath = s"${config.auxPath}fok_fin_stmt_rsbu"


  override val dashboardName: String = Nodet_team_k7m_pa_d_fok_fin_stmt_rsbuOUT //витрина
  override def processName: String = "FOK_BASE"

  def DoFokFinStmtRsbu ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_fok_fin_stmt_rsbuOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
         select
                           		crm_cust_id,
                           		fin_stmt_year,
                           		fin_stmt_period,
                           		cast(fin_stmt_start_dt as timestamp) as fin_stmt_start_dt,
                           		cast(fin_stmt_end_dt as timestamp) as fin_stmt_end_dt,
                           		cast(fin_stmt_rep_dt as timestamp) as fin_stmt_rep_dt,
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
                                   (    select
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
                                               case
                                                    when dh.period in (1,2,3,4) and year > 1900 then case
                                                        when dh.period = 1 then to_date(concat(year,'-03-31'))
                                                        when dh.period = 2 then to_date(concat(year,'-06-30'))
                                                        when dh.period = 3 then to_date(concat(year,'-09-30'))
                                                        when dh.period = 4 then to_date(concat(year,'-12-31'))
                                                      end
                                                    else null
                                                end as fin_stmt_end_dt,
                                                case
                                                    when dh.period in (1,2,3,4) and year > 1900 then case
                                                        when dh.period = 1 then to_date(concat(year,'-04-01'))
                                                        when dh.period = 2 then to_date(concat(year,'-07-01'))
                                                        when dh.period = 3 then to_date(concat(year,'-10-01'))
                                                        when dh.period = 4 then to_date(concat(year + 1,'-01-01'))
                                                      end
                                                    else null
                                               end as fin_stmt_rep_dt,
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
                                               (
                                                  select
                                                          docs_header_id,
                                                          field_name,
                                                          ch,
                                                          cast(nm as decimal(18,6)) as nm
                                                    from
                                                          $Node1t_team_k7m_pa_d_fok_fin_stmt_rsbuIN
                                                ) dd
                                               join $Node2t_team_k7m_pa_d_fok_fin_stmt_rsbuIN dh on dd.docs_header_id = dh.id
                                       group by
                                               dh.DIVID, dh.year, dh.period
                                   ) sel
                            where
                                   fin_stmt_rep_dt is not NULL and
                                   fin_stmt_rep_dt < current_timestamp()
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_fok_fin_stmt_rsbuOUT")

    logInserted()
    logEnd()
  }
}

