package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBLAllClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_dividends_in"
  val Node2t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_dividends_out"
  val Node3t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_nploan"
  val Node4t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_headonloan"
  val Node5t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_docapit"
  val Node6t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_headon_tr"
  val Node7t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_payoffloans"
  val Node8t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_payoffloans_out"
  val Node9t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_receiveloans"
  val Node10t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_sendloans"
  val Node11t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_consult_in"
  val Node12t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_coworking"
  val Node13t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_adregrul"
  val Node14t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_adrrosstat"
  val Node15t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_zalogi"
  val Node16t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_guarantors"
  val Node17t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_founders"
  val Node18t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_crmlk"
  val Node19t_team_k7m_aux_d_k7m_SBLIN = s"${config.aux}.crit_consgr"
  val Nodet_team_k7m_aux_d_k7m_SBLOUT = s"${config.aux}.k7m_SBL"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_SBLOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}k7m_SBL"

  def DoSBLAll() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_SBLOUT).setLevel(Level.WARN)

    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""SELECT
  --1. Критерий  dividends_in
    1000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'dividends_in' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node1t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --2. Критерий  dividends_out
    2000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'dividends_out' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node2t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --3. Критерий  nploan
    3000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'nploan' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node3t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --4. Критерий  headonloan
    4000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'headonloan' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node4t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --5. Критерий  docapit
    5000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'docapit' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node5t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --6. Критерий  headon_tr
    6000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'headon_tr' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node6t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --7. Критерий  payoffloans
    7000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'payoffloans' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node7t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --8. Критерий  payoffloans_out
    8000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'payoffloans_out' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node8t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --9. Критерий  receiveloans
    9000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'receiveloans' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node9t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --10. Критерий  sendloans
    10000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'sendloans' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node10t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --11. Критерий  consult_in
    11000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'consult_in' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node11t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --12. Критерий  coworking
    12000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'coworking' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node12t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --13. Критерий  adregrul
    13000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'adregrul' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node13t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --14. Критерий  adrrosstat
    14000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'adrrosstat' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node14t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --15. Критерий  zalogi
    15000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'zalogi' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node15t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --16. Критерий  guarantors
    16000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'guarantors' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node16t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --17. Критерий  founders
    17000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'founders' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node17t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --18. Критерий  crmlk
    18000000000000000+row_number() over (ORDER BY inn1,inn2) loc_id,
    inn1 from_inn,
    inn2 to_inn,
    'crmlk' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node18t_team_k7m_aux_d_k7m_SBLIN
 union all
SELECT
  --19. Критерий  consgr
    19000000000000000+row_number() over (ORDER BY left_inn,right_inn) loc_id,
    left_inn from_inn,
    right_inn to_inn,
    'consgr' crit_code,
    quantity,
    100 probability -- пока всегда 100
  from $Node19t_team_k7m_aux_d_k7m_SBLIN
"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_SBLOUT")

    logInserted()
    logEnd()
  }


}
