package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

class CluMmz(override val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val processName = "CLU_MMZ"

  val StgSchema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  var dashboardName = s"$MartSchema.mmz_clu_final"

  def mmzClu() = {

    val mmzClu = spark.sql(
      s""" select
                 a.u7m_id,
                 max(case when b.role_id = 0 then b.mmz_off_check_zone end) as borrower_zone,
                 max(case when b.role_id = 1 then b.mmz_off_check_zone end) as participant_zone,
                 max(case when b.role_id = 2 then b.mmz_off_check_zone end) as guarantor_zone,
                 max(case when b.role_id = 3 then b.mmz_off_check_zone end) as pledger_zone
           from
                 $MartSchema.clu a
                 join $StgSchema.mmz_mmz_k7m_out b on (a.u7m_id = b.c7m_id)
                 join (select max(load_dt) load_dt from $StgSchema.mmz_mmz_k7m_out) c on (b.load_dt = c.load_dt)
          group by
                 a.u7m_id""".stripMargin)

    mmzClu
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"${config.paPath}mmz_clu")
      .saveAsTable(s"$MartSchema.mmz_clu")

    val mmzCluByClf = spark.sql(
      s"""select
        |        c.u7m_id_to as u7m_id,
        |        min(a.participant_zone) as mmz_zone
        |  from
        |        $MartSchema.mmz_clf a
        |        join $MartSchema.lk c on (a.f7m_id = c.u7m_id_from)
        | where
        |        a.participant_zone in ('0', '1') and
        |        c.t_to = 'CLU'
        | group by
        |        c.u7m_id_to
      """.stripMargin)

    mmzCluByClf
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"${config.auxPath}mmz_clu_by_clf")
      .saveAsTable(s"$DevSchema.mmz_clu_by_clf")

    val mmzCluByParticipant = spark.sql(
      s"""select
         |        u7m_id,
         |        min(participant_zone) as mmz_zone
         |  from
         |        (
         |          select
         |                  b.u7m_id,
         |                  a.participant_zone
         |            from
         |                  $MartSchema.mmz_clu a
         |                  join (
         |                    select lk_id, u7m_id_from as u7m_id, u7m_id_to as u7m_id_2
         |                      from $MartSchema.lk where t_from = 'CLU' and t_to = 'CLU'
         |                    union all
         |                    select lk_id, u7m_id_to as u7m_id, u7m_id_from as u7m_id_2
         |                      from $MartSchema.lk where t_from = 'CLU' and t_to = 'CLU'
         |                  ) b on (b.u7m_id_2 = a.u7m_id)
         |                  join $MartSchema.lkc c on (c.lk_id = b.lk_id)
         |                  join $StgSchema.rdm_rdm_link_criteria_mast_v d on (c.crit_id = d.code and d.cbr_flag = 1)
         |           where
         |                  a.participant_zone = '0'
         |          union all
         |          select
         |                  b.u7m_id,
         |                  a.participant_zone
         |            from
         |                  $MartSchema.mmz_clu a
         |                  join (
         |                    select lk_id, u7m_id_from as u7m_id, u7m_id_to as u7m_id_2
         |                      from $MartSchema.lk where t_from = 'CLU' and t_to = 'CLU'
         |                    union all
         |                    select lk_id, u7m_id_to as u7m_id, u7m_id_from as u7m_id_2
         |                      from $MartSchema.lk where t_from = 'CLU' and t_to = 'CLU'
         |                  ) b on (b.u7m_id_2 = a.u7m_id)
         |                  join $MartSchema.lkc c on (c.lk_id = b.lk_id and c.link_prob = 100)
         |                  join $StgSchema.rdm_rdm_link_criteria_mast_v d on (c.crit_id = d.code and d.cbr_flag = 1)
         |           where
         |                  a.participant_zone = '1'
         |        ) sel
         | group by
         |        u7m_id
      """.stripMargin)

    mmzCluByParticipant
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"${config.auxPath}mmz_clu_by_participant")
      .saveAsTable(s"$DevSchema.mmz_clu_by_participant")

    val mmzCluFinal = spark.sql(
      s"""select
         |        b.inn,
         |        a.u7m_id,
         |        case
         |            when c.mmz_zone in ('0', '1') then c.mmz_zone
         |            when d.mmz_zone in ('0', '1') then d.mmz_zone
         |            else a.borrower_zone
         |        end borrower_zone,
         |        case
         |            when c.mmz_zone in ('0', '1') then c.mmz_zone
         |            else a.guarantor_zone
         |        end guarantor_zone,
         |        case
         |            when c.mmz_zone in ('0', '1') then c.mmz_zone
         |            else a.participant_zone
         |        end participant_zone
         |  from
         |        $MartSchema.mmz_clu a
         |        join $MartSchema.clu b on (a.u7m_id = b.u7m_id)
         |        left join $DevSchema.mmz_clu_by_clf c on (a.u7m_id = c.u7m_id)
         |        left join $DevSchema.mmz_clu_by_participant d on (a.u7m_id = d.u7m_id)
         | where
         |        a.borrower_zone is not null or
         |        a.guarantor_zone is not null or
         |        a.participant_zone is not null
      """.stripMargin)

    mmzCluFinal
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"${config.paPath}mmz_clu_final")
      .saveAsTable(s"$MartSchema.mmz_clu_final")

    logInserted()
  }

  def mmzClf() = {

    val mmzClf = spark.sql(s"""select
                        a.f7m_id,
                        max(case when b.role_id = 1 then b.mmz_off_check_zone end) as participant_zone,
                        max(case when b.role_id = 2 then b.mmz_off_check_zone end) as guarantor_zone,
                        max(case when b.role_id = 3 then b.mmz_off_check_zone end) as pledger_zone
                  from
                        $MartSchema.clf a
                        join $StgSchema.mmz_mmz_k7m_out b on (a.f7m_id = b.c7m_id)
                        join (select max(load_dt) load_dt from $StgSchema.mmz_mmz_k7m_out) c on (b.load_dt = c.load_dt)
                 group by
                        a.f7m_id""".stripMargin)

    mmzClf
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", s"${config.paPath}mmz_clf")
      .saveAsTable(s"$MartSchema.mmz_clf")

  }

  def run() {
    logStart()

    mmzClf()
    mmzClu()

    logEnd()
  }
}