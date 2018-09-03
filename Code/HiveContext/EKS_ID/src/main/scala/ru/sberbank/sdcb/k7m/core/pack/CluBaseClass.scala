package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CluBaseClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob{

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clu_baseIN = s"$DevSchema.clu_keys"
  val Node2t_team_k7m_aux_d_clu_baseIN = s"$DevSchema.CLU_to_eks_id"
  val Node3t_team_k7m_aux_d_clu_baseIN = s"$DevSchema.clu_extended_basis"
  val Node4t_team_k7m_aux_d_clu_baseIN = s"$DevSchema.basis_client"
  val Nodet_team_k7m_aux_d_clu_baseOUT = s"$DevSchema.clu_base"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clu_baseOUT //витрина
  val dashboardPath = s"${config.auxPath}clu_base"
  override def processName: String = "clu_base"

  def DoCLUBase() {

    Logger.getLogger(Nodet_team_k7m_aux_d_clu_baseOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      cast(k.u7m_id as string) u7m_id,
      k.crm_id,
      k.inn,
      eb.FLAG_BASIS_CLIENT,
      e.eks_id,
      e.eks_duplicate_cnt
  from $Node3t_team_k7m_aux_d_clu_baseIN eb
  left join $Node1t_team_k7m_aux_d_clu_baseIN k
    on eb.u7m_id = k.u7m_id
  left join (
            select
              u7m_id,
              eks_id,
              eks_duplicate_cnt,
              tb_vko_crm_name
          from $Node2t_team_k7m_aux_d_clu_baseIN
          where priority = 1 ) e
    on e.u7m_id = k.u7m_id
 where k.status = 'ACTIVE'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_baseOUT")

    logInserted()

    val checkStage1 = spark.sql(s"""
    SELECT
    INN,
    cast(SUM(SGN) as string) SGN
      FROM (
        SELECT INN, -1 SGN
          FROM $Nodet_team_k7m_aux_d_clu_baseOUT
    where flag_basis_client = 'Y'
    UNION ALL
      SELECT
    org_inn_crm_num,
    1 SGN
      FROM $Node4t_team_k7m_aux_d_clu_baseIN bb
    ) X
    GROUP BY INN
    HAVING
    SUM(SGN)<>0""").collect().toSeq

    val log1 = checkStage1.foreach(c=>log(processName,"Внимание!Требуется согласованное состояние clu_base(-1) и basis_client(1). "+c.getString(0)+": " +c.getString(1), CustomLogStatus.ERROR))

    val checkStage2 = spark.sql(s"""
    SELECT
    u7m_id,
    cast(SUM(SGN) as string) SGN
      FROM (
        SELECT u7m_id, -1 SGN
          FROM $Nodet_team_k7m_aux_d_clu_baseOUT
    UNION ALL
      SELECT
    k.u7m_id,
    1 SGN
      FROM $Node3t_team_k7m_aux_d_clu_baseIN bb
      join $Node1t_team_k7m_aux_d_clu_baseIN k
      on k.u7m_id = bb.u7m_id
    where k.status = 'ACTIVE'
    ) X
    GROUP BY u7m_id
    HAVING
    SUM(SGN)<>0""").collect().toSeq

    val log2 = checkStage2.foreach(c=>log(processName,"Внимание!Требуется согласованное состояние clu_base(-1) и clu_keys + lk/lkc(1). "+c.getString(0)+": " +c.getString(1), CustomLogStatus.ERROR))

    logEnd()
  }

}