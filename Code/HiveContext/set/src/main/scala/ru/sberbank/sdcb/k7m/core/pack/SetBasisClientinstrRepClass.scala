package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetBasisClientinstrRepClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_basis_client_instr_repIN = s"${MartSchema}.clu"
    val  Nodet_team_k7m_aux_d_set_basis_client_instr_repOUT = s"${DevSchema}.set_basis_client_instr_rep"
    val dashboardPath = s"${config.auxPath}set_basis_client_instr_rep"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_basis_client_instr_repOUT //витрина
    override def processName: String = "SET"

    def DoSetBasisClientinstrRep() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_basis_client_instr_repOUT).setLevel(Level.WARN)

      logStart()

      val readHiveTable1 = spark.sql(s"""
select
    c.u7m_id,
    c.inn,
    coalesce(c.short_name,c.full_name) org_name,
    c.bsegment,
   'КД/НКЛ/ВКЛ' as instrument
from $Node1t_team_k7m_aux_d_set_basis_client_instr_repIN c
where c.flag_basis_client = 'Y'
union all
select
    c.u7m_id,
    c.inn,
    coalesce(c.short_name,c.full_name) org_name,
    c.bsegment,
    'ОВЕР' as instrument
from $Node1t_team_k7m_aux_d_set_basis_client_instr_repIN c
where c.flag_basis_client = 'Y'        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_basis_client_instr_repOUT")


      logInserted()
      logEnd()
    }
}

