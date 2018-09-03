package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.tran_misc.{Tables, FieldNames}

class BasisTrHistClass(val spark: SparkSession,
											 val config: Config,
											 tables: Tables)extends EtlLogger with EtlJob with FieldNames {

    private val DevSchema = config.aux

    private val Node1t_team_k7m_aux_d_basis_transactions_histIN = tables.kras //s"${DevSchema}.basis_transactions_kras_delta"
    private val Nodet_team_k7m_aux_d_basis_transactions_histOUT = s"$DevSchema.basis_transactions_hist"
    private val dashboardPath = s"${config.auxPath}basis_transactions_hist"


    override val dashboardName: String = Nodet_team_k7m_aux_d_basis_transactions_histOUT //витрина
    override def processName: String = "TransactHist"


		private val basisClassEks = spark.table(Node1t_team_k7m_aux_d_basis_transactions_histIN)

		private val basisWithCasts = basisClassEks.select(
			basisClassEks.schema.map { f =>
				if (f.dataType == DoubleType) basisClassEks(f.name).cast(DecimalType(38,18)).as(f.name)
				else basisClassEks(f.name)
			} : _* )

		private lazy val HiveTableStage1 = spark.sql(
			s"""
				  |select distinct
  	      |		$keyFields,
  	      |		$krasField,
  	      |		$hashField,
          |   '$execId' as exec_id,
  	      |		cast (current_timestamp as string) as $dtField
  			  |from
					|		basisClassEKSCasted
 			""".stripMargin
		)

    def DoHist() {

      Logger.getLogger(Nodet_team_k7m_aux_d_basis_transactions_histOUT).setLevel(Level.WARN)


      basisWithCasts.createOrReplaceTempView("basisClassEKSCasted")

			HiveTableStage1
				.write
        .format("parquet")
        .option("path", dashboardPath)
        .insertInto(s"$Nodet_team_k7m_aux_d_basis_transactions_histOUT")

      logInserted()
    }
}

