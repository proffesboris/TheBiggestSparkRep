package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.tran_misc.{Tables, FieldNames}

class BasisTrDeltaClass(val spark: SparkSession,
												val config: Config,
												tables: Tables)extends EtlLogger with EtlJob with FieldNames {

    private val DevSchema = config.aux

    private val Node1t_team_k7m_aux_d_basis_transactions_deltaIN = tables.input //s"${Stg0Schema}.innimport_z260318_6_out"
    private val Node2t_team_k7m_aux_d_basis_transactions_deltaIN = s"$DevSchema.basis_transactions_hist"
    private val Nodet_team_k7m_aux_d_basis_transactions_deltaOUT = s"$DevSchema.basis_transactions_delta"
    private val dashboardPath = s"${config.auxPath}basis_transactions_delta"


    override val dashboardName: String = Nodet_team_k7m_aux_d_basis_transactions_deltaOUT //витрина
    override def processName: String = "TransactDelta"

		private lazy val HiveTableStage0 = spark.sql(
			s"""
					|select
				 	|		$keyFields,
				 	|		cast (null as string) as $krasField,
				 	|		cast (null as string) as $hashField,
          |   '$execId' as exec_id,
				 	|		cast (current_timestamp as string) insert_dt
				 	|from
					|		$Node1t_team_k7m_aux_d_basis_transactions_deltaIN d
	 			 	|where
	 			 	|		0=1
	 		""".stripMargin
		)

		private lazy val HiveTableStage1 = spark.sql(
			s"""
			 		|select
			 		|		d.$keyFields,
			 		|		$ktdtField,
			 		|		$requiedFields,
			 		|		$classIdField,
			 		|		$stateIdField,
			 		|		d.$hashField
			 		|from (
			 		|  	select distinct
			 		|				$keyFields,
			 		|				$ktdtField,
			 		|				$requiedFields,
			 		|				$classIdField,
			 		|				$stateIdField,
			 		|				$hashField
			 		|		from
			    |				$Node1t_team_k7m_aux_d_basis_transactions_deltaIN ) d
			 		|		left join
			 		|		(select
			 		|				$keyFields,
			 		|				$hashField
			 		|		from (
			    |				select
			 		|						*,
			 		|						row_number() over (partition by $keyFields order by $dtField desc) rn
			 		|				from
			    |  					$Node2t_team_k7m_aux_d_basis_transactions_deltaIN
			 		|		) h
			 		|		where
			 		|				rn=1) h
          |    on d.$keyFields = h.$keyFields and d.$hashField = h.$hashField
          |  where h.$keyFields is null
		  """.stripMargin
		)

    def DoDelta() {

      Logger.getLogger(Nodet_team_k7m_aux_d_basis_transactions_deltaOUT).setLevel(Level.WARN)

      logStart()

      HiveTableStage0
				.write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy(dtField)
        .option("path", s"${config.auxPath}basis_transactions_hist")
        .saveAsTable(Node2t_team_k7m_aux_d_basis_transactions_deltaIN)

			HiveTableStage1
				.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(Nodet_team_k7m_aux_d_basis_transactions_deltaOUT)

      logInserted()
      logEnd()
    }
}

