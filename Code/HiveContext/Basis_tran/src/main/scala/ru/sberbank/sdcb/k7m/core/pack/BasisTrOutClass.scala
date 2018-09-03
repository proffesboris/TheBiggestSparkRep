package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.tran_misc.{Tables, FieldNames}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

class BasisTrOutClass(val spark: SparkSession,
                      val config: Config,
                      tables: Tables)extends EtlLogger with EtlJob with FieldNames {

    private val DevSchema = config.aux
    private val MartSchema = config.pa


    private val Node1t_team_k7m_aux_d_basis_transactions_outIN = tables.input//s"${Stg0Schema}.basis_eks(2)"
    private val Node2t_team_k7m_aux_d_basis_transactions_outIN = s"$DevSchema.basis_transactions_hist"
    private val Nodet_team_k7m_aux_d_basis_transactions_outOUT = s"$MartSchema.${tables.output}" //s"${DevSchema}.pdeksin"
    private val dashboardPath = s"${config.paPath}${tables.output}"


    override val dashboardName: String = Nodet_team_k7m_aux_d_basis_transactions_outOUT //витрина
    override def processName: String = "TransactHist"

    private lazy val HiveTableStage1 = spark.sql(
      s"""
					|select distinct
          |   a.$keyFields,
          |   a.$ktdtField,
          |   a.$classIdField,
          |   a.$stateIdField,
          |   $requiedFields,
          |   $requiedFields2,
          |   h.$krasField as $krasField2,
          |   h.$krasField
          |from (
          |   select
          |     cast($keyFields as bigint) as $keyFields,
          |     $ktdtField,
          |     $classIdField,
          |     $stateIdField,
          |     $requiedFields,
          |     $requiedFields2
          |   from
					|     $Node1t_team_k7m_aux_d_basis_transactions_outIN) a
          |   join (
          |     select distinct
          |       $keyFields,
          |       $krasField
          |     from (
          |       select
          |         cast($keyFields as bigint) as $keyFields,
          |        $krasField,
          |         row_number() over (partition by cast($keyFields as bigint) order by $dtField desc) rn
          |       from
					|         $Node2t_team_k7m_aux_d_basis_transactions_outIN
          |          ) h1
          |     where
          |       rn=1
          |     ) h
          |     on a.$keyFields = h.$keyFields
      """.stripMargin
    )

  def DoOut() {

      Logger.getLogger(Nodet_team_k7m_aux_d_basis_transactions_outOUT).setLevel(Level.WARN)


    HiveTableStage1
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_basis_transactions_outOUT")

    val checkIn = spark.table(Node1t_team_k7m_aux_d_basis_transactions_outIN).count()

    val pdeksin = spark.table(Nodet_team_k7m_aux_d_basis_transactions_outOUT)

    val checkOut = pdeksin.count()

    val checkOutNull = pdeksin.filter(col(s"$krasField").isNull).count()

    var errorMsg1: String = ""
    var errorMsg2: String = ""

    if (checkIn != checkOut)
    { errorMsg1 = s"Количество записей в $Node1t_team_k7m_aux_d_basis_transactions_outIN ($checkIn) не совпадает с  количеством $Nodet_team_k7m_aux_d_basis_transactions_outOUT ($checkOut). "
      log("TransactHist",errorMsg1, CustomLogStatus.ERROR)}

    if (checkOutNull >0)
    {errorMsg2 = s"Количество записей c пустым $krasField в $Nodet_team_k7m_aux_d_basis_transactions_outOUT ($checkOutNull)"
      log("TransactHist",errorMsg2, CustomLogStatus.ERROR)}

    if ((checkIn != checkOut)||(checkOutNull >0))
      {throw new Exception(errorMsg1+errorMsg2)}

      logInserted(count = checkOut)
    }
}

