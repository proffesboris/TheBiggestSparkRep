package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType, TimestampType}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate
import java.sql.Date


class AccRestClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  import spark.implicits._

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_acc_restIN =  s"${Stg0Schema}.eks_z_records"
  val Node2t_team_k7m_aux_d_k7m_acc_restIN =  s"${Stg0Schema}.eks_z_records_in_20161231"
  val Nodet_team_k7m_aux_d_k7m_acc_restOUT =  s"${DevSchema}.k7m_acc_rest"
  val dashboardPath = s"${config.auxPath}k7m_acc_rest"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_acc_restOUT //витрина
  override def processName: String = "EL"

  def DoAccRest(dateString: String, dateBeginString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_acc_restOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_k7m_acc_restPREP =  Nodet_team_k7m_aux_d_k7m_acc_restOUT.concat("_Prep")//s"t_team_k7m_aux_d.acc_rest_test"
    val Nodet_team_k7m_aux_d_k7m_acc_restPREP_Path = dashboardPath.concat("_Prep")
    val Nodet_team_k7m_aux_d_k7m_acc_restPREP2 =  Nodet_team_k7m_aux_d_k7m_acc_restOUT.concat("_Prep2")//s"t_team_k7m_aux_d.acc_rest_test"
    val Nodet_team_k7m_aux_d_k7m_acc_restPREP2_Path = dashboardPath.concat("_Prep2")
    val Nodet_team_k7m_aux_d_k7m_acc_restPREP3 =  Nodet_team_k7m_aux_d_k7m_acc_restOUT.concat("_Prep3")//s"t_team_k7m_aux_d.acc_rest_test"
    val Nodet_team_k7m_aux_d_k7m_acc_restPREP3_Path = dashboardPath.concat("_Prep3")
    val Nodet_team_k7m_aux_d_k7m_acc_restDt =  Nodet_team_k7m_aux_d_k7m_acc_restOUT.concat("_dt")//s"t_team_k7m_aux_d.acc_rest_test"
    val Nodet_team_k7m_aux_d_k7m_acc_restDt_Path= dashboardPath.concat("_dt")
    val Nodet_team_k7m_aux_d_k7m_acc_restDtProc =  Nodet_team_k7m_aux_d_k7m_acc_restOUT.concat("_dt_processed")//s"t_team_k7m_aux_d.acc_rest_test"
    val Nodet_team_k7m_aux_d_k7m_acc_restDtProc_Path= dashboardPath.concat("_dt_processed")

    val shuffleParts = spark.sqlContext.getConf("spark.sql.shuffle.partitions")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions",s"${shuffleParts.toInt*10}")

    val beginDate: Date = Date.valueOf(LocalDate.parse(dateBeginString))

    val endDate: Date = Date.valueOf(LocalDate.parse(dateString))

    val HiveTableStageOut = spark.table(Node1t_team_k7m_aux_d_k7m_acc_restIN)
      .where(lit(0) =!= lit(0))
      .select(
        lit(0).as("c_arc_move").cast(DecimalType(38,12)),
        lit(0).as("c_balance").cast(DecimalType(38,12)),
        lit(0).as("c_balance_lcl").cast(DecimalType(38,12))
      )

    HiveTableStageOut.write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restOUT")


    val DtProc = spark.table(Node1t_team_k7m_aux_d_k7m_acc_restIN)
      .where(lit(0) =!= lit(0))
      .select(substring(lit(beginDate),1,9).as("part_dt"), lit(s"$execId").as("exec_id"))
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy("exec_id")
      .option("path", Nodet_team_k7m_aux_d_k7m_acc_restDtProc_Path)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restDtProc")

    //val lastDate = spark.sql(s"select coalesce(max(cast(c_date as date)),date '$dateBeginString') dt from ${Nodet_team_k7m_aux_d_k7m_acc_restOUT}").first().getDate(0)

    val offsetDate: Date = Date.valueOf(endDate.toLocalDate.minusMonths(1).withDayOfMonth(1))

    val startDate: Date = if (beginDate.toLocalDate.isAfter(offsetDate.toLocalDate)) {beginDate} else {offsetDate}

    val DtProcLast = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restDtProc)
      .withColumn("max_exec_id",max($"exec_id").over(Window.partitionBy()))
      .where($"max_exec_id"===$"exec_id" && $"part_dt" < substring(lit(startDate),1,9))
      .select($"part_dt", lit(s"$execId").as("exec_id"))
        .collect.toSeq.foreach(rec => {

    val previousInterval = spark.sql(s"select '${rec.getString(0)}' as part_dt, '$execId' as exec_id")
      .write
      .format("parquet")
      .insertInto(s"$Nodet_team_k7m_aux_d_k7m_acc_restDtProc")})

    val drop_parts = spark.sql(s" show partitions ${Nodet_team_k7m_aux_d_k7m_acc_restDtProc}").collect()
      .foreach(prt => if (prt.getString(0) != s"exec_id=$execId") {spark.sql(s"alter table $Nodet_team_k7m_aux_d_k7m_acc_restDtProc drop partition (${prt.getString(0)})")})

    val maxPart: String = spark.sql(s" select nvl(max(part_dt),substr('$dateBeginString',1,9)) dt from ${Nodet_team_k7m_aux_d_k7m_acc_restDtProc}").first().getString(0)
    val maxDate = Date.valueOf(LocalDate.parse( if (maxPart.isEmpty) {dateBeginString} else { s"$maxPart${if (maxPart.last.toString == "0") {"1"} else "0"}"}))
    val recalcAll = (maxDate == dateBeginString)

    val z_records_all = spark.table(Node1t_team_k7m_aux_d_k7m_acc_restIN)
      .where($"c_date" < date_add(lit(endDate), 1).cast(TimestampType) &&
        $"c_date" >= lit(maxDate).cast(TimestampType))
      .select(
        $"id",
        $"collection_id",
        $"C_START_SUM",
        $"C_START_SUM_NAT",
        $"C_SUMMA_nat",
        $"C_SUMMA",
        $"c_date",
        $"C_DT")
      .withColumn("part_dt", substring($"c_date", 1, 9).cast("string"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("part_dt")
      .option("path", Nodet_team_k7m_aux_d_k7m_acc_restDt_Path)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restDt")

    val z_records_temp = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restOUT)
      .where(lit(0)=!=lit(0))
      .select(
        lit("").as("id").cast(DecimalType(38,12)),
        $"c_arc_move",
        $"C_BALANCE",
        $"C_BALANCE_LCL",
        lit(beginDate).cast(TimestampType).as("c_date"),
        lit(1).as("rn"))
      .write
      .format("parquet")
      .mode(if (recalcAll) {SaveMode.Overwrite} else {SaveMode.Append})
      .option("path", Nodet_team_k7m_aux_d_k7m_acc_restPREP2_Path)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restPREP2")

    val cycleDt = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restDt).select($"part_dt").distinct().orderBy($"part_dt").collect().toSeq

    cycleDt.foreach(recDt=>
    {
    if (spark.table(Nodet_team_k7m_aux_d_k7m_acc_restDtProc).where($"part_dt" === lit(recDt.getString(0))).count == 0) {

      val z_records_filtered = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restDt).where($"part_dt" === recDt.getString(0))
        .select(
          $"id",
          $"collection_id",
          $"C_START_SUM",
          $"C_START_SUM_NAT",
          $"C_SUMMA_nat",
          $"C_SUMMA",
          $"c_date",
          when($"C_DT" === lit("1"), lit(-1)).otherwise(1).as("direction"))
        .persist(StorageLevel.MEMORY_ONLY)

      z_records_filtered.count()

      val z_records_numbered = z_records_filtered
        .withColumn("rn", row_number().over(Window.partitionBy($"collection_id").orderBy($"c_date".desc, $"id".desc)))
        .where($"rn" === lit(1))
        .select(
          $"id",
          $"collection_id".as("c_arc_move").cast(DecimalType(38, 12)),
          ($"C_START_SUM" + $"direction" * $"C_SUMMA").as("C_BALANCE").cast(DecimalType(38, 12)),
          ($"C_START_SUM_NAT" + $"direction" * $"C_SUMMA").as("C_BALANCE_LCL").cast(DecimalType(38, 12)),
          $"c_date",
          $"rn").persist()

      z_records_numbered.count()

      z_records_numbered
        .write
        .format("parquet")
        .insertInto(s"$Nodet_team_k7m_aux_d_k7m_acc_restPREP2")

      val DtProc = spark.sql(s" select '${recDt.getString(0)}' as part_dt, '$execId' as exec_id")
        .write
        .format("parquet")
        .insertInto(s"$Nodet_team_k7m_aux_d_k7m_acc_restDtProc")

      z_records_filtered.unpersist()
      z_records_numbered.unpersist()
    }
    })

    val z_rec_temp = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restPREP2)
      .withColumn("rn1", row_number().over(Window.partitionBy($"c_arc_move").orderBy($"c_date".desc, $"id".desc)))
      .where($"rn1"===lit(1))
      .select(
        $"c_arc_move",
        $"C_BALANCE",
        $"C_BALANCE_LCL",
        $"rn1".as("rn"),
        $"c_date",
        $"id")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", Nodet_team_k7m_aux_d_k7m_acc_restPREP3_Path)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restPREP3")

    val HiveTableStage2 = spark.table(Node2t_team_k7m_aux_d_k7m_acc_restIN)
      .where($"c_balance" =!= 0)
      .select(
        $"collection_id".as("c_arc_move").cast(DecimalType(38,12)),
        $"c_balance".cast(DecimalType(38,12)),
        $"c_balance_lcl".cast(DecimalType(38,12)),
        lit(0).as("rn").cast(IntegerType)
      )

    val createHiveTableStage3 = HiveTableStage2
      .union(spark.table(Nodet_team_k7m_aux_d_k7m_acc_restPREP3).select(
                                                        $"c_arc_move",
                                                        $"C_BALANCE",
                                                        $"C_BALANCE_LCL",
                                                        $"rn"))
      .withColumn("rn1", row_number().over(Window.partitionBy($"c_arc_move").orderBy($"rn".desc)))
        .where($"rn1" === lit(1))
              .select(
                   $"c_arc_move",
                   $"C_BALANCE",
                   $"C_BALANCE_LCL")
            .write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .option("path", dashboardPath)
            .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restOUT")

    //сохраним все актуальные записи для будущих расчетов (меньше хранимый объем, меньше операций)
    val z_rec_save = spark.table(Nodet_team_k7m_aux_d_k7m_acc_restPREP3)
      .select(
        $"id",
        $"c_arc_move",
        $"C_BALANCE",
        $"C_BALANCE_LCL",
        $"c_date",
        lit(1).as("rn"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", Nodet_team_k7m_aux_d_k7m_acc_restPREP2_Path)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restPREP2")

    spark.sqlContext.setConf("spark.sql.shuffle.partitions",s"${shuffleParts}")
/*
    val createHiveTableStage3 = spark.sql(
      s"""select
    c_arc_move,
    C_BALANCE,
    C_BALANCE_LCL
from (
    select
          r2.collection_id c_arc_move,
          cast(cast(C_START_SUM as double)as decimal(17,2))+direction*cast(cast(C_SUMMA as double)as decimal(17,2)) C_BALANCE,
          cast(cast(C_START_SUM_NAT as double)as decimal(17,2)) +direction*cast(cast(C_SUMMA_nat as double)as decimal(17,2)) C_BALANCE_LCL,
          row_number () over (partition by r2.collection_id order by r2.rn desc ) rn2
      from (
              select
                 r.collection_id collection_id,
                 C_START_SUM,
                 C_START_SUM_NAT,
                 C_SUMMA_nat,
                 C_SUMMA,
                 cast(case when C_DT = '1' then -1 else 1 end as int) direction ,
                 row_number () over (partition by r.collection_id order by c_date desc, id desc ) rn
              from $Node1t_team_k7m_aux_d_k7m_acc_restIN r
              where r.c_date < --cast(date_add(date'${dateString}',1) as timestamp)
                               unix_timestamp(cast(date'${dateString}' as timestamp)   )*1000 +24*3600*1000
                and r.c_date >= --cast(date'${dateBeginString}' as timestamp)
                                unix_timestamp(cast(date'$dateBeginString' as timestamp))*1000
              UNION all
              --Входящие остатки
              select
                 cast(collection_id as decimal(38,12)) collection_id,
                 cast(c_balance as decimal(17,2)) C_START_SUM,
                 cast(c_balance_lcl as decimal(17,2)) C_START_SUM_NAT,
                 cast(0 as decimal(17,2)) C_SUMMA_nat,
                 cast(0 as decimal(17,2)) C_SUMMA,
                 cast(1 as int) direction ,
                 cast(0 as int) rn
              FROM $Node2t_team_k7m_aux_d_k7m_acc_restIN
              where c_balance <> 0
            ) r2
      where rn<=1
      ) x
    where  rn2=1 """   ).persist(StorageLevel.DISK_ONLY)

    createHiveTableStage3.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_acc_restOUT")
*/

    logInserted()
    logEnd()
  }

}