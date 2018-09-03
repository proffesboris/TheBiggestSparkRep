package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


class ClfKeysClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  import spark.implicits._

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_keysIN = s"${DevSchema}.clf_fpers_ki"
  //  val Node2t_team_k7m_aux_d_clf_keysIN = s"${DevSchema}.clf_bad_with_crm"
  val Node2t_team_k7m_aux_d_clf_keysIN = s"${DevSchema}.clf_bad_filtered"
  val Node3t_team_k7m_aux_d_clf_keysIN = s"${DevSchema}.clf_bad"
  val Node4t_team_k7m_aux_d_clf_keysIN = s"${DevSchema}.clf_bad_without_keys"
  val Nodet_team_k7m_aux_d_clf_keysOUT = s"${DevSchema}.clf_keys"
  val dashboardPath = s"${config.auxPath}clf_keys"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_keysOUT //витрина
  override def processName: String = "CLF"

  def DoClfKeys()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_keysOUT).setLevel(Level.WARN)

    val Nodet_team_k7m_aux_d_clf_keysBad = Nodet_team_k7m_aux_d_clf_keysOUT.concat("_ki_bad")
    val dashboardPathBad = s"${dashboardPath}_ki_bad"
    val Nodet_team_k7m_aux_d_clf_keysHash = Nodet_team_k7m_aux_d_clf_keysOUT.concat("_hash")
    val dashboardPathHash = s"${dashboardPath}_hash"
    val Nodet_team_k7m_aux_d_clf_keysMap = Nodet_team_k7m_aux_d_clf_keysOUT.concat("_map")
    val dashboardPathMap = s"${dashboardPath}_map"

    val smartSrcHiveTableStage1 = spark.sql(
      s"""
         select distinct
         id,
         md5(k1) as h1,
         md5(k2) as h2,
         md5(k3) as h3,
         md5(k4) as h4,
         crm_id as h5
         from $Node1t_team_k7m_aux_d_clf_keysIN
           union all
           select distinct
           id,
           md5(k1) as h1,
           md5(k2) as h2,
           md5(k3) as h3,
           md5(k4) as h4,
           crm_id as h5
           from $Node2t_team_k7m_aux_d_clf_keysIN
         """
    )  .alias("h").persist()

//    smartSrcHiveTableStage1
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_Stage1")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_Stage1")
    //H1
    val Nodet_team_k7m_aux_d_clf_keysD1 = smartSrcHiveTableStage1 //k1 is not null и кол-во таких id с к1
      .select("id","h1")
      .where($"h1".isNotNull)
      .distinct()
      .withColumn("cnt",count("id").over(Window.partitionBy("id")))
      .persist()

    val Nodet_team_k7m_aux_d_clf_keysS1 = Nodet_team_k7m_aux_d_clf_keysD1
      .where($"cnt"===1)
      .join(smartSrcHiveTableStage1.selectExpr("id","h1 as f7m_id","h1 as hf1","h2 as hf2","h3 as hf3","h4 as hf4", "h5 as hf5"),Seq("id"))
      .selectExpr("id as link_id","f7m_id","hf1","hf2","hf3","hf4","hf5")
      .persist()
    //.createGlobalTempView("CLF_KEYS_S1")

//    Nodet_team_k7m_aux_d_clf_keysS1
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_S1")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_S1")

    val Nodet_team_k7m_aux_d_clf_keysT1 = Nodet_team_k7m_aux_d_clf_keysD1
      .where($"cnt"===1)
      .selectExpr("id as link_id","h1 as f7m_id")//записываем их в clf_keys//s1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPathHash)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    val Nodet_team_k7m_aux_d_clf_keysB1 = Nodet_team_k7m_aux_d_clf_keysD1
      .where($"cnt".notEqual(lit(1)))
      .selectExpr("id as link_id","h1 as f7m_id") // плохие для разбора
      .write.format("parquet")
      .mode(SaveMode.Overwrite) //Append
      .option("path", dashboardPathBad)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")

    Nodet_team_k7m_aux_d_clf_keysD1.unpersist()

    val Nodet_team_k7m_aux_d_clf_keysD2 = smartSrcHiveTableStage1 //k2 is not null и кол-во таких id с к2
      .select("id","h2")
      .where(col("h2").isNotNull)
      .distinct()
      .join(Nodet_team_k7m_aux_d_clf_keysS1,col("id")===Nodet_team_k7m_aux_d_clf_keysS1("link_id"),"left_anti")
      .join(Nodet_team_k7m_aux_d_clf_keysS1.select("f7m_id","hf2"),col("h2")===(Nodet_team_k7m_aux_d_clf_keysS1("hf2")),"left")//join c S1, где
      .withColumn(("h"),when(Nodet_team_k7m_aux_d_clf_keysS1("f7m_id").isNotNull,Nodet_team_k7m_aux_d_clf_keysS1("f7m_id")).otherwise(col("h2")))
      .select("id","h")
      .distinct()
      .withColumn("cnt",count("id").over(Window.partitionBy("id")))
      .persist()
    //H2
//    Nodet_team_k7m_aux_d_clf_keysD2
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_D2")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_D2")

    //    val Nodet_team_k7m_aux_d_clf_keysS2 = Nodet_team_k7m_aux_d_clf_keysD2
    //      .where($"cnt"===1)
    //      .join(smartSrcHiveTableStage1.selectExpr("id","h1 as f7m_id","h2 as hf2","h3 as hf3","h4 as hf4", "h5 as hf5"),Seq("id"))
    //      .selectExpr("id as link_id","f7m_id","hf2","hf3","hf4","hf5")
    //      .persist()
    val Nodet_team_k7m_aux_d_clf_keysS2 = Nodet_team_k7m_aux_d_clf_keysD2.where($"cnt"===1)
      .join(smartSrcHiveTableStage1,Seq("id"))
      .select(
        smartSrcHiveTableStage1("id").as("link_id"),
        Nodet_team_k7m_aux_d_clf_keysD2("h").as("f7m_id"),
        smartSrcHiveTableStage1("h1").as("hf1"),
        smartSrcHiveTableStage1("h2").as("hf2"),
        smartSrcHiveTableStage1("h3").as("hf3"),
        smartSrcHiveTableStage1("h4").as("hf4"),
        smartSrcHiveTableStage1("h5").as("hf5")
      ).persist()

//    Nodet_team_k7m_aux_d_clf_keysS2
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_S2")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_S2")

    val Nodet_team_k7m_aux_d_clf_keysT2 = Nodet_team_k7m_aux_d_clf_keysD2
      .where($"cnt"===1)
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathHash)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    val Nodet_team_k7m_aux_d_clf_keysB2 = Nodet_team_k7m_aux_d_clf_keysD2
      .where($"cnt".notEqual(lit(1)))
      .distinct()
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathBad)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")

    Nodet_team_k7m_aux_d_clf_keysD2.unpersist()

    val Nodet_team_k7m_aux_d_clf_keysU2 = Nodet_team_k7m_aux_d_clf_keysS1.union(Nodet_team_k7m_aux_d_clf_keysS2).distinct().persist()

//    Nodet_team_k7m_aux_d_clf_keysU2
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_U2")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_U2")

    val Nodet_team_k7m_aux_d_clf_keysD3 = smartSrcHiveTableStage1 //k2 is not null и кол-во таких id с к2
      .select("id","h3")
      .where($"h3".isNotNull)
      .distinct()
      .join(Nodet_team_k7m_aux_d_clf_keysU2,col("id")===Nodet_team_k7m_aux_d_clf_keysU2("link_id"),"left_anti")
      .join(Nodet_team_k7m_aux_d_clf_keysU2.select("link_id","f7m_id","hf3"),col("h3")===(Nodet_team_k7m_aux_d_clf_keysU2("hf3")),"left")//join c S1, где
      .withColumn(("h"),when(Nodet_team_k7m_aux_d_clf_keysU2("f7m_id").isNotNull,Nodet_team_k7m_aux_d_clf_keysU2("f7m_id")).otherwise(col("h3")))
      .select("id","h")
      .distinct()
      .withColumn("cnt",count("id").over(Window.partitionBy("id")))
      .persist()
    //H3
//    Nodet_team_k7m_aux_d_clf_keysD3
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_D3")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_D3")

    //    val Nodet_team_k7m_aux_d_clf_keysS3 = Nodet_team_k7m_aux_d_clf_keysD3
    //      .where($"cnt"===1)
    //      .join(smartSrcHiveTableStage1.selectExpr("id","h1 as f7m_id","h2 as hf2","h3 as hf3","h4 as hf4", "h5 as hf5"),Seq("id"))
    //      .selectExpr("id as link_id","f7m_id","hf2","hf3","hf4","hf5")
    //      .persist()

    val Nodet_team_k7m_aux_d_clf_keysS3 = Nodet_team_k7m_aux_d_clf_keysD3.where($"cnt"===1)
      .join(smartSrcHiveTableStage1,Seq("id"))
      .select(
        smartSrcHiveTableStage1("id").as("link_id"),
        Nodet_team_k7m_aux_d_clf_keysD3("h").as("f7m_id"),
        smartSrcHiveTableStage1("h1").as("hf1"),
        smartSrcHiveTableStage1("h2").as("hf2"),
        smartSrcHiveTableStage1("h3").as("hf3"),
        smartSrcHiveTableStage1("h4").as("hf4"),
        smartSrcHiveTableStage1("h5").as("hf5")
      ).persist()

//    Nodet_team_k7m_aux_d_clf_keysS3
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_S3")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_S3")

    val Nodet_team_k7m_aux_d_clf_keysT3 = Nodet_team_k7m_aux_d_clf_keysD3
      .where($"cnt"===1)
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathHash)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    val Nodet_team_k7m_aux_d_clf_keysB3 = Nodet_team_k7m_aux_d_clf_keysD3
      .where($"cnt".notEqual(lit(1)))
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathBad)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")

    Nodet_team_k7m_aux_d_clf_keysD3.unpersist()

    val Nodet_team_k7m_aux_d_clf_keysU3 = Nodet_team_k7m_aux_d_clf_keysU2.union(Nodet_team_k7m_aux_d_clf_keysS3).distinct().persist()

//    Nodet_team_k7m_aux_d_clf_keysU3
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_U3")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_U3")
    //H4
    val Nodet_team_k7m_aux_d_clf_keysD4 = smartSrcHiveTableStage1 //k4 is not null и кол-во таких id с к4
      .select("id","h4")
      .where($"h4".isNotNull)
      .distinct()
      .join(Nodet_team_k7m_aux_d_clf_keysU3,col("id")===Nodet_team_k7m_aux_d_clf_keysU3("link_id"),"left_anti")
      .join(Nodet_team_k7m_aux_d_clf_keysU3.select("link_id","f7m_id","hf4"),col("h4")===(Nodet_team_k7m_aux_d_clf_keysU3("hf4")),"left")//join c S1, где
      .withColumn(("h"),when(Nodet_team_k7m_aux_d_clf_keysU3("f7m_id").isNotNull,Nodet_team_k7m_aux_d_clf_keysU3("f7m_id")).otherwise(col("h4")))
      .select("id","h")
      .distinct()
      .withColumn("cnt",count("id").over(Window.partitionBy("id")))
      .persist()


//    Nodet_team_k7m_aux_d_clf_keysD4
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_D4")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_D4")


    //    val Nodet_team_k7m_aux_d_clf_keysS4 = Nodet_team_k7m_aux_d_clf_keysD4
    //      .where($"cnt"===1)
    //      .join(smartSrcHiveTableStage1.selectExpr("id","h1 as f7m_id","h2 as hf2","h3 as hf3","h4 as hf4", "h5 as hf5"),Seq("id"))
    //      .selectExpr("id as link_id","f7m_id","hf2","hf3","hf4","hf5")
    //      .persist()
    val Nodet_team_k7m_aux_d_clf_keysS4 = Nodet_team_k7m_aux_d_clf_keysD4.where($"cnt"===1)
      .join(smartSrcHiveTableStage1,Seq("id"))
      .select(
        smartSrcHiveTableStage1("id").as("link_id"),
        Nodet_team_k7m_aux_d_clf_keysD4("h").as("f7m_id"),
        smartSrcHiveTableStage1("h1").as("hf1"),
        smartSrcHiveTableStage1("h2").as("hf2"),
        smartSrcHiveTableStage1("h3").as("hf3"),
        smartSrcHiveTableStage1("h4").as("hf4"),
        smartSrcHiveTableStage1("h5").as("hf5")
      ).persist()

//    Nodet_team_k7m_aux_d_clf_keysS4
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_S4")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_S4")

    val Nodet_team_k7m_aux_d_clf_keysT4 = Nodet_team_k7m_aux_d_clf_keysD4
      .where($"cnt"===1)
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathHash)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    val Nodet_team_k7m_aux_d_clf_keysB4 = Nodet_team_k7m_aux_d_clf_keysD4
      .where($"cnt".notEqual(lit(1)))
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathBad)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")


    val Nodet_team_k7m_aux_d_clf_keysU4 = Nodet_team_k7m_aux_d_clf_keysU3.union(Nodet_team_k7m_aux_d_clf_keysS4).distinct().persist()

    //H5=CRM_ID
    val Nodet_team_k7m_aux_d_clf_keysD5 = smartSrcHiveTableStage1 //k4 is not null и кол-во таких id с к4
      .select("id","h5")
      .where($"h5".isNotNull)
      .distinct()
      .join(Nodet_team_k7m_aux_d_clf_keysU4,col("id")===Nodet_team_k7m_aux_d_clf_keysU4("link_id"),"left_anti")
      .join(Nodet_team_k7m_aux_d_clf_keysU4.select("link_id","f7m_id","hf5"),col("h5")===(Nodet_team_k7m_aux_d_clf_keysU4("hf5")),"left")//join c S1, где
      .withColumn(("h"),when(Nodet_team_k7m_aux_d_clf_keysU4("f7m_id").isNotNull,Nodet_team_k7m_aux_d_clf_keysU4("f7m_id")).otherwise(col("h5")))
      .select("id","h")
      .distinct()
      .withColumn("cnt",count("id").over(Window.partitionBy("id")))
      .persist()


//    Nodet_team_k7m_aux_d_clf_keysD5
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_D5")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_D5")

    val Nodet_team_k7m_aux_d_clf_keysS5 = Nodet_team_k7m_aux_d_clf_keysD5
      .where($"cnt"===1)
      .join(smartSrcHiveTableStage1,Seq("id"))
      .select(
        smartSrcHiveTableStage1("id").as("link_id"),
        Nodet_team_k7m_aux_d_clf_keysD5("h").as("f7m_id"),
        smartSrcHiveTableStage1("h1").as("hf1"),
        smartSrcHiveTableStage1("h2").as("hf2"),
        smartSrcHiveTableStage1("h3").as("hf3"),
        smartSrcHiveTableStage1("h4").as("hf4"),
        smartSrcHiveTableStage1("h5").as("hf5")
      ).persist()

//    Nodet_team_k7m_aux_d_clf_keysS5
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", s"${dashboardPath}_S5")
//      .saveAsTable(s"${Nodet_team_k7m_aux_d_clf_keysOUT}_S5")

    val Nodet_team_k7m_aux_d_clf_keysT5 = Nodet_team_k7m_aux_d_clf_keysD5
      .where($"cnt"===1)
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathHash)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    val Nodet_team_k7m_aux_d_clf_keysB5 = Nodet_team_k7m_aux_d_clf_keysD5
      .where($"cnt".notEqual(lit(1)))
      .selectExpr("id as link_id","h as f7m_id")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathBad)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")

    //   val Nodet_team_k7m_aux_d_clf_keysD5 = spark.table(s"$Node2t_team_k7m_aux_d_clf_keysIN") //k4 is not null и кол-во таких id с к4
    //     .selectExpr("id","crm_id as h5")
    //     .distinct()
    //     .join(Nodet_team_k7m_aux_d_clf_keysU4,col("id")===Nodet_team_k7m_aux_d_clf_keysU4("link_id"),"left_anti")
    //     .join(Nodet_team_k7m_aux_d_clf_keysU4.select("f7m_id","hf5"),col("h5")===(Nodet_team_k7m_aux_d_clf_keysU4("hf5")),"left")//join c S1, где
    //     .withColumn(("h"),when(Nodet_team_k7m_aux_d_clf_keysU4("f7m_id").isNotNull,Nodet_team_k7m_aux_d_clf_keysU4("f7m_id")).otherwise(col("h5")))
    //     .select("id","h")
    //     .distinct()
    //     .withColumn("cnt",count("id").over(Window.partitionBy("id")))
    //     .persist()

    //   val Nodet_team_k7m_aux_d_clf_keysT5 = Nodet_team_k7m_aux_d_clf_keysD5
    //     .where($"cnt"===1)
    //     .selectExpr("id as link_id","h as f7m_id")
    //     .write.format("parquet")
    //     .mode(SaveMode.Append)
    //     .option("path", dashboardPathHash)
    //     .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")

    //   val Nodet_team_k7m_aux_d_clf_keysB5 = Nodet_team_k7m_aux_d_clf_keysD5
    //     .where($"cnt".notEqual(lit(1)))
    //     .selectExpr("id as link_id","h as f7m_id")
    //     .write.format("parquet")
    //     .mode(SaveMode.Append)
    //     .option("path", dashboardPathBad)
    //     .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysBad")

    //    val Nodet_team_k7m_aux_d_clf_keysClfBad = spark.table(s"$Node3t_team_k7m_aux_d_clf_keysIN")
    //      .selectExpr("id as link_id", "id as f7m_id")
    //      .write.format("parquet")
    //      .mode(SaveMode.Append)
    //      .option("path", dashboardPathHash)
    //      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysHash")
    //Было сделано для ссылочной целостности. Дополнение из CLF_BAD тех, что отфильтровались, но это приводит к дублированию первичного ключа!!!
    //    val clf_hash_out = spark.table(Nodet_team_k7m_aux_d_clf_keysHash)
    //    val clf_keys_ki_bad_out = spark.table(Nodet_team_k7m_aux_d_clf_keysBad)
    //    val clf_bad_origin = spark.table(Node3t_team_k7m_aux_d_clf_keysIN)
    //
    //    val union_part = clf_hash_out.union(clf_keys_ki_bad_out)
    //
    //    val clf_bad_other = clf_bad_origin.join(broadcast(union_part), union_part("link_id") === clf_bad_origin("id"), "left_anti")
    //      .select(clf_bad_origin("id").as("link_id"),
    //              clf_bad_origin("id").as("f7m_id"))
    //      .write.format("parquet").insertInto(Nodet_team_k7m_aux_d_clf_keysHash)
    val clf_bad_without_keys = spark.table(Node4t_team_k7m_aux_d_clf_keysIN)
    val clf_bad_other = clf_bad_without_keys
      .select(clf_bad_without_keys("id").as("link_id"),
        clf_bad_without_keys("id").as("f7m_id"))
      .write.format("parquet").insertInto(Nodet_team_k7m_aux_d_clf_keysHash)

    val Nodet_team_k7m_aux_d_clf_keysMap0 = spark.sql(
      s"""select
          f7m_id , cast(0 as string) as map_id
         from $Nodet_team_k7m_aux_d_clf_keysHash
        where 0=1""")
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathMap)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysMap")

    val Nodet_team_k7m_aux_d_clf_keysMap1 = spark.sql(
      s"""select distinct h.f7m_id,  cast(nvl(mm.max_map_id,0) + rank() over (order by h.f7m_id) as string) as map_id
         from (select distinct f7m_id from $Nodet_team_k7m_aux_d_clf_keysHash) h
         left join $Nodet_team_k7m_aux_d_clf_keysMap m
         on m.f7m_id = h.f7m_id
         cross join (select cast(nvl(max(cast(map_id as bigint)),0) as bigint) max_map_id from $Nodet_team_k7m_aux_d_clf_keysMap) mm
        where m.map_id is null """)
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPathMap)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysMap")

    val smartSrcHiveTableStage2 = spark.sql(
      s"""select distinct link_id, map_id as f7m_id
         from $Nodet_team_k7m_aux_d_clf_keysHash h
         join $Nodet_team_k7m_aux_d_clf_keysMap m
        on m.f7m_id = h.f7m_id""")
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_keysOUT")



    smartSrcHiveTableStage1.unpersist()
    Nodet_team_k7m_aux_d_clf_keysD1.unpersist()
    Nodet_team_k7m_aux_d_clf_keysS1.unpersist()
    Nodet_team_k7m_aux_d_clf_keysS2.unpersist()
    Nodet_team_k7m_aux_d_clf_keysS3.unpersist()
    Nodet_team_k7m_aux_d_clf_keysU2.unpersist()
    Nodet_team_k7m_aux_d_clf_keysU3.unpersist()
    Nodet_team_k7m_aux_d_clf_keysU4.unpersist()

    logInserted()
  }
}