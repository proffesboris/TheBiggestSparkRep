package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import java.time.LocalDate
import java.sql.Date


class exsGenClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_exsgenIN = s"${DevSchema}.basis_client"
    val Node2t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_client"
    val Node3t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_pr_cred"
    val Node4t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_product"
    val Node5t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_part_to_loan"
    val Node6t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_zalog"
    val Node7t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_ac_fin"
    val Node8t_team_k7m_aux_d_exsgenIN = s"${Stg0Schema}.eks_z_cl_priv"
    val Nodet_team_k7m_aux_d_exsgenOUT = s"${DevSchema}.exsgen"
    val dashboardPath = s"${config.auxPath}exsgen"


    override val dashboardName: String = Nodet_team_k7m_aux_d_exsgenOUT //витрина
    override def processName: String = "EXSGen"

    def DoEXSGen(dateString: String) {

      Logger.getLogger(Nodet_team_k7m_aux_d_exsgenOUT).setLevel(Level.WARN)

      logStart()

      import ExsGenMain._

      val date = Date.valueOf(dateString)

      val dateMinus3Yr = Date.valueOf(LocalDate.parse(dateString).minusYears(3))

      val Nodet_team_k7m_aux_d_exsgenPREP = Nodet_team_k7m_aux_d_exsgenOUT.concat("_prep")

      /*val readHiveTable1 = spark.table(s"$Node1t_team_k7m_aux_d_exsgenIN").select("org_crm_id","org_inn_crm_num","org_crm_name")
                                                                          .withColumnRenamed("org_crm_id","to_crm_id")
                                                                          .withColumnRenamed("org_inn_crm_num","to_inn")
                                                                          .withColumnRenamed("org_crm_name","to_name")
      val readHiveTable2 = spark.table(s"$Node2t_team_k7m_aux_d_exsgenIN").select("id","c_inn")
      val readHiveTable3 = spark.table(s"$Node3t_team_k7m_aux_d_exsgenIN").select("id","C_CLIENT").withColumnRenamed("id","cred_id")
      val readHiveTable4 = spark.table(s"$Node4t_team_k7m_aux_d_exsgenIN").select("id","c_date_begin","c_date_begining") //c_date_begnvl")
                                                                          .withColumn("c_date_begnvl",when(col("c_date_begin").isNotNull,col("c_date_begin"))
                                                                                                      .otherwise(col("c_date_begining")))
      val readHiveTable5 = spark.table(s"$Node5t_team_k7m_aux_d_exsgenIN").select("c_product","collection_id")
      val readHiveTable6 = spark.table(s"$Node6t_team_k7m_aux_d_exsgenIN").select("c_part_to_loan","c_acc_zalog","c_user_zalog")
      val readHiveTable7 = spark.table(s"$Node7t_team_k7m_aux_d_exsgenIN").select("id","c_main_v_id")
      val readHiveTable8 = spark.table(s"$Node2t_team_k7m_aux_d_exsgenIN").select("id","c_inn","c_name","class_id")
                                                                          .withColumnRenamed("id","from_eks_id")
                                                                          .withColumnRenamed("c_inn","from_inn")
                                                                          .withColumnRenamed("c_name","from_name")
                                                                          .withColumnRenamed("class_id","from_class")

      val createHiveTableStage1 = readHiveTable1.join(readHiveTable2, readHiveTable1("to_inn") === readHiveTable2("c_inn"))
                                                .join(readHiveTable3, readHiveTable2("id") === readHiveTable3("C_CLIENT"))
                                                .join(readHiveTable4, readHiveTable3("cred_id") === readHiveTable4("id"))
                                                .join(readHiveTable5, readHiveTable3("cred_id") === readHiveTable5("c_product"))
                                                .join(readHiveTable6, readHiveTable5("collection_id") === readHiveTable6("c_part_to_loan"))
                                                .join(readHiveTable7, readHiveTable6("c_acc_zalog") === readHiveTable7("id"))
                                                .join(readHiveTable8, readHiveTable6("c_user_zalog") === readHiveTable8("from_eks_id"))
                                  .filter(readHiveTable7("c_main_v_id").like(mainVId))
                                  .filter(to_date(readHiveTable4("c_date_begnvl")).leq(lit(date)))
                                  .filter(to_date(readHiveTable4("c_date_begnvl")).gt(lit(dateMinus3Yr)))
                                  .withColumn("crit_id",lit("exsgen"))
                                  .select("crit_id","from_eks_id","from_inn","from_name","from_class","to_crm_id","to_inn","to_name","cred_id")
                                  .distinct()
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath.concat("_prep"))
        .saveAsTable(s"$Nodet_team_k7m_aux_d_exsgenPREP")*/

      val readHiveTable21 = spark.table(s"$Node1t_team_k7m_aux_d_exsgenIN").select("org_crm_id","org_inn_crm_num","org_crm_name")
        .withColumnRenamed("org_crm_id","to_crm_id")
        .withColumnRenamed("org_inn_crm_num","to_inn")
        .withColumnRenamed("org_crm_name","to_name")
      val readHiveTable22 = spark.table(s"$Node2t_team_k7m_aux_d_exsgenIN").select("id","c_inn").withColumnRenamed("id","client_id")
      val readHiveTable23 = spark.table(s"$Node3t_team_k7m_aux_d_exsgenIN").select("id","C_CLIENT").withColumnRenamed("id","cred_id")
      val readHiveTable25 = spark.table(s"$Node5t_team_k7m_aux_d_exsgenIN").select("c_product","collection_id")
      val readHiveTable26 = spark.table(s"$Node6t_team_k7m_aux_d_exsgenIN").select("id","c_part_to_loan","c_acc_zalog","c_user_zalog").withColumnRenamed("id","zal_id")
      val readHiveTable27 = spark.table(s"$Node7t_team_k7m_aux_d_exsgenIN").select("id","c_main_v_id")
      val readHiveTable28 = spark.table(s"$Node2t_team_k7m_aux_d_exsgenIN").select("id","c_inn","c_name","class_id")
        .withColumnRenamed("id","from_eks_id")
        .withColumnRenamed("c_inn","from_inn")
        .withColumnRenamed("c_name","from_name")
        .withColumnRenamed("class_id","from_class")

      val createHiveTableStage2 = readHiveTable21.join(readHiveTable22, readHiveTable21("to_inn") === readHiveTable22("c_inn"))
        .join(readHiveTable23, readHiveTable22("client_id") === readHiveTable23("C_CLIENT"))
        .join(readHiveTable25, readHiveTable23("cred_id") === readHiveTable25("c_product"))
        .join(readHiveTable26, readHiveTable25("collection_id") === readHiveTable26("c_part_to_loan"))
        .join(readHiveTable27, readHiveTable26("c_acc_zalog") === readHiveTable27("id"))
        .join(readHiveTable28, readHiveTable26("c_user_zalog") === readHiveTable28("from_eks_id"))
        .filter(readHiveTable22("client_id").notEqual(readHiveTable28("from_eks_id")))
        .filter(to_date(readHiveTable28("from_class")).notEqual(lit(s"${clPriv}")) || readHiveTable28("from_inn").notEqual(readHiveTable21("to_inn")))
        .withColumn("crit_id",concat(lit(s"exsgen_set"),when(readHiveTable26("zal_id").isNotNull && !readHiveTable27("c_main_v_id").like(mainVId),lit(s"_zal")).otherwise(lit(s"_sur"))))
        .select("crit_id","from_eks_id","from_inn","from_name","from_class","to_crm_id","to_inn","to_name","cred_id")
        .distinct()
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath.concat("_prep"))
        .saveAsTable(s"$Nodet_team_k7m_aux_d_exsgenPREP")

      val readExsTableCLU = spark.table(Nodet_team_k7m_aux_d_exsgenPREP).selectExpr("crit_id","cred_id","from_class", "'CLU' from_obj", "from_eks_id", "from_inn", "from_name",
                                                                                    "cast(null as timestamp) from_date_of_birth", "cast(null as string) from_doc_num",
                                                                                    "'CLU' to_obj",      "to_crm_id",      "to_inn",      "to_name")
      val readExsTableCLF = spark.table(Nodet_team_k7m_aux_d_exsgenPREP).selectExpr("crit_id","cred_id","from_class", "'CLF' from_obj", "from_eks_id", "from_inn", "from_name",
                                                                                    "'CLU' to_obj",      "to_crm_id",      "to_inn",      "to_name")
      val readClpTableCLF = spark.table(Node8t_team_k7m_aux_d_exsgenIN).selectExpr("id", "c_date_pers from_date_of_birth", "concat(c_doc_ser,c_doc_num) from_doc_num")

      val createHiveTableStage3 = readExsTableCLU
          .filter(readExsTableCLU("from_class").notEqual(lit(clPriv)))
          .select("crit_id","cred_id","from_obj", "from_eks_id", "from_inn", "from_name",
            "from_date_of_birth", "from_doc_num",
            "to_obj", "to_crm_id", "to_inn", "to_name")
        .union(
          readExsTableCLF.filter(readExsTableCLF("from_class").like(s"${clPriv}"))
            .join(readClpTableCLF,readExsTableCLF("from_eks_id") === readClpTableCLF("id"))
            .select("crit_id","cred_id","from_obj", "from_eks_id", "from_inn", "from_name",
              "from_date_of_birth", "from_doc_num",
              "to_obj", "to_crm_id", "to_inn", "to_name"))
        .withColumn("loc_id", row_number().over(Window.orderBy("from_inn","to_inn")))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_exsgenOUT")

      logInserted()
      logEnd()
    }
}

