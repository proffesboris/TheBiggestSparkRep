package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


class repClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_pa_d_reports_repIN = s"$MartSchema.LK"
    val Node2t_team_k7m_pa_d_reports_repIN = s"$MartSchema.LKC"
    val Node3t_team_k7m_pa_d_reports_repIN = s"$MartSchema.CLU"
    val Node4t_team_k7m_pa_d_reports_repIN = s"$MartSchema.CLF"
    val Node5t_team_k7m_pa_d_reports_repIN = s"$DevSchema.k7m_group_gsz"
    val Node6t_team_k7m_pa_d_reports_repIN = s"$Stg0Schema.rdm_link_criteria_mast"
    val Nodet_team_k7m_pa_d_reports_repOUT = s"$MartSchema.reports_rep"
    val dashboardPath = s"${config.paPath}reports_rep"
    val Nodet_team_k7m_pa_d_reports_repHIST = s"$MartSchema.reports_rep_hist"
    val histPath = s"${config.paPath}reports_rep_hist"
    val Nodet_team_k7m_pa_d_reports_repTMP = s"$DevSchema.reports_rep_hist_tmp"
    val histTmpPath = s"${config.auxPath}reports_rep_hist_tmp"


    override val dashboardName: String = Nodet_team_k7m_pa_d_reports_repOUT //витрина
    override def processName: String = "REP"

    def DoRep() {

      Logger.getLogger(Nodet_team_k7m_pa_d_reports_repOUT).setLevel(Level.WARN)

      logStart()

      def gsz_select_str (site:String):List[String] = {
      List(s"major_id as CRM_TOP_GSZ_ID_$site",
      s"coalesce(gsz_name_4, gsz_name_3,gsz_name_2,gsz_name) as CRM_TOP_GSZ_NAME_ID_$site",
      s"coalesce(c2_id,id) as CRM_GSZ_ID_$site",
      s"coalesce(gsz_name_2, gsz_name) as CRM_GSZ_NAME_ID_$site",
      s"org_crm_id")}

      val repDF = spark.table(Node1t_team_k7m_pa_d_reports_repIN).alias("LK").select("LK_ID", "U7M_ID_FROM", "U7M_ID_TO", "T_FROM", "T_TO","FLAG_NEW")
            .join(spark.table(Node2t_team_k7m_pa_d_reports_repIN).alias("LKC").select("LK_ID","CRIT_ID","LINK_PROB", "QUANTITY")
                  ,Seq("LK_ID"),"inner")
            .join(spark.table(Node6t_team_k7m_pa_d_reports_repIN).alias("CRT").filter(col("rep_flag") === (lit(1))).selectExpr("NM as CRIT_NM", "CODE")
                  ,col("crit_id")===col("code"),"inner")
            .join(spark.table(Node3t_team_k7m_pa_d_reports_repIN).alias("CLUF").selectExpr("FULL_NAME as FULL_NAME_FROM_U", "U7M_ID", "CRM_ID", "INN as INN_FROM_U")
              ,col("LK.T_FROM")===lit("CLU") && col("lk.U7M_ID_FROM")===col("CLUF.u7m_id"),"left")
            .join(spark.table(Node3t_team_k7m_pa_d_reports_repIN).alias("CLUT").selectExpr("FULL_NAME as FULL_NAME_TO_U", "U7M_ID", "CRM_ID", "INN as INN_TO_U")
              ,col("LK.T_TO")===lit("CLU") && col("lk.U7M_ID_TO")===col("CLUT.u7m_id"),"left")
            .join(spark.table(Node4t_team_k7m_pa_d_reports_repIN).alias("CLFF").selectExpr("F7M_ID", "concat(S_NAME,' ',F_NAME,' ',L_Name) as FULL_NAME_FROM_F", "INN as INN_FROM_F", "CRM_ID")
              ,col("LK.T_FROM")===lit("CLF") && col("lk.U7M_ID_FROM")===col("CLFF.F7M_ID"),"left")
            .join(spark.table(Node4t_team_k7m_pa_d_reports_repIN).alias("CLFT").selectExpr("F7M_ID", "concat(S_NAME,' ',F_NAME,' ',L_Name) as FULL_NAME_TO_F", "INN as INN_TO_F", "CRM_ID")
              ,col("LK.T_TO")===lit("CLF") && col("lk.U7M_ID_TO")===col("CLFT.F7M_ID"),"left")
            .join(spark.table(Node5t_team_k7m_pa_d_reports_repIN).alias("GSZF").selectExpr( gsz_select_str(site="FROM"): _* )
                  ,col("GSZF.org_crm_id") === col("CLUF.crm_id"),"left")
            .join(spark.table(Node5t_team_k7m_pa_d_reports_repIN).alias("GSZT").selectExpr(gsz_select_str(site="TO"): _* )
                  ,col("GSZT.org_crm_id") === col("CLUT.crm_id"),"left")
          .selectExpr("case when CRM_TOP_GSZ_ID_FROM <> CRM_GSZ_ID_FROM then CRM_TOP_GSZ_ID_FROM end CRM_TOP_GSZ_ID_FROM",
                      "case when CRM_TOP_GSZ_ID_FROM <> CRM_GSZ_ID_FROM then CRM_TOP_GSZ_NAME_ID_FROM end CRM_TOP_GSZ_NAME_ID_FROM",
                      "CRM_GSZ_ID_FROM","CRM_GSZ_NAME_ID_FROM",
                      "U7M_ID_FROM", "nvl(CLUF.CRM_ID,CLFF.CRM_ID) as CRM_ID_FROM",
                      "nvl(FULL_NAME_FROM_U,FULL_NAME_FROM_F) as FROM_NM",
                      "nvl(INN_FROM_U,INN_FROM_F) as INN_FROM",
                      "CRIT_ID","FLAG_NEW",
                      "concat(CRIT_NM,case when quantity is not null then concat(' (',cast(round(QUANTITY*100,2) as string),')') end ) CRIT_NM",
                      "QUANTITY",
                      "U7M_ID_TO", "nvl(CLUT.CRM_ID,CLFT.CRM_ID) as CRM_ID_TO",
                      "nvl(INN_TO_U,INN_TO_F) as INN_TO",
                      "nvl(FULL_NAME_TO_U,FULL_NAME_TO_F) as TO_NM",
                      "case when CRM_TOP_GSZ_ID_TO <> CRM_GSZ_ID_TO then CRM_TOP_GSZ_ID_TO end CRM_TOP_GSZ_ID_TO",
                      "case when CRM_TOP_GSZ_ID_TO <> CRM_GSZ_ID_TO then CRM_TOP_GSZ_NAME_ID_TO end CRM_TOP_GSZ_NAME_ID_TO",
                      "CRM_GSZ_ID_TO","CRM_GSZ_NAME_ID_TO ",
                      "LINK_PROB"
          )
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repOUT")

      logInserted()

      val repHistDF = spark.table(s"$Nodet_team_k7m_pa_d_reports_repOUT")
                           .where("1=0")
                           .withColumn("from_ts",current_timestamp())
                           .withColumn("to_ts",current_timestamp())
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .option("path", histPath)
        .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repHIST")

      val actualTS = LoggerUtils.obtainRunDtTimestamp(spark,DevSchema)

      val greatestDt = spark.table(Nodet_team_k7m_pa_d_reports_repHIST)
                                  .selectExpr("max(nvl(to_ts,from_ts)) dt")
                                  .withColumn("dt2",when(col("dt").isNull,lit(actualTS)).otherwise(col("dt")))
                                  .drop(col("dt")).first().getTimestamp(0)

      if (! actualTS.before(greatestDt)) {

        val repTmpDF1 = spark.table(s"$Nodet_team_k7m_pa_d_reports_repHIST")
          .where(col("to_ts") < lit(actualTS))
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .option("path", histTmpPath)
          .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repTMP")

        val repTmpDF2 = spark.table(s"$Nodet_team_k7m_pa_d_reports_repHIST").as("h")
          .where(when(col("to_ts").isNull, lit(actualTS)).otherwise(col("to_ts")) === lit(actualTS))
          .drop(col("to_ts"))
          .withColumn("to_ts", lit(actualTS))
          .join(spark.table(s"$Nodet_team_k7m_pa_d_reports_repOUT").as("r"),
              when(col("h.CRM_TOP_GSZ_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_ID_FROM")) === when(col("r.CRM_TOP_GSZ_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_ID_FROM")) &&
              when(col("h.CRM_TOP_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_NAME_ID_FROM")) === when(col("r.CRM_TOP_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_FROM")) &&
              when(col("h.CRM_GSZ_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_GSZ_ID_FROM")) === when(col("r.CRM_GSZ_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_GSZ_ID_FROM")) &&
              when(col("h.CRM_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_GSZ_NAME_ID_FROM")) === when(col("r.CRM_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_GSZ_NAME_ID_FROM")) &&
              when(col("h.U7M_ID_FROM").isNull, lit("")).otherwise(col("h.U7M_ID_FROM")) === when(col("r.U7M_ID_FROM").isNull, lit("")).otherwise(col("r.U7M_ID_FROM")) &&
              when(col("h.CRM_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_ID_FROM")) === when(col("r.CRM_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_ID_FROM")) &&
              when(col("h.FROM_NM").isNull, lit("")).otherwise(col("h.FROM_NM")) === when(col("r.FROM_NM").isNull, lit("")).otherwise(col("r.FROM_NM")) &&
              when(col("h.INN_FROM").isNull, lit("")).otherwise(col("h.INN_FROM")) === when(col("r.INN_FROM").isNull, lit("")).otherwise(col("r.INN_FROM")) &&
              when(col("h.CRIT_ID").isNull, lit("")).otherwise(col("h.CRIT_ID")) === when(col("r.CRIT_ID").isNull, lit("")).otherwise(col("r.CRIT_ID")) &&
              when(col("h.FLAG_NEW").isNull, lit(0)).otherwise(col("h.FLAG_NEW")) === when(col("r.FLAG_NEW").isNull, lit(0)).otherwise(col("r.FLAG_NEW")) &&
              when(col("h.CRIT_NM").isNull, lit("")).otherwise(col("h.CRIT_NM")) === when(col("r.CRIT_NM").isNull, lit("")).otherwise(col("r.CRIT_NM")) &&
              when(col("h.QUANTITY").isNull, lit(0)).otherwise(col("h.QUANTITY")) === when(col("r.QUANTITY").isNull, lit(0)).otherwise(col("r.QUANTITY")) &&
              when(col("h.U7M_ID_TO").isNull, lit("")).otherwise(col("h.U7M_ID_TO")) === when(col("r.U7M_ID_TO").isNull, lit("")).otherwise(col("r.U7M_ID_TO")) &&
              when(col("h.CRM_ID_TO").isNull, lit("")).otherwise(col("h.CRM_ID_TO")) === when(col("r.CRM_ID_TO").isNull, lit("")).otherwise(col("r.CRM_ID_TO")) &&
              when(col("h.INN_TO").isNull, lit("")).otherwise(col("h.INN_TO")) === when(col("r.INN_TO").isNull, lit("")).otherwise(col("r.INN_TO")) &&
              when(col("h.TO_NM").isNull, lit("")).otherwise(col("h.TO_NM")) === when(col("r.TO_NM").isNull, lit("")).otherwise(col("r.TO_NM")) &&
              when(col("h.CRM_TOP_GSZ_ID_TO").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_ID_TO")) === when(col("r.CRM_TOP_GSZ_ID_TO").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_ID_TO")) &&
              when(col("h.CRM_TOP_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_NAME_ID_TO")) === when(col("r.CRM_TOP_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_TO")) &&
              when(col("h.CRM_GSZ_ID_TO").isNull, lit("")).otherwise(col("h.CRM_GSZ_ID_TO")) === when(col("r.CRM_GSZ_ID_TO").isNull, lit("")).otherwise(col("r.CRM_GSZ_ID_TO")) &&
              when(col("h.CRM_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("h.CRM_GSZ_NAME_ID_TO")) === when(col("r.CRM_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("r.CRM_GSZ_NAME_ID_TO")) &&
              when(col("h.LINK_PROB").isNull, lit(0)).otherwise(col("h.LINK_PROB")) === when(col("r.LINK_PROB").isNull, lit(0)).otherwise(col("r.LINK_PROB"))
              ,
                "left_anti")
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .option("path", histTmpPath)
          .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repTMP")

        val repTmpDF3 = spark.table(s"$Nodet_team_k7m_pa_d_reports_repOUT").as("r")
          .join(spark.table(s"$Nodet_team_k7m_pa_d_reports_repHIST").as("h"),
            when(col("h.CRM_TOP_GSZ_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_ID_FROM")) === when(col("r.CRM_TOP_GSZ_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_ID_FROM")) &&
              when(col("h.CRM_TOP_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_NAME_ID_FROM")) === when(col("r.CRM_TOP_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_FROM")) &&
              when(col("h.CRM_GSZ_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_GSZ_ID_FROM")) === when(col("r.CRM_GSZ_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_GSZ_ID_FROM")) &&
              when(col("h.CRM_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_GSZ_NAME_ID_FROM")) === when(col("r.CRM_GSZ_NAME_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_GSZ_NAME_ID_FROM")) &&
              when(col("h.U7M_ID_FROM").isNull, lit("")).otherwise(col("h.U7M_ID_FROM")) === when(col("r.U7M_ID_FROM").isNull, lit("")).otherwise(col("r.U7M_ID_FROM")) &&
              when(col("h.CRM_ID_FROM").isNull, lit("")).otherwise(col("h.CRM_ID_FROM")) === when(col("r.CRM_ID_FROM").isNull, lit("")).otherwise(col("r.CRM_ID_FROM")) &&
              when(col("h.FROM_NM").isNull, lit("")).otherwise(col("h.FROM_NM")) === when(col("r.FROM_NM").isNull, lit("")).otherwise(col("r.FROM_NM")) &&
              when(col("h.INN_FROM").isNull, lit("")).otherwise(col("h.INN_FROM")) === when(col("r.INN_FROM").isNull, lit("")).otherwise(col("r.INN_FROM")) &&
              when(col("h.CRIT_ID").isNull, lit("")).otherwise(col("h.CRIT_ID")) === when(col("r.CRIT_ID").isNull, lit("")).otherwise(col("r.CRIT_ID")) &&
              when(col("h.FLAG_NEW").isNull, lit(0)).otherwise(col("h.FLAG_NEW")) === when(col("r.FLAG_NEW").isNull, lit(0)).otherwise(col("r.FLAG_NEW")) &&
              when(col("h.CRIT_NM").isNull, lit("")).otherwise(col("h.CRIT_NM")) === when(col("r.CRIT_NM").isNull, lit("")).otherwise(col("r.CRIT_NM")) &&
              when(col("h.QUANTITY").isNull, lit(0)).otherwise(col("h.QUANTITY")) === when(col("r.QUANTITY").isNull, lit(0)).otherwise(col("r.QUANTITY")) &&
              when(col("h.U7M_ID_TO").isNull, lit("")).otherwise(col("h.U7M_ID_TO")) === when(col("r.U7M_ID_TO").isNull, lit("")).otherwise(col("r.U7M_ID_TO")) &&
              when(col("h.CRM_ID_TO").isNull, lit("")).otherwise(col("h.CRM_ID_TO")) === when(col("r.CRM_ID_TO").isNull, lit("")).otherwise(col("r.CRM_ID_TO")) &&
              when(col("h.INN_TO").isNull, lit("")).otherwise(col("h.INN_TO")) === when(col("r.INN_TO").isNull, lit("")).otherwise(col("r.INN_TO")) &&
              when(col("h.TO_NM").isNull, lit("")).otherwise(col("h.TO_NM")) === when(col("r.TO_NM").isNull, lit("")).otherwise(col("r.TO_NM")) &&
              when(col("h.CRM_TOP_GSZ_ID_TO").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_ID_TO")) === when(col("r.CRM_TOP_GSZ_ID_TO").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_ID_TO")) &&
              when(col("h.CRM_TOP_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("h.CRM_TOP_GSZ_NAME_ID_TO")) === when(col("r.CRM_TOP_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_TO")) &&
              when(col("h.CRM_GSZ_ID_TO").isNull, lit("")).otherwise(col("h.CRM_GSZ_ID_TO")) === when(col("r.CRM_GSZ_ID_TO").isNull, lit("")).otherwise(col("r.CRM_GSZ_ID_TO")) &&
              when(col("h.CRM_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("h.CRM_GSZ_NAME_ID_TO")) === when(col("r.CRM_GSZ_NAME_ID_TO").isNull, lit("")).otherwise(col("r.CRM_GSZ_NAME_ID_TO")) &&
              when(col("h.LINK_PROB").isNull, lit(0)).otherwise(col("h.LINK_PROB")) === when(col("r.LINK_PROB").isNull, lit(0)).otherwise(col("r.LINK_PROB")) &&
              when(col("to_ts").isNull, lit(actualTS)).otherwise(col("to_ts")) === lit(actualTS)
            ,
            "left")
          .withColumn("from_ts2", when(col("from_ts").isNull, lit(actualTS)).otherwise(col("from_ts")))
          .drop(col("to_ts"))
          .withColumn( "to_ts",lit("").cast("Timestamp"))
          .select(when(col("r.CRM_TOP_GSZ_ID_FROM").isNull, col("h.CRM_TOP_GSZ_ID_FROM")).otherwise(col("r.CRM_TOP_GSZ_ID_FROM")).as("CRM_TOP_GSZ_ID_FROM"),
            when(col("r.CRM_TOP_GSZ_NAME_ID_FROM").isNull, col("h.CRM_TOP_GSZ_NAME_ID_FROM")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_FROM")).as("CRM_TOP_GSZ_NAME_ID_FROM"),
            when(col("r.CRM_GSZ_ID_FROM").isNull, col("h.CRM_GSZ_ID_FROM")).otherwise(col("r.CRM_GSZ_ID_FROM")).as("CRM_GSZ_ID_FROM"),
            when(col("r.CRM_GSZ_NAME_ID_FROM").isNull, col("h.CRM_GSZ_NAME_ID_FROM")).otherwise(col("r.CRM_GSZ_NAME_ID_FROM")).as("CRM_GSZ_NAME_ID_FROM"),
            when(col("r.U7M_ID_FROM").isNull, col("h.U7M_ID_FROM")).otherwise(col("r.U7M_ID_FROM")).as("U7M_ID_FROM"),
            when(col("r.CRM_ID_FROM").isNull, col("h.CRM_ID_FROM")).otherwise(col("r.CRM_ID_FROM")).as("CRM_ID_FROM"),
            when(col("r.FROM_NM").isNull, col("h.FROM_NM")).otherwise(col("r.FROM_NM")).as("FROM_NM"),
            when(col("r.INN_FROM").isNull, col("h.INN_FROM")).otherwise(col("r.INN_FROM")).as("INN_FROM"),
            when(col("r.CRIT_ID").isNull, col("h.CRIT_ID")).otherwise(col("r.CRIT_ID")).as("CRIT_ID"),
            when(col("r.FLAG_NEW").isNull, col("h.FLAG_NEW")).otherwise(col("r.FLAG_NEW")).as("FLAG_NEW"),
            when(col("r.CRIT_NM").isNull, col("h.CRIT_NM")).otherwise(col("r.CRIT_NM")).as("CRIT_NM"),
            when(col("r.QUANTITY").isNull, col("h.QUANTITY")).otherwise(col("r.QUANTITY")).as("QUANTITY"),
            when(col("r.U7M_ID_TO").isNull, col("h.U7M_ID_TO")).otherwise(col("r.U7M_ID_TO")).as("U7M_ID_TO"),
            when(col("r.CRM_ID_TO").isNull, col("h.CRM_ID_TO")).otherwise(col("r.CRM_ID_TO")).as("CRM_ID_TO"),
            when(col("r.INN_TO").isNull, col("h.INN_TO")).otherwise(col("r.INN_TO")).as("INN_TO"),
            when(col("r.TO_NM").isNull, col("h.TO_NM")).otherwise(col("r.TO_NM")).as("TO_NM"),
            when(col("r.CRM_TOP_GSZ_ID_TO").isNull, col("h.CRM_TOP_GSZ_ID_TO")).otherwise(col("r.CRM_TOP_GSZ_ID_TO")).as("CRM_TOP_GSZ_ID_TO"),
            when(col("r.CRM_TOP_GSZ_NAME_ID_TO").isNull, col("h.CRM_TOP_GSZ_NAME_ID_TO")).otherwise(col("r.CRM_TOP_GSZ_NAME_ID_TO")).as("CRM_TOP_GSZ_NAME_ID_TO"),
            when(col("r.CRM_GSZ_ID_TO").isNull, col("h.CRM_GSZ_ID_TO")).otherwise(col("r.CRM_GSZ_ID_TO")).as("CRM_GSZ_ID_TO"),
            when(col("r.CRM_GSZ_NAME_ID_TO").isNull, col("h.CRM_GSZ_NAME_ID_TO")).otherwise(col("r.CRM_GSZ_NAME_ID_TO")).as("CRM_GSZ_NAME_ID_TO"),
            when(col("r.LINK_PROB").isNull, col("h.LINK_PROB")).otherwise(col("r.LINK_PROB")).as("LINK_PROB"),
            col("from_ts2").as("from_ts"),
            col("to_ts"))
          .write
          .format("parquet")
          .mode(SaveMode.Append)
          .option("path", histTmpPath)
          .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repTMP")


        val repHistDF = spark.table(s"$Nodet_team_k7m_pa_d_reports_repTMP")
          .write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .option("path", histPath)
          .saveAsTable(s"$Nodet_team_k7m_pa_d_reports_repHIST")
      }
      logEnd()
    }
}

