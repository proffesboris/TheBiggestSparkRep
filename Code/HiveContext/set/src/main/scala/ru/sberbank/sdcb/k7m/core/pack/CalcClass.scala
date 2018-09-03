package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

class CalcClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_t_cred_pcIN = s"${DevSchema}.set_paym"
    val Node2t_team_k7m_aux_t_cred_pcIN = s"${DevSchema}.set_wro"
    val Node3t_team_k7m_aux_t_cred_pcIN = s"${DevSchema}.set_cred_enriched"
    val Node4t_team_k7m_aux_t_cred_pcIN = s"${DevSchema}.eks_z_ft_money"
    val Nodet_team_k7m_aux_t_cred_pcOUT = s"${DevSchema}.set_bo_agg2"
    val dashboardPath = s"${config.auxPath}set_bo_agg2"


    override val dashboardName: String = Nodet_team_k7m_aux_t_cred_pcOUT //витрина
    override def processName: String = "SET"

    def DoCalc() {

    import spark.implicits._

      Logger.getLogger(Nodet_team_k7m_aux_t_cred_pcOUT).setLevel(Level.WARN)

      logStart()


     // In[3]:

      //Згрузим данные из HDFS в датафреймы-Pandas-
      val wo =  spark.table(Node2t_team_k7m_aux_t_cred_pcIN)
        .withColumn("c_date_notunix", $"c_date")

      val deals =  spark.table(Node3t_team_k7m_aux_t_cred_pcIN)

      val tr = spark.table(Node1t_team_k7m_aux_t_cred_pcIN)
        .withColumn("c_date_notunix", $"c_date")
        .join(deals,Seq("pr_cred_id"),"left_semi")

      val trs = tr.groupBy($"pr_cred_id").agg(sum("c_summa").as("c_summa"),
                                              sum("c_summa_in_cr_v").as("c_summa_in_cr_v"),
        min("c_date_notunix").as("start_date"),
        max("c_date_notunix").as("end_date"),
        count("c_summa_in_cr_v").as("count"))

      val agg = deals.join(trs, Seq("pr_cred_id"), "left")

      val trs_wo = wo.groupBy($"pr_cred_id").agg(sum("c_summa").as("wo_summa"),sum("c_summa_in_cr_v").as("wo_summa_in_cr_v"))

      val agg2 = agg.join(trs_wo, Seq("pr_cred_id"), "left")
        .withColumn("wo_summa",when($"wo_summa".isNotNull,$"wo_summa").otherwise(lit(0)))
        .withColumn("wo_summa_in_cr_v",when($"wo_summa_in_cr_v".isNotNull,$"wo_summa_in_cr_v").otherwise(lit(0)))


      val tr2 = tr
        .withColumn("total", sum($"c_summa").over(Window.partitionBy("pr_cred_id")))
        .withColumn("total_in_cr_v", sum($"c_summa_in_cr_v").over(Window.partitionBy("pr_cred_id")))
        .withColumn("cumsum", sum($"c_summa").over(Window.partitionBy("pr_cred_id").orderBy("c_date_notunix")))
        .withColumn("cumsum_in_cr_v", sum($"c_summa_in_cr_v").over(Window.partitionBy("pr_cred_id").orderBy("c_date_notunix")))
        .withColumn("alpha_005", ($"cumsum_in_cr_v">$"total_in_cr_v"*lit(0.05)).cast("boolean"))
        .withColumn("alpha_01", ($"cumsum_in_cr_v">$"total_in_cr_v"*lit(0.1)).cast("boolean"))
        .withColumn("alpha_015", ($"cumsum_in_cr_v">$"total_in_cr_v"*lit(0.15)).cast("boolean")).persist()

      logInserted(count = tr2.count())


      // In[13]:

     //для  5%, 10%, 15% уровеня подсчитаем сумму до и дату


      val trs2 = tr2.groupBy("pr_cred_id","total").agg(
        min(when($"alpha_005",$"c_summa_in_cr_v")).as("alpha_005_cumsum"),
        min(when($"alpha_01",$"c_summa_in_cr_v")).as("alpha_01_cumsum"),
        min(when($"alpha_015",$"c_summa_in_cr_v")).as("alpha_015_cumsum"),
        min(when($"alpha_005",$"c_date_notunix")).as("alpha_005_date"),
        min(when($"alpha_01",$"c_date_notunix")).as("alpha_01_date"),
        min(when($"alpha_015",$"c_date_notunix")).as("alpha_015_date")
      )

      val agg3 = agg2.join(trs2, Seq("pr_cred_id"), "left")

      // In[14]:

      //определим баллон, и его дату

      val trgr1 = tr2.groupBy("pr_cred_id")
        .agg(max($"c_date_notunix").as("balon_date"))

      val agg4 = agg3.join(trgr1, Seq("pr_cred_id"), "left")
        .withColumn("balon_date_fill",when($"balon_date".isNull,$"c_date_ending").otherwise($"balon_date"))
          .drop("balon_date")
          .withColumnRenamed("balon_date_fill","balon_date")
        .withColumn("days_after_balon",(datediff($"balon_date",$"ddog")*lit(0.9).cast("Int")))
        .withColumn("date_balon_starts",expr("date_add(ddog,days_after_balon)"))

      //здесь важно не перепутать, tr3 в цепочке, а trgr1 и trgr2 независимо ответвляется
      val tr3 = tr2.join(agg4.select("pr_cred_id", "date_balon_starts"),Seq("pr_cred_id"),"left")
        .withColumn("isin_balon",($"c_date_notunix">=$"date_balon_starts").cast("boolean"))
        .withColumn("balon_part",when($"isin_balon","c_summa_in_cr_v"))

      val trgr2 = tr3.groupBy("pr_cred_id").agg(sum($"balon_part").as("balon_sum"))

      val agg5 = agg4.join(trgr2, Seq("pr_cred_id"), "left")

      // In[15]:

      //переведем даты в дистанции от ddog
      val agg6 = agg5.withColumn("days_over_lim",(datediff($"c_date_ending",$"end_date").cast("Int")))


      // In[16]:
      val agg7 = agg6.withColumn("start_days",(datediff($"start_date",$"ddog").cast("Int")))
        .withColumn("end_days",(datediff($"end_date",$"ddog").cast("Int")))


      // In[17]:

      val agg8 = agg7
      .withColumn("alpha_005_days",(datediff($"alpha_005_date",$"ddog").cast("Int")))
      .withColumn("alpha_01_days",(datediff($"alpha_01_date",$"ddog").cast("Int")))
      .withColumn("alpha_015_days",(datediff($"alpha_015_date",$"ddog").cast("Int")))
      .withColumn("balon_days",(datediff($"balon_date",$"ddog").cast("Int")))
      .persist()

      logInserted(count = agg8.count())
      // In[18]:

      //определим параметры гаммы
      val tr4 = tr3.withColumn("graph_y",lit(1)-($"cumsum_in_cr_v"-$"c_summa_in_cr_v")/$"total_in_cr_v")
      .join(deals.select("pr_cred_id","ddog", "dur_in_days"), Seq("pr_cred_id"))
       .withColumn("graph_x",datediff($"c_date_notunix",$"ddog").cast("int")/$"dur_in_days")
        .withColumn("ro", sqrt( pow(lit(1)-$"graph_y",2) + pow(lit(1)-$"graph_x",2)))
        .withColumn("ro_min", min($"ro").over(Window.partitionBy("pr_cred_id")))
        .persist()

      logInserted(count = tr4.count())

      val agg9 = agg8.join(tr4.filter($"ro_min"===$"ro").select("pr_cred_id", "graph_x", "graph_y"), Seq("pr_cred_id"), "left")
        .withColumn("bias",$"graph_x" + $"graph_y")
        .withColumn("gamma", lit(-1) + $"bias")
        .distinct()


      // In[19]:

      //переведем суммы во флоат
      val agg10 = agg9.withColumn("summa_ru_d",$"summa_ru".cast("float")).drop("summa_ru").withColumnRenamed("summa_ru_d","summa_ru")
        .withColumn("summa_base_d",$"summa_base".cast("float")).drop("summa_base").withColumnRenamed("summa_base_d","summa_base")


      // In[20]:

      //определим накопленные суммы в долях от суммы
      val agg11 = agg10
      .withColumn("alpha_005_cumsum_t",$"alpha_005_cumsum"/$"total")
      .withColumn("alpha_01_cumsum_t",$"alpha_01_cumsum"/$"total")
      .withColumn("alpha_015_cumsum_t",$"alpha_015_cumsum"/$"total")
      .withColumn("balon_sum_t",$"balon_sum"/$"total")
      .withColumn("alpha_005_cumsum_d",$"alpha_005_cumsum"/$"summa_base")
      .withColumn("alpha_01_cumsum_d",$"alpha_01_cumsum"/$"summa_base")
      .withColumn("alpha_015_cumsum_d",$"alpha_015_cumsum"/$"summa_base")
      .withColumn("balon_sum_d",$"balon_sum"/$"summa_base")


      // In[21]:

      //а сроки в полях от дюрации
      val agg12 = agg11
        .withColumn("start_days_proc", $"start_days"/$"dur_in_days").drop("start_date").drop("start_days").withColumnRenamed("start_days_proc","start_date")
        .withColumn("alpha_005_days_proc", $"alpha_005_days"/$"dur_in_days").drop("alpha_005_date").withColumnRenamed("alpha_005_days_proc","alpha_005_date")
        .withColumn("alpha_01_days_proc", $"alpha_01_days"/$"dur_in_days").drop("alpha_01_date").withColumnRenamed("alpha_01_days_proc","alpha_01_date")
        .withColumn("alpha_015_days_proc", $"alpha_015_days"/$"dur_in_days").drop("alpha_015_date").withColumnRenamed("alpha_015_days_proc","alpha_015_date")
        .withColumn("balon_days_proc", $"balon_days"/$"dur_in_days").drop("balon_date").withColumnRenamed("balon_days_proc","balon_date")

      agg12.drop("date_balon_starts")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_t_cred_pcOUT")

      logInserted()
      logEnd()
    }
}

