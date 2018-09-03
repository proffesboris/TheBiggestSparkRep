package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EIOFilterLkLkc(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob{

  val Node_CluBase_IN = s"${config.aux}.clu_base"
  val Node_LkLkc_AUX_IN = s"${config.aux}.lk_lkc"
  val Node_EksEiogen_AUX_IN = s"${config.aux}.k7m_eks_eiogen"
  val Node_Lk_AUX_IN = s"${config.aux}.lk"
  val Node_Lkc_AUX_IN = s"${config.aux}.lkc"
  val Node_EIOSelected_INOUT = s"${config.aux}.EIO_for_LKC_selected"

  val Node_Lkc_PA_OUT = s"${config.pa}.lkc"
  val Node_Lk_PA_OUT= s"${config.pa}.lk"


  var dashboardName: String = Node_Lk_PA_OUT //витрина
  override def processName: String = "clu_base"

  def DoEIOSelected() {
    Logger.getLogger(Node_EIOSelected_INOUT).setLevel(Level.WARN)
    logStart()
    dashboardName = Node_EIOSelected_INOUT //витрина
    val createHiveTableEIOSelected = spark.sql(
      s"""
         select
             lkc_id,
             u7m_id,
             crit_id
         from (
             select
                 u7m_id,
                 lkc_id,
                 crit_id,
                 row_number() over (partition by u7m_id order by priority,lkc_id) rn
             from (
                 select
                     b.u7m_id,
                     1 priority,
                     lkc.crit_id,
                     lkc.lkc_id
                 from
                     $Node_CluBase_IN b
                     join $Node_Lk_AUX_IN l
                         on b.u7m_id = l.u7m_id_to
                     join $Node_Lkc_AUX_IN lkc
                         on l.lk_id = lkc.lk_id
                     join
                     $Node_LkLkc_AUX_IN lc
                     on lc.lkc_id = lkc.lkc_id
                     join $Node_EksEiogen_AUX_IN ei
                     on concat('EK_',cast(ei.loc_id as string))  = lc.link_id
                 where lkc.crit_id = 'EIOGEN_EKS'
                     and lc.link_id like 'EK_%'
                     and b.eks_id = ei.id
                 union all
                 select
                     b.u7m_id,
                     2 priority,
                     lkc.crit_id,
                     lkc.lkc_id
                 from
                     $Node_CluBase_IN b
                     join $Node_Lk_AUX_IN  l
                         on b.u7m_id = l.u7m_id_to
                     join $Node_Lkc_AUX_IN lkc
                         on l.lk_id = lkc.lk_id
                 where lkc.crit_id = 'EIOGen'
                  and b.flag_basis_client = 'N'
             ) x
         ) y
         where rn = 1
       """.stripMargin
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}EIO_for_LKC_selected")
      .saveAsTable(Node_EIOSelected_INOUT)
    logInserted()

    //ОТФИЛЬТРОВАННЫЙ PA.LKC
     dashboardName  = Node_Lkc_PA_OUT //витрина PA.LKC
     val createHiveTableLKC = spark.sql(
      s"""
         select
            lkc.*
         from $Node_Lkc_AUX_IN lkc
           left join
           $Node_EIOSelected_INOUT f
          on lkc.lkc_id = f.lkc_id
          where
                 lkc.crit_id not in ('EIOGEN_EKS','EIOGen')
                 OR f.lkc_id is not null
       """.stripMargin
      ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.paPath}lkc")
        .saveAsTable(Node_Lkc_PA_OUT)
        logInserted()

        dashboardName  = Node_Lk_PA_OUT //витрина PA.LK
        val createHiveTableLK = spark.sql(
        s"""
         select
            lk.*
         from $Node_Lk_AUX_IN lk
           join
           (select distinct lk_id from $Node_Lkc_PA_OUT ) k
          on lk.lk_id = k.lk_id
       """.stripMargin
      ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.paPath}lk")
        .saveAsTable(Node_Lk_PA_OUT)
        logInserted()
    logEnd()
  }

}