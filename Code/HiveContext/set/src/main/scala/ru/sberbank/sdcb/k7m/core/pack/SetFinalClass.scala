package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


class SetFinalClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val tbl_set_blocked = "set_blocked"
  val tbl_set_blocked_OUT = s"${config.aux}.$tbl_set_blocked"
  val tbl_set_blocked_Path = s"${config.auxPath}${tbl_set_blocked}"

  val tbl_set_active = "set_active"
  val tbl_set_active_OUT = s"${config.aux}.$tbl_set_active"
  val tbl_set_active_Path = s"${config.auxPath}$tbl_set_active"

  val tbl_set = "set"
  val tbl_set_OUT = s"${config.pa}.$tbl_set"
  val tbl_set_Path = s"${config.paPath}$tbl_set"

  val tbl_set_item = "set_item"
  val tbl_set_item_OUT = s"${config.pa}.$tbl_set_item"
  val tbl_set_item_Path = s"${config.paPath}$tbl_set_item"

  var dashboardName: String = "set_stub"
  override val processName: String  = "SET"

  def run() {
    Logger.getLogger(tbl_set_blocked_OUT).setLevel(Level.WARN)

    logStart()
    dashboardName = tbl_set_blocked_OUT
    //добавим поручителей базису
    val readHiveTable1 = spark.sql(s"""
        select
          si.set_id,
          si.obj,
          si.u7m_id,
          sic.check_name
        from   ${config.aux}.set_item si
          join ${config.pa}.set_item_check  sic
          on si.u7m_id = sic.u7m_id
        where sic.check_name  not like '%GEN'
          and sic.check_flg>0
        union all
        select
          si.set_id,
          si.obj,
          si.u7m_id,
          'NO_CRM_ID' as check_name
        from ${config.aux}.set_item si
          join ${config.aux}.clu_base b
          on b.u7m_id = si.u7m_id
        where
          si.obj = 'CLU'
          and b.crm_id is null
        union all
        select
          si.set_id,
          si.obj,
          si.u7m_id,
          'NO_CRM_ID' as check_name
        from ${config.aux}.set_item si
          join ${config.pa}.clF b
        on b.f7m_id = si.u7m_id
          where si.obj = 'CLF'
          and b.crm_id is null
        union all
        select
          si.set_id,
          si.obj,
          si.u7m_id,
          'NO_EIO' as check_name
        from ${config.aux}.set_item si
          join ${config.aux}.clu_base b
        on b.u7m_id = si.u7m_id
          left join
          (
          select lk.u7m_id_to
          from ${config.pa}.lk
          join ${config.pa}.lkc
            on lk.lk_id = lkc.lk_id
          where lkc.crit_id like 'EIO%'
          ) eio
          on eio.u7m_id_to = si.u7m_id
        where si.obj = 'CLU'
        and eio.u7m_id_to is null
      """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", tbl_set_blocked_Path)
      .saveAsTable(s"$tbl_set_blocked_OUT")
    logInserted()

    dashboardName = tbl_set_active_OUT
    //добавим поручителей базису
    val readHiveTable2 = spark.sql(s"""
        select s.set_id
        from ${config.aux}.set s
          left join ${config.aux}.set_blocked b
        on s.set_id = b.set_id
        where b.set_id is null
                                            """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", tbl_set_active_Path)
      .saveAsTable(s"$tbl_set_active_OUT")
    logInserted()

    val ds_set_active = spark.table(tbl_set_active_OUT)

    dashboardName = tbl_set_OUT
    //добавим поручителей базису
    val ds_set = spark.table(s"${config.aux}.set")

    val readHiveTable3 = ds_set.join( broadcast(ds_set_active),ds_set_active("set_id") === ds_set("set_id") )
      .select(ds_set("*"))
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$tbl_set_Path" )
      .saveAsTable(tbl_set_OUT)
    logInserted()

    dashboardName = tbl_set_item_OUT
    //добавим поручителей базису
    val ds_set_item = spark.table(s"${config.aux}.set_item")

    val readHiveTable4 = ds_set_item.join( broadcast(ds_set_active),ds_set_active("set_id") === ds_set_item("set_id") )
      .select(ds_set_item("*"))
      .withColumnRenamed("u7m_id","7m_id")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$tbl_set_item_Path" )
      .saveAsTable(tbl_set_item_OUT)
    logInserted()

    logEnd()
  }
}