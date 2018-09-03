package ru.sberbank.sdcb.k7m.core.pack

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.Utils._
import ru.sberbank.sdcb.k7m.java.{Utils => JavaUtils}

class VDCRMOut(override val spark: SparkSession, val config: Config) extends EtlJob with EtlIntegrLogger with EtlLogger {

  override val processName = "VDCRMOut"
  var dashboardName: String = "VDCRMOut"

  val now = Calendar.getInstance().getTime
  //---------------------------------------------------------
  val date = new SimpleDateFormat("yyyyMMdd").format(now)
  val timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(now)
  val bDate = LoggerUtils.obtainRunDtWithoutTime(spark, config.aux)

  override val logSchemaName: String = config.aux
  val targPath = s"${config.paPath}/export/vdcrmout"
  def fileName = s"DMK7M_CRMARC_${date}_${timestamp}_0001_0001.000001.csv"

  val Stg0Schema = config.stg
  val CRM_Green_List_OUT_IN = s"${Stg0Schema}.crm_green_list_out"
  //---------------------------------------------------------

  def csvOptions = Map(
    "sep" -> ";",
    "header" -> "false",
    "escapeQuotes" -> "false",
    "quote" -> "",
    "dateFormat" -> "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "encoding" -> "windows-1251")
  
  def run(client_list: Int) {

    logIntegrStart()

    dashboardName = s"${config.pa}.VDCRMOut"

    val ListString: String = if (client_list > 0) {s"        left semi join $CRM_Green_List_OUT_IN l on l.org_crm_id = clu.crm_id"} else {""} //добавлен фильтр для выгрузки в CRM по short_list от Ивановой Е.Г.

    spark.sql(s"drop table if exists ${config.pa}.VDCRMOut")

    spark.sql(s"""create table ${config.pa}.VDCRMOut
                 |stored as parquet
                 |as
                 |select  crm_id,
                 |        OfferExpDate,
                 |        eks_id,
                 |        org_id,
                 |        sclir,
                 |        inn,
                 |        kpp,
                 |        f_name_km,
                 |        s_name_km,
                 |        l_name_km,
                 |        m_phone_phonenumber,
                 |        email,
                 |        km_tb,
                 |        func_division,
                 |        ter_division,
                 |        f_name_eio,
                 |        s_name_eio,
                 |        l_name_eio,
                 |        Position,
                 |        flag_ben,
                 |        acc_no,
                 |        acc_bik,
                 |        ACC_DATE_OP,
                 |        acc_guid
                 |  from (
                 |     select
                 |        clu.crm_id ,
                 |        Cast(date_sub(Current_Date,-8) AS TIMESTAMP) AS OfferExpDate,
                 |        clu.eks_id,
                 |        clu.org_id,
                 |        clu.sclir,
                 |        clu.inn,
                 |        clu.kpp,
                 |        vko.fst_name as f_name_km,
                 |        vko.last_name as s_name_km,
                 |        vko.mid_name as l_name_km,
                 |        coalesce(vko.cell_ph_num, '88005555777') as m_phone_phonenumber,
                 |        vko.alt_email_addr as email,
                 |        vko.terbank as km_tb,
                 |        case when SUBSTR(a.tb_code,1,2)='00' then  '38' else SUBSTR(a.tb_code,1,2) end as acc_TB,
                 |        case when tb.tb_code='00' then  '38' else tb.tb_code end as dict_tb,
                 |        vko.func_podr as func_division,
                 |        vko.ter_podr as ter_division,
                 |        clf_eio.f_name AS f_name_eio,
                 |        clf_eio.s_name AS s_name_eio,
                 |        clf_eio.l_name AS l_name_eio,
                 |        clf_eio.Position_eio as Position,
                 |        CASE WHEN clu.borrower_flag = True THEN 1 ELSE 0 end AS flag_ben, --1 TRUE «кредитующийся клиент» из BURROWER_FLAG = 1 True для BEN_FLAG «Не Запрашивать»   --О False «новый Заемщик» из BURROWER_FLAG = О FALSE для BEN_FLAG «Запрашивать»
                 |        a.acc_no,
                 |        a.acc_bik,
                 |        a.ACC_DATE_OP,
                 |        a.acc_guid
                 |    FROM
                 |        ${config.pa}.clu clu
                 |        $ListString
                 |        INNER JOIN ${config.aux}.vdokrout_score sc on sc.u7m_id = clu.u7m_id
                 |        INNER JOIN ${config.pa}.acc a ON a.cl_eks_id = clu.eks_id
                 |        INNER JOIN ${config.aux}.k7m_vko vko ON vko.org_crm_id = clu.crm_id
                 |        INNER JOIN ${config.aux}.dict_tb_mapping tb on tb.crm_tb_name = vko.terbank
                 |        LEFT JOIN ${config.pa}.clu_check b ON clu.u7m_id = b.u7m_id AND b.check_name = 'GEN_TOTAL_CHECK'
                 |        LEFT JOIN (
                 |            SELECT
                 |                    lk_eio.u7m_id_to,
                 |                    clf.f7m_id,
                 |                    clf.f_name,
                 |                    clf.s_name,
                 |                    clf.l_name,
                 |                    e.Position_eio
                 |              FROM
                 |                    ${config.pa}.lk lk_eio
                 |                    INNER JOIN ${config.pa}.lkc lkc_eio ON lk_eio.lk_id = lkc_eio.lk_id AND lkc_eio.crit_id in ('EIOGEN_EKS', 'EIOGen')
                 |                    INNER JOIN ${config.pa}.clf clf ON lk_eio.u7m_id_from = clf.f7m_id
                                      INNER JOIN  ${config.aux}.clf_position_eio e
                                       on e.f7m_id = clf.f7m_id
                 |             WHERE
                 |                    clf.f_name is not null and
                 |                    clf.s_name is not null and
                 |                    e.Position_eio is not null
                 |        ) clf_eio ON clu.u7m_id = clf_eio.u7m_id_to
                 |  WHERE
                 |    (Substr(a.acc_no,1,5) like '40502%'
                 |           or Substr(a.acc_no,1,5) like '40602%'
                 |           or Substr(a.acc_no,1,5) like '40702%')
                 |        and a.acc_currency = 'RUR'               -- требование от 14.08.2018 по исключению валютных счетов Шилова Е.
                 |        --and substr(a.tb_code,1,2) = tb.tb_code   -- требование от 14.08.2018 по счетам открытым в ТБ = ТБ ВКО (КМ) (Шилова Е.)
                 |        and COALESCE(b.check_flg, 0) = 0
                 |        and cast('$bDate'as timestamp) between a.acc_date_op and coalesce(a.acc_date_close,cast('9999-12-31'as timestamp))-- заменить первый литерал на параметр - Бизнес-Дата расчета
                 |    and a.acc_guid is not null
                 |    --and vko.cell_ph_num is not null
                 |    --and clf_eio.f_name is not null
                 |    --and clf_eio.s_name is not null
                 |    --and clf_eio.l_name is not null
                 |    ) a
                 |where acc_TB = dict_tb                --требование от 17.08.2018  ТБ = ТБ ВКО (КМ) (замена 00 = 38) (Шилова Е.)
            """.stripMargin)

    logIntegrInserted()
    logInserted()

    val checkStageCnt = spark.sql(s"""
                                          select cast(count(*) as string) cnt from (
                                                select count(acc_no) as cnt from ${config.pa}.VDCRMOut
                                                group by acc_no
                                            ) t
                                          where cnt >1""").first().getString(0)


    if (checkStageCnt != "0"){
      val ErrorMSG = s"Duplicate string in ${config.pa}.VDCRMOut: "+checkStageCnt
      log(processName,ErrorMSG, CustomLogStatus.ERROR)
      throw new IllegalArgumentException(ErrorMSG)
    }

    val df = spark.table(s"${config.pa}.VDCRMOut")

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    dashboardName = fileName

    val count = df.count()

    df
      .formatDates(csvOptions("dateFormat"))
      .concat(csvOptions("sep"))
      .withColumn("concat", concat(col("concat"), lit("\r")))
      .write
      .mode(SaveMode.Overwrite)
      .text(s"$targPath/0001")
      //.options(csvOptions)
      //.csv(s"$targPath/0001")

    JavaUtils.copyMerge(
      fs, new Path(s"$targPath/0001"),
      fs, new Path(s"$targPath/$dashboardName"),
      true, conf,
      null, null)

    JavaUtils.encodeFile(fs, new Path(s"$targPath/$dashboardName"), csvOptions("encoding"))

    logIntegrSaved(count)

    logIntegrEnd()
  }

}
