package ru.sberbank.sdcb.k7m.core.pack

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.java.{Utils => JavaUtils}
import ru.sberbank.sdcb.k7m.core.pack.Utils._

class VDLawRobotOut(override val spark: SparkSession, val config: Config) extends EtlJob with EtlIntegrLogger with EtlLogger {

  override val processName = "VDLawRobotOut"
  var dashboardName: String = "VDLawRobotOut"

  val now = Calendar.getInstance().getTime
  //---------------------------------------------------------
  val timestamp = new SimpleDateFormat("yyMMddHHmmss").format(now)

  override val logSchemaName: String = config.aux
  val targPath = s"${config.paPath}/export/vdrlout"
  def fileName(num: String) = s"K7MRUARC_${num}_$timestamp.csv"
  val CRM_Green_List_OUT_IN = s"${config.stg}.crm_green_list_out"
  val csvOptions = Map(
    "sep" -> ";",
    "header" -> "false",
    "escapeQuotes" -> "false",
    "dateFormat" -> "dd.MM.yyyy",
    "timestampFormat" -> "dd.MM.yyyy")

  //---------------------------------------------------------

  val conf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(conf)

  def run(client_list: Int) {

    logIntegrStart()

    dashboardName = s"${config.aux}.VDRLOUT_CLU_101"

    //добавлен фильтр для выгрузки в RL по short_list от Ивановой Е.Г.
    val ListString: String = if (client_list > 0) {s"join $CRM_Green_List_OUT_IN src on src.org_crm_id = clu.crm_id"} else {""}

    spark.sql(s"drop table if exists ${config.aux}.VDRLOUT_CLU_101")

    spark.sql(s"""create table ${config.aux}.VDRLOUT_CLU_101 stored as parquet
                  as
                  select
                          clu_filtered.U7M_ID
                          ,clu_filtered.CRM_ID
                          ,clu_filtered.INN
                          ,clu_filtered.OGRN
                          ,clu_filtered.KPP
                          ,clu_filtered.SHORT_NAME -- Добавлено поле в версии 27
                          ,clu_filtered.RU_ID
                          ,1 as ROLE
                    from
                          (
                              select
                                      clu.U7M_ID
                                      ,clu.CRM_ID
                                      ,clu.INN
                                      ,clu.OGRN
                                      ,clu.KPP
                                      ,clu.SHORT_NAME -- Добавлено поле в версии 27
                                      ,clu.RU_ID
                                      ,check.check_flg
                                from
                                      ${config.pa}.clu
                                      $ListString
                                      left join (
                                      --заёмщика может отсечь любая проверка в CLU или SCORE
                                      --поэтому берём максимальный флаг из обеих проверок
                                          select
                                                  c.u7m_id,
                                                  max(c.check_flg) as check_flg
                                            from
                                                  (
                                                      select
                                                              u7m_id, check_flg
                                                        from
                                                              ${config.pa}.clu_check
                                                       where
                                                              check_name = 'GEN_OFFLINE_CHECK'
                                                      union all
                                                      select
                                                              u7m_id, check_flg
                                                        from
                                                              ${config.pa}.score_check
                                                       where
                                                              check_name = 'GEN_OFFLINE_CHECK'
                                                  ) c
                                           group by
                                                  c.u7m_id
                                      ) check on (check.u7m_id = clu.u7m_id)
                               where
                                      clu.flag_basis_client = 'Y'
                          ) clu_filtered
                   where
                          coalesce(clu_filtered.check_flg,0) <> 1
                      and clu_filtered.ru_id is not null""".stripMargin)

    logIntegrInserted()
    logInserted()

    dashboardName = s"${config.aux}.VDRLOUT_LKC_102"

    spark.sql(s"drop table if exists ${config.aux}.VDRLOUT_LKC_102")

    spark.sql(s"""create table ${config.aux}.VDRLOUT_LKC_102 stored as parquet as
                select
                        VDRLOUT_CLU_101.CRM_ID as CRM_ID_FROM,
                        CLU.CRM_ID as CRM_ID_TO,
                        LKC.CRIT_ID,
                        LKC.LINK_PROB
                  from
                        ${config.pa}.LK
                        join ${config.pa}.LKC on LK.LK_ID = LKC.LK_ID
                        join ${config.aux}.VDRLOUT_CLU_101 on VDRLOUT_CLU_101.u7m_id = lk.u7m_id_from
                        -- Этот блок не выгружается
                        -- он нужен для того, чтобы не выгружать связи (LKC), у которых поручитель (u7m_id_to) не прошёл проверку.
                        -- В отличие от заёмщиков, поручителей фильтруем только по CLU_CHECK
                        join (
                            select
                                    clu.u7m_id,
                                    clu.crm_id
                              from
                                    ${config.pa}.clu clu
                                    left join (
                                        select
                                                clu_check.u7m_id,
                                                clu_check.check_flg
                                          from
                                                ${config.pa}.clu_check
                                         where
                                                clu_check.check_name = 'GEN_OFFLINE_CHECK'
                                    ) clu_check on clu_check.u7m_id = clu.u7m_id
                             where
                                    1=1 and
                                    coalesce(clu_check.check_flg,0) = 0 --добавлено условие
                                and clu.crm_id is not null
                                and clu.ru_id is not null
                        ) clu on LK.u7m_id_to = clu.u7m_id
                        -- Выгружаем всех заёмщиков, попавших в SET, вне зависимости от номера критерия LKC
                        join ${config.pa}.set on VDRLOUT_CLU_101.u7m_id = set.u7m_b_id
                        join ${config.pa}.set_item on set.set_id = set_item.set_id and set_item.obj = 'CLU'
                 where
                        CLU.CRM_ID = set_item.7m_id""".stripMargin)

    logIntegrInserted()
    logInserted()

    dashboardName = s"${config.aux}.VDRLOUT_CLU_103"

    spark.sql(s"drop table if exists ${config.aux}.VDRLOUT_CLU_103")

    spark.sql(s"""create table ${config.aux}.VDRLOUT_CLU_103 stored as parquet
                as
                select --дедубликацию тут не надо
                         clu_filtered.U7M_ID
                        ,clu_filtered.CRM_ID
                        ,clu_filtered.INN
                        ,clu_filtered.OGRN
                        ,clu_filtered.KPP
                        ,clu_filtered.SHORT_NAME -- Добавлено поле в версии 27
                        ,clu_filtered.RU_ID
                        ,2 as ROLE
                  from
                        (
                            select
                                    clu.U7M_ID
                                    ,clu.CRM_ID
                                    ,clu.INN
                                    ,clu.OGRN
                                    ,clu.KPP
                                    ,clu.SHORT_NAME -- Добавлено поле в версии 27
                                    ,clu.RU_ID
                                    ,clu_check.check_flg
                              from
                                    ${config.aux}.VDRLOUT_LKC_102
                                    join ${config.pa}.clu on VDRLOUT_LKC_102.crm_id_to = clu.crm_id
                                    left join (
                                        select
                                                u7m_id,
                                                check_flg
                                          from
                                                ${config.pa}.clu_check
                                         where
                                                clu_check.check_name = 'GEN_OFFLINE_CHECK'
                                    ) clu_check on clu_check.u7m_id = clu.u7m_id
                             where
                                    1=1
                                and coalesce(clu_check.check_flg,0) = 0 --добавлено условие
                                and clu.crm_id is not null --добавлено условие
                        ) clu_filtered
                 where
                        1=1
                    and coalesce(clu_filtered.check_flg,0) <> 1""".stripMargin)

    logIntegrInserted()
    logInserted()

    dashboardName = s"${config.aux}.VDRLOUT_CLU_104"

    spark.sql(s"drop table if exists ${config.aux}.VDRLOUT_CLU_104")

    spark.sql(s"""create table ${config.aux}.VDRLOUT_CLU_104 stored as parquet as
                select
                        clu.CRM_ID
                        ,clu.INN
                        ,clu.OGRN
                        ,clu.KPP
                        ,clu.SHORT_NAME -- Добавлено поле в версии 27
                        ,clu.RU_ID
                        ,case when sum(clu.role) >= 3 then 3 else sum(clu.role) end as role
                  from
                        (
                            select distinct
                                    CRM_ID
                                    ,INN
                                    ,OGRN
                                    ,KPP
                                    ,SHORT_NAME
                                    ,RU_ID
                                    ,ROLE
                              from
                                    ${config.aux}.VDRLOUT_CLU_101
                            union all
                            select distinct
                                    CRM_ID
                                    ,INN
                                    ,OGRN
                                    ,KPP
                                    ,SHORT_NAME
                                    ,RU_ID
                                    ,ROLE
                              from
                                    ${config.aux}.VDRLOUT_CLU_103
                        ) CLU
                 group by
                        clu.CRM_ID
                        ,clu.INN
                        ,clu.OGRN
                        ,clu.KPP
                        ,clu.SHORT_NAME
                        ,clu.RU_ID""".stripMargin)

    logIntegrInserted()
    logInserted()

    val egrul = spark.sql(s"""select
                                 clu.crm_id
                                 ,fns_egrul.svnaimul as SvNaimUL
                                 ,fns_egrul.svadresul as SvAdresUL
                                 ,fns_egrul.svadrelpochty as SvAdrElPochty
                                 ,fns_egrul.svobrul as SvObrUL
                                 ,fns_egrul.svregorg as SvRegOrg
                                 ,fns_egrul.svstatus as SvStatus
                                 ,fns_egrul.svprekrul as SvPrekrUL
                                 ,fns_egrul.svuchetno as SvUchetNO
                                 ,fns_egrul.svregpf as SvRegPF
                                 ,fns_egrul.svregfss as SvRegFSS
                                 ,fns_egrul.svustkap as SvUstKap
                                 ,fns_egrul.svtipustav as SvTipUstav
                                 ,fns_egrul.svuprorg as SvUprOrg
                                 ,fns_egrul.sveddolzhnfl as SvedDolzhnFL
                                 ,fns_egrul.svuchredit as SvUchredit
                                 ,fns_egrul.svdolyaooo as SvDolyaOOO
                                 ,fns_egrul.svderzhreestrao as SvDerzhReestrAO
                                 ,fns_egrul.svokved as SvOKVED
                                 ,fns_egrul.svlicenziya as SvLicenziya
                                 ,fns_egrul.svpodrazd as SvPodrazd
                                 ,fns_egrul.svreorg as SvReorg
                                 ,fns_egrul.svpredsh as SvPredsh
                                 ,fns_egrul.svkfhpredsh as SvKFHPredsh
                                 ,fns_egrul.svpreem as SvPreem
                                 ,fns_egrul.svkfhpreem as SvKFHPreem
                                 ,fns_egrul.svzapegrul as SvZapEGRUL
                                 ,fns_egrul.datavyp as DataVyp
                                 ,fns_egrul.ogrn as OGRN
                                 ,fns_egrul.dataogrn as DataOGRN
                                 ,fns_egrul.inn as INN
                                 ,fns_egrul.kpp as KPP
                                 ,fns_egrul.spropf as SprOPF
                                 ,fns_egrul.kodopf as KodOPF
                                 ,fns_egrul.polnnaimopf as PolnNaimOPF
                            from
                                  ${config.stg}.fns_egrul fns_egrul
                                  join ${config.aux}.VDRLOUT_CLU_104 clu on clu.inn = fns_egrul.inn""".stripMargin)

    val EGRUL = ((df: DataFrame) => {
      df
        .withColumn("egrul", to_json(struct(
          df
            .drop("crm_id")
            .columns
            .map(col): _*
        )))
        .select("crm_id", "egrul")
    }) (egrul)

    val dataFrames = Seq(
      (spark.table(s"${config.aux}.VDRLOUT_CLU_104"), "csv"),
      (spark.table(s"${config.aux}.VDRLOUT_LKC_102"), "csv"),
      (EGRUL.concat(csvOptions("sep")), "text"))

    dataFrames.zip(Stream from 1).map { case ((df, format), i) =>

      val num = s"000$i"
      dashboardName = fileName(num)

      val count = df.count()

      df
        .write
        .mode(SaveMode.Overwrite)
        .options(csvOptions)
        .format(format)
        .save(s"$targPath/$num")

      JavaUtils.copyMerge(
        fs, new Path(s"$targPath/$num"),
        fs, new Path(s"$targPath/$dashboardName"),
        true, conf,
        null, null)

      logIntegrSaved(count)
    }

    logIntegrEnd()

  }
}