package ru.sberbank.sdcb.k7m.core.pack

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.Utils._
import ru.sberbank.sdcb.k7m.java.{Utils => JavaUtils}

class Vdmmz(override val spark: SparkSession, val config: Config) extends EtlJob with EtlIntegrLogger with EtlLogger  {

  override val processName = "VDMMZOut"
  var dashboardName: String = "VDMMZOut"

  val now = Calendar.getInstance().getTime
  //---------------------------------------------------------
  val date = new SimpleDateFormat("yyyyMMdd").format(now)
  val timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(now)

  override val logSchemaName: String = config.aux
  val targPath = s"${config.paPath}/export/vdmmzout"
  def fileName(num: String) = s"DMK7M_MMZARC_${date}_${timestamp}_0001_$num.csv"
  val packName = s"Packet_K7M_MMZ_IN_FROM_PIM_${new SimpleDateFormat("yyyyMMddHHmm").format(now)}.zip"

  //---------------------------------------------------------

  def csvOptions = Map(
    "sep" -> ";",
    "header" -> "false",
    "escapeQuotes" -> "false",
    "quote" -> "",
    "dateFormat" -> "dd.MM.yyyy",
    "timestampFormat" -> "dd.MM.yyyy")

  def run() {

    logIntegrStart()

    val mmz = "mmz_0001"
    val rel = "dict_mmz_pty_role"

    val mmzSelect = s"""with lk_clu as (
                       | select 
                       |   a3.u7m_id,
                       |   1 as role_id,
                       |   case when length(a3.inn) = 12 then 2 else 1 end as client_type,
                       |   a3.inn,
                       |   a3.ogrn,
                       |   NULL as first_name,
                       |   NULL as middle_name,
                       |   NULL as last_name,
                       |   NULL as birthday,
                       |   NULL as id_type,
                       |   NULL as id_series,
                       |   NULL as id_num,
                       |   NULL as issued_code,
                       |   NULL as id_date,
                       |   a3.crm_id,
                       |   NULL as mob_tel
                       |   from
                       |   ${config.pa}.clu a1
                       |   join ${config.pa}.lk a2 on (a1.u7m_id = a2.u7m_id_from and a2.t_to = 'CLU')
                       |   join ${config.pa}.clu a3 on (a2.u7m_id_to = a3.u7m_id) 
                       |  where
                       |   a1.flag_basis_client = 'Y' and 
                       |   exists (
                       |    select
                       |      1
                       |      from
                       |      ${config.pa}.lkc a4
                       |      join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |     where
                       |      a4.lk_id  = a2.lk_id and
                       |      a5.cbr_flag = 1
                       |   )
                       | union all
                       | select 
                       |   a3.u7m_id,
                       |   1 as role_id,
                       |   case when length(a3.inn) = 12 then 2 else 1 end as client_type,
                       |   a3.inn,
                       |   a3.ogrn,
                       |   NULL as first_name,
                       |   NULL as middle_name,
                       |   NULL as last_name,
                       |   NULL as birthday,
                       |   NULL as id_type,
                       |   NULL as id_series,
                       |   NULL as id_num,
                       |   NULL as issued_code,
                       |   NULL as id_date,
                       |   a3.crm_id,
                       |   NULL as mob_tel
                       |   from
                       |   ${config.pa}.clu a1
                       |   join ${config.pa}.lk a2 on (a1.u7m_id = a2.u7m_id_to and a2.t_from = 'CLU')
                       |   join ${config.pa}.clu a3 on (a2.u7m_id_from = a3.u7m_id)
                       |  where
                       |   a1.flag_basis_client = 'Y' and 
                       |   exists (
                       |    select
                       |      1
                       |      from
                       |      ${config.pa}.lkc a4
                       |      join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |     where
                       |      a4.lk_id  = a2.lk_id and
                       |      a5.cbr_flag = 1
                       |   )
                       |)
                       |select 
                       |  client_type,
                       |  concat_ws(';', collect_set( cast( role_id as string))) as role_id,
                       |  u7m_id as c7m_id,
                       |  inn as tax_id,
                       |  ogrn as psrn,
                       |  first_name,
                       |  middle_name,
                       |  last_name,
                       |  birthday,
                       |  id_type,
                       |  id_series,
                       |  id_num,
                       |  issued_code,
                       |  id_date,
                       |  crm_id,
                       |  mob_tel
                       |  from
                       |  ( select distinct * from (
                       |   select 
                       |     a1.u7m_id,
                       |     0 as role_id,
                       |     1 as client_type,
                       |     a1.inn,
                       |     a1.ogrn,
                       |     NULL as first_name,
                       |     NULL as middle_name,
                       |     NULL as last_name,
                       |     NULL as birthday,
                       |     NULL as id_type,
                       |     NULL as id_series,
                       |     NULL as id_num,
                       |     NULL as issued_code,
                       |     NULL as id_date,
                       |     a1.crm_id,
                       |     NULL as mob_tel
                       |     from
                       |     ${config.pa}.clu a1
                       |    where
                       |     a1.flag_basis_client = 'Y'
                       |   union all
                       |   select  
                       |     a1.7m_id as u7m_id,
                       |     2 as role_id,
                       |     case when length(a2.inn) = 12 then 2 else 1 end as client_type,
                       |     a2.inn,
                       |     a2.ogrn,
                       |     NULL as first_name,
                       |     NULL as middle_name,
                       |     NULL as last_name,
                       |     NULL as birthday,
                       |     NULL as id_type,
                       |     NULL as id_series,
                       |     NULL as id_num,
                       |     NULL as issued_code,
                       |     NULL as id_date,
                       |     a2.crm_id,
                       |     NULL as mob_tel
                       |     from
                       |     ${config.pa}.set_item a1
                       |     join ${config.pa}.clu a2 on (a1.7m_id = a2.u7m_id)
                       |    where
                       |     a1.obj = 'CLU'
                       |   union all
                       |   select 
                       |     a1.7m_id as u7m_id,
                       |     2 as role_id,
                       |     0 as client_type,
                       |     a2.inn,
                       |     NULL as ogrn,
                       |     a2.f_name as first_name,
                       |     a2.l_name as middle_name,
                       |     a2.s_name as last_name,
                       |     a2.b_date as birthday,
                       |     a2.id_type,
                       |     a2.id_series,
                       |     a2.id_num,
                       |     a2.id_source as issued_code,
                       |     a2.id_date,
                       |     a2.crm_id as crm_id,
                       |     a2.m_phone_phonenumber as mob_tel
                       |     from
                       |     ${config.pa}.set_item a1
                       |     join ${config.pa}.clf a2 on (a1.7m_id = a2.f7m_id)
                       |    where
                       |     a1.obj = 'CLF'  
                       |   union all
                       |   select
                       |     *
                       |     from
                       |     lk_clu
                       |   union all
                       |   select 
                       |     a1.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a1.inn,
                       |     NULL as ogrn,
                       |     a1.f_name as first_name,
                       |     a1.l_name as middle_name,
                       |     a1.s_name as last_name,
                       |     a1.b_date as birthday,
                       |     a1.id_type,
                       |     a1.id_series,
                       |     a1.id_num,
                       |     a1.id_source as issued_code,
                       |     a1.id_date,
                       |     a1.crm_id as crm_id,
                       |     a1.m_phone_phonenumber as mob_tel
                       |     from
                       |     ${config.pa}.clf a1
                       |     join ${config.pa}.lk a2 on (a1.f7m_id = a2.u7m_id_to and a2.t_to = 'CLF')
                       |     join ${config.pa}.clu a3 on (a2.u7m_id_from = a3.u7m_id)
                       |    where
                       |           a3.flag_basis_client = 'Y' and 
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )
                       |   union all
                       |   select 
                       |     a1.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a1.inn,
                       |     NULL as ogrn,
                       |     a1.f_name as first_name,
                       |     a1.l_name as middle_name,
                       |     a1.s_name as last_name,
                       |     a1.b_date as birthday,
                       |     a1.id_type,
                       |     a1.id_series,
                       |     a1.id_num,
                       |     a1.id_source as issued_code,
                       |     a1.id_date,
                       |     a1.crm_id as crm_id,
                       |     a1.m_phone_phonenumber as mob_tel
                       |     from
                       |     ${config.pa}.clf a1
                       |     join ${config.pa}.lk a2 on (a1.f7m_id = a2.u7m_id_from and a2.t_from = 'CLF')
                       |     join ${config.pa}.clu a3 on (a2.u7m_id_to = a3.u7m_id)
                       |    where
                       |           a3.flag_basis_client = 'Y' and 
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )  
                       |   union all
                       |   select
                       |     a1.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a1.inn,
                       |     NULL as ogrn,
                       |     a1.f_name as first_name,
                       |     a1.l_name as middle_name,
                       |     a1.s_name as last_name,
                       |     a1.b_date as birthday,
                       |     a1.id_type,
                       |     a1.id_series,
                       |     a1.id_num,
                       |     a1.id_source as issued_code,
                       |     a1.id_date,
                       |     a1.crm_id as crm_id,
                       |     a1.m_phone_phonenumber as mob_tel
                       |     from
                       |     ${config.pa}.clf a1
                       |     join ${config.pa}.lk a2 on (a1.f7m_id = a2.u7m_id_to and a2.t_to = 'CLF')
                       |     join ${config.pa}.set_item a3 on (a2.u7m_id_from = a3.7m_id and a3.obj = 'CLU')
                       |    where
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )
                       |   union all
                       |   select 
                       |     a1.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a1.inn,
                       |     NULL as ogrn,
                       |     a1.f_name as first_name,
                       |     a1.l_name as middle_name,
                       |     a1.s_name as last_name,
                       |     a1.b_date as birthday,
                       |     a1.id_type,
                       |     a1.id_series,
                       |     a1.id_num,
                       |     a1.id_source as issued_code,
                       |     a1.id_date,
                       |     a1.crm_id as crm_id,
                       |     a1.m_phone_phonenumber as mob_tel
                       |     from
                       |     ${config.pa}.clf a1
                       |     join ${config.pa}.lk a2 on (a1.f7m_id = a2.u7m_id_from and a2.t_from = 'CLF')
                       |     join ${config.pa}.set_item a3 on (a2.u7m_id_to = a3.7m_id and a3.obj = 'CLU')
                       |    where
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )
                       |   union all
                       |   select 
                       |     a3.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a3.inn,
                       |     NULL as ogrn,
                       |     a3.f_name as first_name,
                       |     a3.l_name as middle_name,
                       |     a3.s_name as last_name,
                       |     a3.b_date as birthday,
                       |     a3.id_type,
                       |     a3.id_series,
                       |     a3.id_num,
                       |     a3.id_source as issued_code,
                       |     a3.id_date,
                       |     a3.crm_id as crm_id,
                       |     a3.m_phone_phonenumber as mob_tel
                       |     from
                       |     lk_clu a1
                       |     join ${config.pa}.lk a2 on (a1.u7m_id = a2.u7m_id_from and a2.t_to = 'CLF')
                       |     join ${config.pa}.clf a3 on (a2.u7m_id_to = a3.f7m_id)
                       |    where
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )
                       |   union all
                       |   select 
                       |     a3.f7m_id as u7m_id,
                       |     1 as role_id,
                       |     0 as client_type,
                       |     a3.inn,
                       |     NULL as ogrn,
                       |     a3.f_name as first_name,
                       |     a3.l_name as middle_name,
                       |     a3.s_name as last_name,
                       |     a3.b_date as birthday,
                       |     a3.id_type,
                       |     a3.id_series,
                       |     a3.id_num,
                       |     a3.id_source as issued_code,
                       |     a3.id_date,
                       |     a3.crm_id as crm_id,
                       |     a3.m_phone_phonenumber as mob_tel
                       |     from
                       |     lk_clu a1
                       |     join ${config.pa}.lk a2 on (a1.u7m_id = a2.u7m_id_to and a2.t_from = 'CLF')
                       |     join ${config.pa}.clf a3 on (a2.u7m_id_from = a3.f7m_id)
                       |    where
                       |     exists (
                       |      select
                       |        1
                       |        from
                       |        ${config.pa}.lkc a4
                       |        join ${config.stg}.rdm_link_criteria_mast a5 on (a4.crit_id = a5.code)
                       |       where
                       |        a4.lk_id  = a2.lk_id and
                       |        UPPER(a5.code) in ('BEN_CRM', '5.1.7BEN', '5.1.7BENREL', '5.1.7BENGD', 'EIOGEN_EKS', '5.1.6TP')
                       |     )
                       |  ) a ) b
                       | where
                       |  1 = CASE
                       |    WHEN client_type = 2 and inn is not NULL and ogrn is not NULL THEN 1
                       |    WHEN client_type = 1 and inn is not NULL THEN 1
                       |    WHEN client_type = 0 and first_name is not NULL and last_name is not NULL and (birthday is not NULL or (trim(coalesce(id_series, '')) <> '' and trim(coalesce(id_num, '')) <> '') or trim(coalesce(inn, '')) <> '') THEN 1
                       |    ELSE 0
                       |   END
                       | group by
                       |  u7m_id,
                       |  client_type,
                       |  inn,
                       |  ogrn,
                       |  first_name,
                       |  middle_name,
                       |  last_name,
                       |  birthday,
                       |  id_type,
                       |  id_series,
                       |  id_num,
                       |  issued_code,
                       |  id_date,
                       |  crm_id,
                       |  mob_tel
      """.stripMargin

    val relSelect = s"""select
                       |		a.role_id,
                       |		a.name
                       |  from
                       |		(
                       |			select 0 as role_id, 'Заемщик' as name
                       |			union all
                       |			select 1 as role_id, 'Участник группы' as name
                       |			union all
                       |			select 2 as role_id, 'Поручитель' as name
                       |			union all
                       |			select 3 as role_id, 'Залогодатель' as name
                       |		) a""".stripMargin

    val mmzDF = spark.sql(mmzSelect)
    mmzDF.cache().createOrReplaceTempView(mmz)

    val relDF = spark.sql(relSelect)

    dashboardName = s"${config.pa}.$mmz"

    spark.sql(s"drop table if exists ${config.pa}.$mmz")
    spark.sql(s"create table ${config.pa}.$mmz as select * from $mmz")

    logIntegrInserted()
    logInserted()

    val conf = spark.sparkContext.hadoopConfiguration
    implicit val fs = FileSystem.get(conf)

    val files = Seq(mmzDF, relDF).zip(Stream from 1).map { case (df, i) =>

      val num = s"000$i"
      dashboardName = fileName(num)
      val file = s"$targPath/$dashboardName"

      val count = df.count()
      val header = df.getHeader()

      df
        .quoteStrings()
        .formatDates(csvOptions("dateFormat"))
        .concat(csvOptions("sep"))
        .write
        .format("text")
        .mode("overwrite")
        .save(s"$targPath/$num")

      JavaUtils.copyMerge(
        fs, new Path(s"$targPath/$num"),
        fs, new Path(file),
        true, conf,
        null, header)

      logIntegrSaved(count)

      file
    }

    ZipUtils.zipFiles(s"$targPath/$packName", files)

    logIntegrEnd()
  }

}
