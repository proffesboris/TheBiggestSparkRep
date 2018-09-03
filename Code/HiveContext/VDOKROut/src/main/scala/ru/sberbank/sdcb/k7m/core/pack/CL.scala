package ru.sberbank.sdcb.k7m.core.pack

import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.Utils._
import ru.sberbank.sdcb.k7m.java.{Utils => JavaUtils}

class CL(override val spark: SparkSession, val config: Config, date: String) extends EtlJob with EtlIntegrLogger with EtlLogger {

  override val processName = "VDOKROutCL"
  var dashboardName: String = "VDOKROutCL"

  override val logSchemaName: String = config.aux
  val targPath = s"${config.paPath}/export/vdokrout"
  val clPack = s"7m_1_${date}_cl"

  val conf = spark.sparkContext.hadoopConfiguration
  implicit val fs = FileSystem.get(conf)

  private val bDate = LoggerUtils.obtainRunDt(spark, logSchemaName)

  case class Data(name: String, df: DataFrame)

  def csvOptions = Map(
    "sep" -> ";",
    "header" -> "false",
    "escape" -> "\"",
    "dateFormat" -> "yyyy-MM-dd",
    "timestampFormat" -> "yyyy-MM-dd")

  val cluFilter = spark.sql(s"""select clu.u7m_id from ${config.pa}.clu clu
    left join ${config.pa}.CLU_CHECK b on (clu.U7M_ID = b.U7M_ID and b.check_name = 'GEN_TOTAL_CHECK')
    where
    COALESCE(b.check_flg, 0) = 0
    --remove after psi
    and clu.crm_id is not null and clu.eks_id is not null""")

  def clu: Data = {

    dashboardName = s"${config.aux}.vdokrout_clu"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_clu")

    spark.sql(
      s"""--create table ${config.aux}.vdokrout_clu as
         |select
         |a.exec_id,
         |a.u7m_id,
         |crm_id,
         |flag_resident,
         |inn,
         |kio,
         |cast(substr(org_id,1,22) as string) as org_id,
         |uc_id,
         |mg_id,
         |coalesce(cast(substr(ogrn,1,15)as string), '1111111111111')  as ogrn,
         |cast(substr(org_ogrn_s_n,1,15) as string) as org_ogrn_s_n,
         |coalesce(cast(substr(ogrn_sn,1,15) as string), '77') as ogrn_sn,
         |coalesce(cast(substr(ogrn_code,1,15) as string), '111111111') as ogrn_code,
         |eks_id,
         |flag_basis_client,
         |kpp,
         |okpo,
         |cast(substr(full_name,1,1000) as string) as full_name,
         |cast(substr(short_name,1,256) as string) as short_name,
         |opf_eks_cd,
         |opf_eks_nm,
         |crm_opf_nm,
         |cast(substr(opf,1,30) as string) as opf,
         |opf_nm,
         |reg_date_crm,
         |reg_date_integrum,
         |from_unixtime(unix_timestamp(reg_date),  'YYYY-MM-dd') as reg_date,
         |reg_country,
         |cast(coalesce(substr(reg_address_postalcode,1,6),'111111') as string)  as reg_address_postalcode,
         |cast(coalesce(substr(reg_address_regionname,1,100),'%') as string)  as reg_address_regionname,
         |reg_address_shortregiontypename,
         |cast(coalesce(substr(reg_address_districtname,1,60),'%') as string) as reg_address_districtname,
         |reg_address_shortdistricttypename,
         |cast(coalesce(substr(reg_address_cityname,1,50),'%') as string) as reg_address_cityname,
         |reg_address_shortcitytypename,
         |cast(coalesce(substr(reg_address_villagename,1,60), '%') as string) as reg_address_villagename,
         |cast(coalesce(substr(reg_address_shortvillagetypename,1,10),'%') as string) as reg_address_shortvillagetypename,
         |cast(coalesce(substr(reg_address_streetname,1,50),'%') as string) as reg_address_streetname,
         |cast(coalesce(substr(reg_address_shortstreettypename,1,16),'%') as string) as reg_address_shortstreettypename,
         |reg_address_housenumber,
         |reg_address_buildingnumber,
         |reg_address_blocknumber,
         |reg_address_flatnumber,
         |cast(coalesce(substr(o_phone_phonenumber,1,18),'111-11-11') as string) as o_phone_phonenumber,
         |cast(substr(m_phone_phonenumber,1,18) as string) as m_phone_phonenumber,
         |cast(substr(email,1,60) as string) as email,
         |cast(coalesce(substr(f_address_postalcode,1,6),'111111') as string) as f_address_postalcode,
         |cast(coalesce(substr(f_address_regionname,1,100), '%') as string)  as f_address_regionname,
         |cast(substr(f_address_districtname,1,60) as string)  as f_address_districtname,
         |cast(coalesce(substr(f_address_cityname,1,50), '%') as string)  as f_address_cityname,
         |cast(coalesce(substr(f_address_villagename,1,60), '%') as string)  as  f_address_villagename,
         |cast(coalesce(substr(f_address_shortvillagetypename,1,60), '%') as string)  as f_address_shortvillagetypename,
         |cast(coalesce(substr(f_address_streetname,1,50), '%') as string)  as f_address_streetname,
         |cast(coalesce(substr(f_address_shortstreettypename,1,16), '%') as string)  as f_address_shortstreettypename,
         |f_address_housenumber,
         |f_address_buildingnumber,
         |f_address_blocknumber,
         |f_address_flatnumber,
         |fax,
         |crm_gsz_id,
         |crm_top_gsz_id,
         |cast(coalesce(substr(bsegment_eng,1,16),'-') as string)  as bsegment,
         |rsegment,
         |okk_cd,
         |okk,
         |cast(coalesce(substr(soun,1,15),'000000000000000') as string)  as soun,
         |coalesce(from_unixtime(unix_timestamp(soun_data),  'YYYY-MM-dd'), '2005-12-31') as soun_data,
         |cast(coalesce(substr(industry,1,255),'НЕИЗВЕСТНАЯ ТОРГОВЛЯ') as string)  as industry,
         |okved_crm_cd,
         |cast(coalesce(substr(okved,1,22),'11.11') as string)  as okved,
         |tb_vko_id,
         |tb_vko_name,
         |borrower_flag,
         |pd_main_process_offline,
         |rating_appr_dt_offline,
         |rating_offline_calc_id,
         |rating_main_process_offline,
         |rating_main_process_price_offline,
         |rating_main_process_rezerv_offline,
         |rating_model_name_offline,
         |rating_online_calc_id,
         |from_unixtime(unix_timestamp(rep_date_offline),  'YYYY-MM-dd') as rep_date_offline,
         |coalesce(from_unixtime(unix_timestamp(sbbol_ddog),  'YYYY-MM-dd'), '2011-01-26') as sbbol_ddog,
         |cast(coalesce(sbbol_ndog,'1111/1111/111111') as string) as sbbol_ndog,
         |cast(coalesce(substr(sclir,1,38),'1111') as string) as sclir,
         |cast(coalesce(substr(ru_id,1,100),'-') as string) as ru_id,
         |cast(coalesce(offline_ru_id,'-1') as string) as offline_ru_id, --SDCB-442
         |cast(coalesce(offline_ru_mark, 5) as int) as offline_ru_mark, --SDCB-442
         |ru_ogrn,
         |ru_kpp,
         |ru_full_name,
         |ru_short_name,
         |ru_opf,
         |ben_flag,
         |cast(coalesce(assets,'0') as decimal(16,4))  as assets,
         |cast(coalesce(active_exp_pl,'0') as decimal(16,4)) as active_exp_pl,
         |active_gua_pl,
         |active_sur_pl,
         |lim_struct
         |from ${config.pa}.clu a""".stripMargin)
      .join(broadcast(cluFilter), Seq("u7m_id"))
      .write
      .format("parquet")
      .option("path", s"${config.auxPath}vdokrout_clu")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${config.aux}.vdokrout_clu")

    logIntegrInserted()
    logInserted()

    Data("CLU", spark.sql(
      s"""select
         |    a.U7M_ID,
         |    a.CRM_ID,
         |    a.INN,
         |    a.EKS_ID,
         |    a.ORG_ID,
         |    a.UC_ID,
         |    a.MG_ID
         |  from
         |    ${config.aux}.vdokrout_clu a""".stripMargin))
  }

  def lk: Data = {

    dashboardName = s"${config.aux}.vdokrout_lk"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_lk")

    spark.sql(
      s"""
         |select
         |    a.*
         |  from
         |     ${config.pa}.LK a
         |""".stripMargin)
      .join(broadcast(cluFilter).select(col("u7m_id") as "psi_from"), col("u7m_id_from") === col("psi_from") and col("t_from") === lit("CLU"),"left")
      .join(broadcast(cluFilter).select(col("u7m_id") as "psi_to"), col("u7m_id_to") === col("psi_to") and col("t_to") === lit("CLU"),"inner")
      .join(broadcast(spark.table(s"${config.pa}.CLF")).select(col("f7m_id") as "clf_from"), col("u7m_id_from") === col("clf_from") and col("t_from") === lit("CLF"),"left")
      .filter(col("clf_from").isNotNull or col("psi_from").isNotNull)
      .write
      .format("parquet")
      .option("path", s"${config.auxPath}vdokrout_lk")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${config.aux}.vdokrout_lk")

    logIntegrInserted()
    logInserted()

    Data("LK", spark.sql(
      s"""select
         |    a.LK_ID,
         |    a.U7M_ID_FROM,
         |    a.T_FROM,
         |    a.U7M_ID_TO,
         |    a.T_TO
         |  from
         |    ${config.aux}.vdokrout_lk a""".stripMargin))
  }

  def lkc: Data = {

    dashboardName = s"${config.aux}.vdokrout_lkc"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_lkc")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_lkc as
         |select
         |    a.*
         |  from
         |    ${config.pa}.LKC a
         | where
         |    exists (select 1 from ${config.aux}.vdokrout_lk a1 where a1.lk_id = a.lk_id)""".stripMargin)

    logIntegrInserted()
    logInserted()

    Data("LKC", spark.sql(
      s"""select
         |    a.LKC_ID,
         |    a.LK_ID,
         |    a.CRIT_ID
         |  from
         |    ${config.aux}.vdokrout_lkc a""".stripMargin))
  }

  def score: Data = {

    dashboardName = s"${config.aux}.vdokrout_score"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_score")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_score as
         |select
         |    a.U7M_ID
         |  from
         |    ${config.pa}.score_keys a
         |    inner join ${config.aux}.vdokrout_clu b
         |            on a.U7M_ID = b.U7M_ID
         | left join ${config.pa}.SCORE_CHECK c on (a.U7M_ID = c.U7M_ID and c.check_name = 'GEN_TOTAL_CHECK')
         | where COALESCE(c.check_flg, 0) = 0
          """.stripMargin)

    logIntegrInserted()
    logInserted()
    dashboardName = s"${config.aux}.clf_position_eio"
    spark.sql(s"drop table if exists ${config.aux}.clf_position_eio")
    spark.sql(s"""
    create table ${config.aux}.clf_position_eio
    stored as parquet
    as
    select
    lk.u7m_id_from as f7m_id,
    max(lkc.post_nm_from) as position_eio,
    min(lkc.post_nm_from) as alternative_position_eio,
    count(distinct regexp_replace(lower(lkc.post_nm_from),' ','')) count_of_alternatives
    from
    (
      select
        distinct u7m_id
        from
        (
          select u7m_id
            from ${config.aux}.vdokrout_score
        union all
        select 7m_id as u7m_id
        from ${config.pa}.set_item
    where obj='CLU') x
    ) s
    join ${config.pa}.lk  lk
      on s.u7m_id = lk.u7m_id_to
    join ${config.pa}.lkc lkc
      on lk.lk_id = lkc.lk_id
    join ${config.pa}.clf clf
      on lk.u7m_id_from = clf.f7m_id
    where lkc.crit_id like 'EIO%'
    group by lk.u7m_id_from
    """.stripMargin)
    logInserted()
    Data("SCORE", spark.table(s"${config.aux}.vdokrout_score"))
  }



  def clf: Data = {

    dashboardName = s"${config.aux}.vdokrout_clf"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_clf")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_clf as
         |select
         |a.f7m_id,
         |crmr_id,
         |transact_id,
         |crm_id,
         |coalesce(cast(substr(s_name,1,40) as string),'-') as s_name,
         |coalesce(cast(substr(f_name,1,40) as string),'-') as f_name,
         |cast(substr(l_name,1,40) as string) as l_name,
         |cast(substr(b_place,1,250) as string) as b_place,
         |coalesce(from_unixtime(unix_timestamp(b_date),  'YYYY-MM-dd'), '1900-01-01') as b_date,
         |coalesce(cast(substr(id_type,1,2) as string),'-') as id_type,
         |coalesce(cast(substr(id_series,1,20) as string),'-') as id_series,
         |coalesce(cast(substr(id_num,1,20) as string),'-') as id_num,
         |coalesce(cast(substr(id_source,1,160) as string),'-') as   id_source,
         |coalesce(cast(substr(id_source_kod,1,7) as string),'-') as   id_source_kod,
         |coalesce(from_unixtime(unix_timestamp(id_date),  'YYYY-MM-dd'), '1900-01-01') as id_date,
         |coalesce(inn, '111111111111' ) as inn,
         |cast(substr(d_address_country,1,100) as string) as d_address_country,
         |cast(substr(d_address_postalcode,1,6) as string) as d_address_postalcode,
         |cast(substr(d_address_regionname,1,100) as string) as d_address_regionname,
         |cast(substr(d_address_districtname,1,60) as string) as d_address_districtname,
         |cast(substr(d_address_cityname,1,50) as string) as d_address_cityname,
         |cast(substr(d_address_villagename,1,60) as string) as d_address_villagename,
         |coalesce(cast(substr(d_address_shortvillagetypename,1,10) as string),'%') as d_address_shortvillagetypename,
         |coalesce(cast(substr(d_address_streetname,1,50) as string),'%') as d_address_streetname,
         |coalesce(cast(substr(d_address_shortstreettypename,1,16) as string),'%') as d_address_shortstreettypename,
         |cast(substr(d_address_housenumber,1,20) as string) as d_address_housenumber,
         |cast(substr(d_address_buildingnumber,1,20) as string) as d_address_buildingnumber,
         |cast(substr(d_address_blocknumber,1,20) as string) as d_address_blocknumber,
         |cast(substr(d_address_flatnumber,1,20) as string) as d_address_flatnumber,
         |cast(substr(f_address_country,1,20) as string) as  f_address_country,
         |cast(substr(f_address_postalcode,1,6) as string) as f_address_postalcode,
         |cast(substr(f_address_regionname,1,100) as string) as f_address_regionname,
         |cast(substr(f_address_districtname,1,60) as string) as f_address_districtname,
         |cast(substr(f_address_cityname,1,50) as string) as f_address_cityname,
         |cast(substr(f_address_villagename,1,60) as string) as  f_address_villagename,
         |coalesce(cast(substr(f_address_shortvillagetypename,1,60) as string),'%')  as  f_address_shortvillagetypename,
         |coalesce(cast(substr(f_address_streetname,1,50) as string),'%')  as f_address_streetname,
         |coalesce(cast(substr(f_address_shortstreettypename,1,16) as string),'%')  as f_address_shortstreettypename,
         |cast(substr(f_address_housenumber,1,20) as string) as f_address_housenumber,
         |cast(substr(f_address_buildingnumber,1,20) as string) as f_address_buildingnumber,
         |cast(substr(f_address_blocknumber,1,20) as string) as  f_address_blocknumber,
         |cast(substr(f_address_flatnumber,1,20) as string) as   f_address_flatnumber,
         |cast(substr(snils,1,15) as string) as  snils,
         |cast(substr(h_phone_phonenumber,1,18) as string) as h_phone_phonenumber,
         |cast(substr(w_phone_phonenumber,1,18) as string) as w_phone_phonenumber,
         |cast(substr(m_phone_phonenumber,1,18) as string) as m_phone_phonenumber,
         |from_unixtime(unix_timestamp(udbo_agree_date),  'YYYY-MM-dd') as udbo_agree_date,
         |cast(substr(udbo_agree_no,1,20) as string) as udbo_agree_no,
         |coalesce(cast(e.position_eio as string),'-') as position,
         |email,
         |func_division,
         |ter_division,
         |km_tb,
         |a.exec_id
         from
             ${config.pa}.CLF a
            inner join (
             select distinct lk.7m_id from
               (select u7m_id_from as 7m_id from ${config.aux}.vdokrout_lk where t_from = 'CLF'
               union all
               select u7m_id_to as 7m_id from ${config.aux}.vdokrout_lk where t_to = 'CLF') lk
            ) b on (a.f7m_id = b.7m_id)
         left join ${config.aux}.clf_position_eio e
         on e.f7m_id = a.f7m_id
         """.stripMargin)

    logIntegrInserted()
    logInserted()


    Data("CLF", spark.sql(
      s"""select
         |    a.F7M_ID,
         |    a.CRMR_ID,
         |    a.TRANSACT_ID,
         |    a.CRM_ID
         |  from
         |    ${config.aux}.vdokrout_clf a""".stripMargin))

  }


  def set: Data = {

    dashboardName = s"${config.aux}.vdokrout_set"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_set")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_set as
         select
             a.SET_ID,
             a.U7M_B_ID
         from
             ${config.pa}.SET a
                 inner join ${config.aux}.vdokrout_score b
                         on a.U7M_B_ID = b.U7M_ID
         """.stripMargin)

    logIntegrInserted()
    logInserted()

    Data("SET", spark.table(s"${config.aux}.vdokrout_set"))
  }

  def setItem: Data = {

    dashboardName = s"${config.aux}.vdokrout_set_item"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_set_item")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_set_item as
         select
             a.*
         from
             ${config.pa}.SET_ITEM a
             inner join ${config.aux}.vdokrout_set b
                         on a.SET_ID = b.SET_ID

            left join ${config.aux}.vdokrout_clu clu
             on a.7m_id = clu.u7m_id
                         and a.obj = 'CLU'
            left join ${config.aux}.vdokrout_clf clf
             on a.7m_id = clf.f7m_id
              and a.obj = 'CLF'
         where
           (
            a.obj = 'CLU'
              and clu.u7m_id is not null
            or a.obj = 'CLF'
              and clf.f7m_id is not null
          )
         and exists (select 1 from ${config.aux}.vdokrout_set a1 where a1.set_id = a.set_id)""".stripMargin)

    logIntegrInserted()
    logInserted()

    Data("SET_ITEM", spark.sql(
      s"""select
         |    a.SET_ITEM_ID,
         |    a.SET_ID,
         |    a.7M_ID,
         |    a.OBJ
         |  from
         |    ${config.aux}.vdokrout_set_item a""".stripMargin))

  }

  def acc: Data = {

    dashboardName = s"${config.aux}.vdokrout_acc"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_acc")

    spark.sql(s"""create table ${config.aux}.vdokrout_acc as
                 |select
                 |cast(cast(a.ACC_ID as bigint) as string) as ACC_ID,
                 |'' exec_id,
                 |a.ACC_NO,
                 |a.ACC_D_NO,
                 |a.ACC_D_DT,
                 |a.CL_EKS_ID,
                 |a.FLAG_OVER,
                 |a.ACC_CURRENCY,
                 |a.ACC_BIK,
                 |coalesce(b.u7m_id, c.7m_id) as 7m_id,
                 |'CLU' as obj
                 |from
                 |${config.pa}.ACC a
                 Left join ${config.aux}.vdokrout_clu b on a.U7M_ID = b.U7M_ID and flag_basis_client = 'Y' --необходимо при окончании ПСИ vdokrout_clu заменить на vdokrout_clu
                 inner join ${config.pa}.clu clu on a.U7M_ID = clu.U7M_ID
                 Left join
                 (
                   select
                      distinct 7m_id
                   from ${config.aux}.vdokrout_set_item c
                   where  obj = 'CLU'
                  ) c on clu.u7m_id = c.7m_id
                 where
                 substr(a.ACC_NO, 1, 3) between '405%' and '407%'
                 And (c.7m_id is not null or b.u7m_id is not null)
                 And cast('$bDate' as timestamp) between a.acc_date_op and coalesce(a.acc_date_close, cast('9999-12-31 00:00:00' as timestamp))""".stripMargin)

    logIntegrInserted()
    logInserted()

    Data("ACC", spark.sql(
      s"""select
         |    coalesce(a.ACC_ID, '-') as ACC_ID,
         |    coalesce(a.OBJ, '-') as OBJ,
         |    coalesce(a.7M_ID, '-') as 7M_ID,
         |    coalesce(a.ACC_NO, '-') as ACC_NO,
         |    coalesce(a.ACC_D_NO, '-') as ACC_D_NO,
         |    coalesce(a.ACC_D_DT, cast('1900-01-01' as timestamp)) as ACC_D_DT,
         |    coalesce(a.CL_EKS_ID, '-') as CL_EKS_ID
         |  from
         |    ${config.aux}.vdokrout_acc a""".stripMargin))

  }

  def rdoc: Data = {

    dashboardName = s"${config.aux}.vdokrout_rdoc"

    spark.sql(s"drop table if exists ${config.aux}.vdokrout_rdoc")

    spark.sql(
      s"""create table ${config.aux}.vdokrout_rdoc as
         |select
         |    a.*
         |  from
         |    ${config.pa}.rdoc a
         |    --SDCB-541 выгружаем документы только по клиентам, которые попали в выгрузку
         |    inner join ${config.aux}.vdokrout_clu b
         |      on a.7m_id = b.u7m_id
         |    where DOC_NAME is not null and ID_DOC_ECM is not null""".stripMargin)

    logIntegrInserted()
    logInserted()

    Data("RDOC", spark.sql(
      s"""select
         |    a.RDOC_ID,
         |    a.OBJ,
         |    a.7M_ID
         |  from
         |    ${config.aux}.vdokrout_rdoc a""".stripMargin))

  }

  def dt: Data = {

    ToKeyValue.gen(config.aux, "vdokrout_clu", config.aux, "vdokrout_clu_kv")(spark, config.aux, config.auxPath)
    ToKeyValue.gen(config.aux, "vdokrout_lk", config.aux, "vdokrout_lk_kv")(spark, config.aux, config.auxPath)
    ToKeyValue.gen(config.aux, "vdokrout_lkc", config.aux, "vdokrout_lkc_kv")(spark, config.aux, config.auxPath)
    ToKeyValue.gen(config.aux, "vdokrout_clf", config.aux, "vdokrout_clf_kv")(spark, config.aux, config.auxPath)
   // ToKeyValue.gen(config.aux, "vdokrout_set_item", config.aux, "vdokrout_set_item_kv")(spark, config.aux, config.auxPath)
    ToKeyValue.gen(config.aux, "vdokrout_rdoc", config.aux, "vdokrout_rdoc_kv")(spark, config.aux, config.auxPath)
    ToKeyValue.gen(config.aux, "vdokrout_acc", config.aux, "vdokrout_acc_kv")(spark, config.aux, config.auxPath)

    Data("DT", spark.sql(
      s"""select 'SCORE' as obj, a.u7m_id as obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.pa}.score a inner join ${config.aux}.vdokrout_score b on a.U7M_ID = b.U7M_ID
         |union all
         |select 'CLU' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_clu_kv a
         |union all
         |select 'LK' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_lk_kv a
         |union all
         |select 'LKC' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_lkc_kv a
         |union all
         |select 'CLF' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_clf_kv a
         |union all
        -- |select 'SET_ITEM' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_set_item_kv a
        -- |union all
         |select 'RDOC' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_rdoc_kv a
         |union all
         |select 'ACC' as obj, a.obj_id, a.key, a.value_c, a.value_n, a.value_d from ${config.aux}.vdokrout_acc_kv a
         |""".stripMargin))

  }

  def run(): Unit = {

    logIntegrStart()

    Seq(clu, lk, lkc, score, clf, set, setItem, acc, rdoc, dt).foreach { case Data(name, df) => {

      dashboardName = s"$clPack/$name.csv"

      val count = df.count()
      val header = df.getHeader()

      df
        .replace("[\\n,\\r,\\;]", " ")
        .formatNumbers()
        .write
        .mode(SaveMode.Overwrite)
        .options(csvOptions)
        .csv(s"$targPath/$clPack/$name")

      JavaUtils.copyMerge(
        fs, new Path(s"$targPath/$clPack/$name"),
        fs, new Path(s"$targPath/$dashboardName"),
        true, conf,
        null, header)

      logIntegrSaved(count)
    }
    }

    ZipUtils.zipDir(new Path(s"$targPath/$clPack"))

    logIntegrEnd()

  }

}
