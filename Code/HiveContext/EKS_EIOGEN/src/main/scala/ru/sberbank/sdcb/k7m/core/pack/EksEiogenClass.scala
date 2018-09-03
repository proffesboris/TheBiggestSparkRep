package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EksEiogenClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_beneficiary"
  val Node2t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_CLIENT"
  val Node3t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_cl_priv"
  val Node4t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_CL_CORP"
  val Node5t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_CL_ORG"
  val Node6t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_benef_sign"
  val Node7t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${DevSchema}.basis_client"
  val Node8t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_persons_pos"
  val Node9t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_casta"
  val Node10t_team_k7m_aux_d_K7M_EKS_EIOGENIN = s"${Stg0Schema}.eks_z_certific_type"
  val eioEgrul = s"${DevSchema}.EIO_egrul"
  val eiogenUnfiltered = s"${DevSchema}.eks_eiogen_unfiltered"
  val eiogenEnrich = s"${DevSchema}.eiogen_enrich"
  val eiogenBad = s"${DevSchema}.k7m_eiogen_bad"
  val Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUT = s"${DevSchema}.K7M_EKS_EIOGEN"
  val dashboardPath = s"${config.auxPath}K7M_EKS_EIOGEN"
 

  var dashboardName: String = "EKS_EIOGEN" //витрина
  override def processName: String = "EKS_EIOGEN"

  def DoEksEiogen() {

    Logger.getLogger("org").setLevel(Level.WARN)
    logStart()

    val Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep = Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUT.concat("_Prep")

    dashboardName = Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep

    val createHiveTableStage1 = spark.sql(
      s"""select distinct
                    cl2.id
                  , cl.C_INN  as inn_benef
                  , cl.C_NAME as fio_benef
                  , clp.c_date_pers as birth_dt
                  , cft.c_kod as Doc_type
                  , clp.c_Doc_ser as doc_ser
                  , clp.c_Doc_num as doc_num
                  , clp.c_doc_date as doc_date
                  , clp.c_BORN
                  , clp.c_REG_STATUS
                  , clp.c_FAMILY_CL
                  , clp.c_SNAME_CL
                  , clp.c_NAME_CL
                  , cl2.C_INN  inn_org
                  , cl2.C_NAME name_org
                  , clc.C_BENEFICIARIES
                  , 1                    as link_prob
                  , 'EIOGEN_EKS'         as crit
                  , cas.C_VALUE position
                  , p_pos.c_work_begin
                  , p_pos.c_work_end
          from $Node2t_team_k7m_aux_d_K7M_EKS_EIOGENIN  cl2
         join  $Node4t_team_k7m_aux_d_K7M_EKS_EIOGENIN clc         on clc.id =cl2.id
         join  $Node8t_team_k7m_aux_d_K7M_EKS_EIOGENIN p_pos       on p_pos.collection_id = clc.C_ALL_BOSS and p_pos.C_SIGN_TODAY = '1' --cast(current_date as timestamp) >= p_pos.c_work_begin and cast(current_date as timestamp) <= p_pos.c_work_end
         join  $Node9t_team_k7m_aux_d_K7M_EKS_EIOGENIN cas         on p_pos.C_RANGE = cas.ID  and p_pos.C_EIO = '1'
         join  $Node3t_team_k7m_aux_d_K7M_EKS_EIOGENIN clp         on clp.ID  = p_pos.C_FASE
         join  $Node2t_team_k7m_aux_d_K7M_EKS_EIOGENIN  cl         on clp.ID  = cl.ID
         join  $Node10t_team_k7m_aux_d_K7M_EKS_EIOGENIN cft        on clp.c_doc_type = cft.id
       """
    )  // .createTempView(Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_TempEiogen")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep")

    logInserted()
    dashboardName = eiogenUnfiltered

    val createHiveTableStage2 = spark.sql(
      s"""select row_number() over (order by t.id) loc_id   --loc_id уникальный loc_id = EK_0123456789
                ,t.*
          from
(select distinct
          e.id id
         ,inn_benef
         ,fio_benef
         ,birth_dt
         ,doc_type
         ,doc_ser
         ,doc_num
         ,doc_date
         ,c_born
         ,c_reg_status
         ,c_family_cl
         ,c_sname_cl
         ,c_name_cl
         ,e.inn_org
         ,name_org
         ,c_beneficiaries
         ,link_prob
         ,crit
         ,position
         from $Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep e
         join
         (  select e.id, e.inn_org, count(1) cnt
         from $Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUTPrep e
       --  join $Node7t_team_k7m_aux_d_K7M_EKS_EIOGENIN b on b.org_inn_crm_num = e.inn_org
           where 1=1
         and (length(e.inn_benef) <> 10 or e.inn_benef is null)
           group by e.id, e.inn_org
           having cnt=1
         ) t
         on e.id = t.id and t.inn_org = e.inn_org) t
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}eks_eiogen_unfiltered")
      .saveAsTable(s"$eiogenUnfiltered")

    logInserted()
    dashboardName = eiogenEnrich

    spark.sql(s"""
                  select eks.*
                  , upper(concat(egr.s_name,'_',egr.f_name,'_',egr.l_name)) egr_fio
                  , trim(upper(egr.position)) egr_position
                  , case when trim(upper(eks.position)) = trim(upper(egr.position)) then 0 else 1 end as pos_flg
                  , case when upper(concat(eks.c_family_cl,'_',eks.c_name_cl,'_',eks.c_sname_cl)) = upper(concat(egr.s_name,'_',egr.f_name,'_',egr.l_name)) then 0 else 1 end as fio_flg
                  from ${config.aux}.eks_eiogen_unfiltered eks
                  join ${config.aux}.eio_egrul egr on egr.inn = eks.inn_org
                  union all
                  select eks.*
                  , '' as egr_fio
                  , '' as egr_position
                  , 0 as pos_flg
                  , 0 as fio_flg
                  from ${config.aux}.eks_eiogen_unfiltered eks
                  left join ${config.aux}.eio_egrul egr on egr.inn = eks.inn_org
                  where egr.inn is null
                  """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}eiogen_enrich")
      .saveAsTable(s"$eiogenEnrich")

    logInserted()
    dashboardName = eiogenBad

    spark.sql(s"""
          select e.*
          from ${config.aux}.eiogen_enrich e
          where (pos_flg + fio_flg) > 0
          """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}k7m_eiogen_bad")
      .saveAsTable(s"$eiogenBad")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUT

    spark.sql(s"""
          select e.*
          from ${config.aux}.eiogen_enrich e
          where (pos_flg + fio_flg) = 0
          """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(Nodet_team_k7m_aux_d_K7M_EKS_EIOGENOUT)

    logInserted()
    logEnd()
  }
}


