package ru.sberbank.sdcb.k7m.core.pack

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluToEksIdClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$DevSchema.dict_tb_mapping"
  val Node2t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$DevSchema.clu_keys"
  val Node3t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$DevSchema.basis_client"
  val Node4t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$Stg0Schema.eks_Z_CLIENT"
  val Node5t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$Stg0Schema.eks_z_ac_fin"
  val Node6t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$Stg0Schema.eks_Z_solClient"
  val Node7t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$Stg0Schema.eks_z_product"
  val Node8t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$Stg0Schema.eks_z_branch"
  val Node9t_team_k7m_aux_d_K7M_CLU_to_eks_idIN = s"$DevSchema.clu_extended_basis"
  val Nodet_team_k7m_aux_d_K7M_CLU_to_eks_idOUT = s"$DevSchema.CLU_to_eks_id"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CLU_to_eks_idOUT //витрина
  val dashboardPath = s"${config.auxPath}CLU_to_eks_id"
  override def processName: String = "clu_base"

  def DoCluToEksId(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CLU_to_eks_idOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      u7m_id,
      eks_id,
      count(*) over (partition by x.inn) EKS_DUPLICATE_CNT,
      row_number() over (partition by x.inn order by x.eks_id desc) priority,
      inn,
      kpp,
      tb_vko_crm_name,
      TB_CODE,
      acc_example,
      bal2_max,
      bal2_min,
      FLAG_BASIS_CLIENT
    from (
          select
                 max(bs.inn) inn,
                 max(c.c_kpp) kpp,
                 max(bb.tb_vko_crm_name) tb_vko_crm_name,
                 max(SUBSTR(b.c_local_code,1,2)) TB_CODE,
                 max(a.c_main_v_id) acc_example,
                 max(substr(a.c_main_v_id,1,5)) bal2_max,
                 max(substr(a.c_main_v_id,1,5)) bal2_min,
                 eb.FLAG_BASIS_CLIENT,
                 bs.u7m_id,
                 c.id eks_id
            from $Node9t_team_k7m_aux_d_K7M_CLU_to_eks_idIN eb
            join $Node2t_team_k7m_aux_d_K7M_CLU_to_eks_idIN  bs
              on eb.u7m_id = bs.u7m_id
            join $Node3t_team_k7m_aux_d_K7M_CLU_to_eks_idIN bb -- = t_team_k7m_aux_d.basis_client
              on bb.org_inn_crm_num = bs.inn
            JOIN $Node1t_team_k7m_aux_d_K7M_CLU_to_eks_idIN M   --Ручная таблица
              ON M.crm_tb_name = bb.tb_vko_crm_name
            join $Node4t_team_k7m_aux_d_K7M_CLU_to_eks_idIN c
              on bs.inn = c.c_inn
            join $Node5t_team_k7m_aux_d_K7M_CLU_to_eks_idIN a
              on  c.id   = coalesce(a.c_client_r,a.c_client_v)
            join $Node6t_team_k7m_aux_d_K7M_CLU_to_eks_idIN s
              on c.id = s.c_client
            join $Node7t_team_k7m_aux_d_K7M_CLU_to_eks_idIN  p --=t_team_k7m_prototypes.k7m_z_product, 0-слой
              on p.id =  s.id
            join $Node8t_team_k7m_aux_d_K7M_CLU_to_eks_idIN b
              on a.c_filial = b.id
           where CAST (date'${dateString}' AS TIMESTAMP) between a.c_date_op and coalesce(a.c_date_close,CAST (date'${dateString}' AS TIMESTAMP))
             and CAST (date'${dateString}' AS TIMESTAMP) between p.c_date_begin and coalesce(p.c_date_close,CAST (date'${dateString}' AS TIMESTAMP))
             and substr(a.c_main_v_id,1,5) between '40500' and '40807'
             and nvl(bs.status,'') ='ACTIVE'
             and eb.FLAG_BASIS_CLIENT ='Y'
             AND case when M.tb_code='00' then  '38' else M.tb_code end =  case when SUBSTR(b.c_local_code,1,2)='00' then  '38' else SUBSTR(b.c_local_code,1,2) end
             and upper(c.c_name) not like '%ФИЛИАЛ%'
             and upper(c.c_name) not like '%ОТДЕЛЕНИЕ%'
             and upper(c.c_name) not like '%УЧАСТОК%'
             and substr(c.c_kpp,5,2) not in ('02','03','04','05','31','32','43','44','45')
          group by
              bs.u7m_id,
              c.id,
              eb.FLAG_BASIS_CLIENT
    ) x"""
    ).persist()

    val recsWrittenCount1 = createHiveTableStage1.count()

    createHiveTableStage1
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLU_to_eks_idOUT")

    logInserted(count = recsWrittenCount1)

    val createHiveTableStage2 = spark.sql(
      s"""select
      u7m_id,
      eks_id,
      count(*) over (partition by x.inn) EKS_DUPLICATE_CNT,
      row_number() over (partition by  x.inn order by  x.is_dbo, x.is_acc, x.eks_id desc) priority,
      inn,
      kpp,
      cast(null as string) tb_vko_crm_name,
      cast(null as string) TB_CODE,
      acc_example,
      concat('is_dbo=',is_dbo) bal2_max,
      concat('is_acc=',is_acc) bal2_min,
      FLAG_BASIS_CLIENT
  from (
        select
               max(bs.inn) inn,
               max(c.c_kpp) kpp,
               max(a.c_main_v_id) acc_example,
               max(substr(a.c_main_v_id,1,5)) bal2_max,
               max(substr(a.c_main_v_id,1,5)) bal2_min,
               'N' FLAG_BASIS_CLIENT,
               bs.u7m_id,
               c.id eks_id,
               min(case
                 when p.id is null then '1'
                 else '0'
               end) is_dbo,
               min(case
                 when a.id is null then '1'
                 else '0'
               end) is_acc
          from $Node9t_team_k7m_aux_d_K7M_CLU_to_eks_idIN eb
          join $Node2t_team_k7m_aux_d_K7M_CLU_to_eks_idIN bs
            on eb.u7m_id = bs.u7m_id
          left join $Node3t_team_k7m_aux_d_K7M_CLU_to_eks_idIN bb
             on bb.org_inn_crm_num = bs.inn
          join $Node4t_team_k7m_aux_d_K7M_CLU_to_eks_idIN c
            on bs.inn = c.c_inn
          left join
                (
                select
                       ai.id,
                       ai.c_main_v_id,
                       coalesce(ai.c_client_r,ai.c_client_v) c_client
                  from $Node5t_team_k7m_aux_d_K7M_CLU_to_eks_idIN ai
                 where substr(ai.c_main_v_id,1,5) between '40500' and '40807'
                   and CAST (date'${dateString}'   AS TIMESTAMP)   between ai.c_date_op and coalesce(ai.c_date_close,CAST (date'${dateString}'   AS TIMESTAMP))
                ) a
            on c.id  = a.c_client
          left join $Node6t_team_k7m_aux_d_K7M_CLU_to_eks_idIN s
            on c.id = s.c_client
          left join
                (
                select p.id
                from  $Node7t_team_k7m_aux_d_K7M_CLU_to_eks_idIN p
                where CAST (date'${dateString}'   AS TIMESTAMP)   between p.c_date_begin and coalesce(p.c_date_close,CAST (date'${dateString}'   AS TIMESTAMP))
                ) p
            on p.id = s.id
         where nvl(bs.status,'') ='ACTIVE'
           and bb.org_inn_crm_num is null
           and eb.FLAG_BASIS_CLIENT ='N'
           and upper(c.c_name) not like '%ФИЛИАЛ%'
           and upper(c.c_name) not like '%ОТДЕЛЕНИЕ%'
           and upper(c.c_name) not like '%УЧАСТОК%'
           and substr(c.c_kpp,5,2) not in ('02','03','04','05','31','32','43','44','45')
         group by
                bs.u7m_id,
                c.id,
                eb.FLAG_BASIS_CLIENT
       ) x"""
    ).persist()

    val recsWrittenCount = createHiveTableStage2.count()

    createHiveTableStage2
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CLU_to_eks_idOUT")

    logInserted(count = recsWrittenCount)
    logEnd()
  }


}
