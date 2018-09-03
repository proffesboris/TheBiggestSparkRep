package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class DashboardClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  //val dashboardPath = s"${config.auxPath}STG_GSZ_CRMLK"
  val Nodet_team_k7m_aux_d_clf_vkoOUT = s"${config.aux}.k7m_vko"
  val dashboardPathVko = s"${config.auxPath}k7m_vko"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_vkoOUT //витрина
  override def processName: String = "GSZ"


   def DoSmartBuyers(spark:org.apache.spark.sql.SparkSession,
                    SourceSchemaName: String,
                    TargSchemaName: String,
                    TargTableName: String
                   ) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val smartSrcHiveTable_t7 = spark.sql(
      s""" select row_id as ID, name, descr_text
         from $SourceSchemaName.CRM_CX_GSZ_CRIT_ADM
      """
    )
   smartSrcHiveTable_t7.write.format("parquet").mode("overwrite").saveAsTable(s"$TargSchemaName$TargTableName")

  }

  def DoSmartGSZ() {
    logStart()
//   val smartSrcHiveTable_t7 = spark.sql(
//     s""" select distinct CRIT_ID,
//                        ID_FROM,
//                        name_FROM,
//                        inn_FROM,
//                        kio_FROM,
//                        ID_TO,
//                        name_TO,
//                        inn_TO,
//                        kio_TO,
//                        gsz_id,
//                        gsz_name
//          from (select I3.CRIT_ID --Id критерия
//                      ,org2.id        as ID_FROM --Id св. участника
//                      ,org2.full_name as name_FROM --Участник
//                      ,org2.inn       as inn_FROM
//                      ,org2.kio       as kio_FROM
//                      ,org.id         as ID_TO --Id участника
//                      ,org.full_name  as name_TO --Участник
//                      ,org.inn        as inn_TO
//                      ,org.kio        as kio_TO
//                      ,gsz.row_id     as gsz_id --Id ГСЗ
//                      ,gsz.name       as gsz_name --Название ГСЗ
//                  from ${config.stg}.crm_cx_gsz gsz
//                  join ${config.stg}.crm_CX_PARTY_GSZ_X gsz_x
//                    on (gsz_x.gsz_id = gsz.row_id)
//                  join ${config.aux}.K7M_crm_org org
//                    on (org.id = gsz_x.row_id)
//                  join ${config.stg}.crm_CX_GSZMEM_INTER i
//                    on (i.PARTY_ID = org.id)
//                  join ${config.stg}.crm_CX_GSZMEM_INTER i2
//                    on (i2.LINK_ID = i.LINK_ID)
//                  join ${config.aux}.K7M_crm_org org2
//                    on (org2.id = i2.PARTY_ID)
//                  join ${config.stg}.crm_CX_GSZ_CRITER i3
//                    on (i3.LINK_ID = i2.LINK_ID)
//                 where (i2.PARTY_ID <> org.id))
//     """
//   )
//   smartSrcHiveTable_t7
//     .write
//     .format("parquet")
//     .mode("overwrite")
//     .option("path", dashboardPath)
//     .saveAsTable(dashboardName)

    val createK7mVko  = spark.sql(
      s""" select
           row_number() over(order by t.crm_id) as loc_id
         , t.crm_id, t.eks_id, t.inn, t.fio, t.last_name, t.fst_name, t.mid_name, t.birth_dt
        -- ,cell_ph_num,work_ph_num
         , regexp_replace(regexp_replace(cell_ph_num,' [0() -]+$$', ''),'[А-Яа-яA-Za-z]','') as cell_ph_num
         , regexp_replace(regexp_replace(work_ph_num,' [0() -]+$$', ''),'[А-Яа-я,A-Za-z]','') as work_ph_num
         , t.ALT_EMAIL_ADDR, t.Doc_type
         , t.doc_ser, t.doc_num
         , t.terbank
         , t.ter_podr
         , t.func_podr
         , t.crit, t.org_crm_id, t.org_inn_crm_num
         from
         (select distinct
          c.row_id 											as crm_id
         , cast(null as string)     							as eks_id
         , cast(null as string) 	   							as inn
         , concat(c.last_name,' ',c.fst_name,' ',c.mid_name) as fio
         , c.last_name
         , c.fst_name
         , c.mid_name
         , c.birth_dt    									as birth_dt
         , c.cell_ph_num as cell_ph_num
         , c.work_ph_num as work_ph_num
         , c.ALT_EMAIL_ADDR
         , '21'             									as Doc_type
         , rd.series        									as doc_ser
         , rd.doc_number    									as doc_num
         , max(ext2.name)  as terbank
         , max(spar3.name) as ter_podr
         , max(ext3.name)  as func_podr
         , 'VK' as crit
         , b.org_crm_id as org_crm_id
         , b.org_inn_crm_num as org_inn_crm_num
         from ${config.aux}.basis_client b
         inner join ${config.stg}.crm_s_org_ext  ext   on ext.row_id = b.org_crm_id
         inner join ${config.stg}.crm_s_postn   pos    on pos.row_id = ext.pr_postn_id
         inner join ${config.stg}.crm_s_contact c      on c.par_row_id = pos.pr_emp_id and c.emp_flg = 'Y'
         inner join ${config.stg}.crm_s_org_ext ext2   on ext2.par_row_id = pos.bu_id
         inner join ${config.stg}.crm_s_org_ext ext3   on ext3.par_row_id=pos.OU_ID
         inner join ${config.stg}.crm_s_party spar3    on spar3.row_id=ext3.X_OSB_DIV_ID
         inner join ${config.stg}.rdm_rdm_int_org_mast_v mast on spar3.row_id=mast.crmorgunitid
         left join  ${config.stg}.crm_CX_REG_DOC rd on c.row_id = rd.obj_id and rd.status = 'Действует'
         group by c.row_id, concat(c.last_name,' ',c.fst_name,' ',c.mid_name), c.last_name
         , c.fst_name
         , c.mid_name
         , c.birth_dt
         , c.cell_ph_num
         , c.work_ph_num
         , c.ALT_EMAIL_ADDR , rd.series
         , rd.doc_number, b.org_crm_id, b.org_inn_crm_num
         ) t
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", {dashboardPathVko})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_vkoOUT")

    logInserted()
    logEnd()
  }

}

