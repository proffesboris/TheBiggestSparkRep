package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfRawKeysClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

//  val Node1t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.K7M_KM"
  val Node2t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.K7M_EKS_EIOGEN"
  val Node3t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.mrt3_gsl_final"
  val Node4t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.k7m_crmlk" //из CRM_BASE
  val Node5t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.basis_client"
  val Node6t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.exsgen"
 // val Node7t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.K7M_INTEGRUM_EIO"
 // val Node8t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.K7M_CRM_BEN"
 val Node9t_team_k7m_aux_d_clf_raw_keysIN = s"${DevSchema}.k7m_vko"
   val Nodet_team_k7m_aux_d_clf_raw_keysOUT = s"${DevSchema}.clf_raw_keys"
  val dashboardPath = s"${config.auxPath}clf_raw_keys"
//  val dashboardPathVko = s"${config.auxPath}k7m_vko"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_raw_keysOUT //витрина
  override def processName: String = "CLF"

  def DoClfRawKeys()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_raw_keysOUT).setLevel(Level.WARN)

//    val createHiveTableStage1 = spark.sql(
//      s""" --1. KM_
//         select
//          concat('KM_',loc_id)        as id  -- KM_0123456789
//         , km_id 	                    as crm_id
//         , cast(null as string)       as eks_id
//         , cast(null as string) 	    as inn
//         , concat(s_name,' ',f_name,' ',l_name)  as fio
//         , b_date                     as birth_dt
//         , cast(null as string)       as Doc_type
//         , cast(null as string)       as doc_ser
//         , cast(null as string)       as doc_num
//         , m_phone                    as cell_ph_num
//         ,'CRM_KM'                    as crit
//         from $Node1t_team_k7m_aux_d_clf_raw_keysIN k
//         join $Node5t_team_k7m_aux_d_clf_raw_keysIN b on b.org_crm_id = k.crm_org_id
//    """
//    )
//      .write.format("parquet")
//      .mode(SaveMode.Overwrite)
//      .option("path", dashboardPath)
//      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    val createHiveTableStage2 = spark.sql(
      s""" --2. EK_
        select
          concat('EK_',loc_id) as id  -- EK_0123456789
        , cast(null as string) 		     as crm_id
        , cast(e.id as string)           as eks_id
        , inn_benef                    as inn
        , fio_benef                    as fio
        , birth_dt
        , cast(Doc_type as string)     as Doc_type
        , doc_ser                      as doc_ser
        , doc_num                      as doc_num
        , cast(doc_date as timestamp)  as doc_date
        , cast(null as string)  		   as cell_ph_num
        , crit
        , cast(position as string)     as position --210518
        from $Node2t_team_k7m_aux_d_clf_raw_keysIN e
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    val createHiveTableStage3 = spark.sql(
      s""" --3 UL_
         select
           concat('UL_',loc_id) as id   -- UL_0123456789
         , cast(crm_id1 as string) 		   as crm_id
         , cast(null as string)           as eks_id
         , case when length(trim(regexp_replace(inn1,'[^0-9]',''))) = 10 then null
           else trim(regexp_replace(inn1,'[^0-9]','')) end inn
         , fio1 as fio
         , cast(birth_date1 as timestamp) as birth_dt
         , cast(null as string) Doc_type
         , substr(passport1,1,4) doc_ser
         , substr(passport1,5,6) doc_num
         , issue_dt                     as doc_date
        -- , cast(m_phone as string) 		   as cell_ph_num
         ,cast(null as string) cell_ph_num
         , 'UL' as crit
         , cast(post as string) as  position --210518
         from $Node3t_team_k7m_aux_d_clf_raw_keysIN
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

     val createHiveTableStage4 = spark.sql(
      s"""   ---5 GS_
         select
         concat('GS_',loc_id) as id   -- GS_0123456789
       , id_from 	     as crm_id
       , cast(null as string)           as eks_id
       , inn_from       as inn
       , name_from 	as fio
       , cast(null as timestamp) 			as 	birth_dt
       , cast(null as string)          as Doc_type
       , cast(null as string)        	as doc_ser
       , cast(null as string)    		as doc_num
       , cast(null as timestamp)       as doc_date
       , cast(null as string) 			as cell_ph_num
       , 'CRM_LK' as crit
       , cast(null as string) as  position --210518
        from $Node4t_team_k7m_aux_d_clf_raw_keysIN c
        join $Node5t_team_k7m_aux_d_clf_raw_keysIN b on c.id_to = b.org_crm_id
       where length(inn_from)=12
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    val createHiveTableStage5 = spark.sql(
      s"""  ---5 GS_
        select
         concat('GS_',loc_id) as id  -- GS_0123456789
        , id_to 	    as crm_id
        , cast(null as string)          as eks_id
        , inn_to       	as inn
        , name_to 		  as fio
        , cast(null as timestamp) 			as 	birth_dt
        , cast(null as string)          as Doc_type
        , cast(null as string)        	as doc_ser
        , cast(null as string)    		as doc_num
        , cast(null as timestamp)     as doc_date
        , cast(null as string) 			as cell_ph_num
        , 'CRM_LK' as crit
        , cast(null as string) as  position --210518
         from $Node4t_team_k7m_aux_d_clf_raw_keysIN c
          join $Node5t_team_k7m_aux_d_clf_raw_keysIN b on c.id_from = b.org_crm_id
          where length(inn_to)=12
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    val createHiveTableStage6 = spark.sql(
      s"""  -- 6 EX
          select
           concat('EX_',loc_id) as id -- EX_0123456789
         , cast(null as string) 	     		as crm_id
         , cast(from_eks_id as string)     	as eks_id
         , from_inn      	as inn
         , from_name 		as fio
         , from_date_of_birth 	as 	birth_dt
         , cast(null as string)         		as Doc_type
         , substr(regexp_replace(from_doc_num,'[^0-9]',''),1,4)     as doc_ser
         , substr(regexp_replace(from_doc_num,'[^0-9]',''),-6)      as doc_num
         , cast(null as timestamp)       as doc_date
         , cast(null as string) 			as cell_ph_num
         , 'EXGEN' 		as crit
         , cast(null as string) as  position --210518
         from
          $Node6t_team_k7m_aux_d_clf_raw_keysIN
          where from_obj = 'CLF'
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    val createHiveTableStage8 = spark.sql(
      s"""  -- 8 VKO
         select
         | concat('VK_', loc_id)   as id
         |, cast(crm_id as string) as crm_id
         |, cast(eks_id as string) as eks_id
         |, inn                    as inn
         |, fio                    as fio
         |, birth_dt               as 	birth_dt
         |, cast(Doc_type as string)    as Doc_type
         |, doc_ser
         |, doc_num
         |, cast(null as timestamp)       as doc_date
         |, cell_ph_num
         |, crit
         |, cast(null as string) as  position
         |from $Node9t_team_k7m_aux_d_clf_raw_keysIN
       """.stripMargin
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", {dashboardPath})
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    //потом убрать П 3-4
//    val createHiveTableStage7 = spark.sql(
//      s"""  select
//           concat('UL_',loc_id) as id   -- UL_0123456789
//         , cast(null as string) 		   		as crm_id
//         , cast(null as string)           as eks_id
//         , cast(ul_inn as string)      as inn
//         , fio
//         , cast(birth_dt as timestamp) as birth_dt
//         , cast(null as string) Doc_type
//         , cast(null as string) doc_ser
//         , cast(null as string) doc_num
//         , cast(null as string)  as cell_ph_num
//         , crit
//         from $Node7${config.aux}_clf_raw_keysIN
//       """
//    ).write
//      .format("parquet")
//      .mode(SaveMode.Append)
//      .option("path", {dashboardPath})
//      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")
//
//    val createHiveTableStage8 = spark.sql(
//      s"""   select
//           concat('UL_', loc_id) as id   -- ul_0123456789
//         , cast(fz_ben as string)	     as crm_id
//         , cast(null as string)           as eks_id
//         , cast(null as string)           as inn
//         , concat(last_name,' ',fst_name,' ',mid_name) as fio
//         , cast(birth_dt as timestamp) as birth_dt
//         , type          as Doc_type
//         , series        as doc_ser
//         , doc_number    as doc_num
//         , cell_ph_num
//         , crit
//          from $Node8t_team_k7m_aux_d_clf_raw_keysIN
//       """
//    ).write
//      .format("parquet")
//      .mode(SaveMode.Append)
//      .option("path", {dashboardPath})
//      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_raw_keysOUT")

    logInserted()
  }
}

