package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfRawKeysClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_raw_keysIN = s"${DevSchema}.clf_raw_keys"
   val Nodet_team_k7m_aux_d_tmp_clf_raw_keysOUT = s"${DevSchema}.tmp_clf_raw_keys"
  val dashboardPath = s"${config.auxPath}tmp_clf_raw_keys"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_raw_keysOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfRawKeys()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_raw_keysOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
          select
         keys.id
        ,keys.crm_id
        ,keys.eks_id
        ,keys.inn
        ,keys.fio
        ,cast(keys.birth_dt as timestamp) as birth_dt
        ,keys.doc_type
        ,keys.doc_ser
        ,keys.doc_num
        ,keys.doc_date
        ,keys.cell_ph_num
        ,keys.crit
        ,keys.position
        ,cast(case when (keys.fio is not null) and (keys.doc_ser is not null) and (keys.doc_num is not null) and (keys.birth_dt is not null)
         and (cast(trim(regexp_replace(concat(keys.doc_ser, keys.doc_num), '[^0-9]+', '')) as string)!='')
         then
         concat(trim(regexp_replace(upper(keys.fio),'[^A-ZА-Я]+',' ')),'_', trim(regexp_replace(concat(keys.doc_ser, keys.doc_num),'[^0-9]+','')),'_', date_format(keys.birth_dt, 'yyyy-MM-dd'))
         else null
         end as string) as k1
         ,cast(case when (keys.fio is not null) and (keys.cell_ph_num is not null) and (keys.birth_dt is not null) then
         concat(trim(regexp_replace(upper(keys.fio),'[^A-ZА-Я]+',' ')),'_', date_format(keys.birth_dt, 'yyyy-MM-dd'),'_',trim(regexp_replace(keys.cell_ph_num,'[^0-9]+','')))
         else null
         end as string) as k2
         ,cast(case when (keys.fio is not null) and (keys.inn is not null) then
         concat(trim(regexp_replace(upper(keys.fio),'[^A-ZА-Я]+',' ')),'_', trim(regexp_replace(keys.inn,'[^0-9]+','')))
         else null
         end as string) as k3
         ,cast(case when (keys.inn is not null) then
         trim(regexp_replace(keys.inn,'[^0-9]+',''))
         else null
         end as string) as k4
         ,cast(trim(regexp_replace(upper(keys.fio), '[^A-ZА-Я]+', ' ')) as string)      as keys_fio
         ,cast(trim(regexp_replace(keys.inn,'[^0-9]','')) as string)                    as keys_inn
         ,cast(trim(regexp_replace(concat(keys.doc_ser, keys.doc_num), '[^0-9]+', '')) as string)      as keys_doc_ser
         ,cast(date_format(birth_dt, 'yyyy-MM-dd') as string)                           as keys_birth_dt
         ,cast(trim(regexp_replace(keys.cell_ph_num,'[^0-9]+','')) as string)           as keys_cell_ph_num
          from
          (select id,crm_id,eks_id,inn,fio,
          case when birth_dt <> cast(to_date(birth_dt) as timestamp)
               then cast (date_add(to_date(birth_dt),1) as timestamp)
               else birth_dt end  birth_dt,
          doc_type,doc_ser,doc_num,doc_date,cell_ph_num,crit,position
          from $Node1t_team_k7m_aux_d_tmp_clf_raw_keysIN
          where fio is not null or inn is not null) keys
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_raw_keysOUT")

    logInserted()
  }
}
