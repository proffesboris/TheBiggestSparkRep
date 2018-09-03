package ru.sberbank.sdcb.k7m.core.pack

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class RDMTBMappingClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Nodet_team_k7m_aux_d_K7M_dict_tb_mappingOUT = s"$DevSchema.dict_tb_mapping"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_dict_tb_mappingOUT //витрина
  val dashboardPath = s"${config.auxPath}dict_tb_mapping"
  override def processName: String = "clu_base"

    def DoRDMTBMapping() {

      Logger.getLogger(Nodet_team_k7m_aux_d_K7M_dict_tb_mappingOUT).setLevel(Level.WARN)

      logStart()

      val createHiveTableStage1 = spark.sql(
        s"""select 'Центральный аппарат' crm_tb_name,'00' tb_code union all
select 'Центрально-Черноземный банк' crm_tb_name,'13' tb_code union all
select 'Уральский банк' crm_tb_name,'16' tb_code union all
select 'Байкальский банк' crm_tb_name,'18' tb_code union all
select 'Московский банк' crm_tb_name,'38' tb_code union all
select 'Среднерусский банк' crm_tb_name,'40' tb_code union all
select 'Волго-Вятский банк' crm_tb_name,'42' tb_code union all
select 'Сибирский банк' crm_tb_name,'44' tb_code union all
select 'Западно-Уральский банк' crm_tb_name,'49' tb_code union all
select 'Юго-Западный банк' crm_tb_name,'52' tb_code union all
select 'Поволжский банк' crm_tb_name,'54' tb_code union all
select 'Северо-Западный банк' crm_tb_name,'55' tb_code union all
select 'Западно-Сибирский банк' crm_tb_name,'67' tb_code union all
select 'Дальневосточный банк' crm_tb_name,'70' tb_code union all
select 'Северный банк' crm_tb_name,'77' tb_code"""
      ).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_dict_tb_mappingOUT")

      logInserted()
      logEnd()
    }

}
