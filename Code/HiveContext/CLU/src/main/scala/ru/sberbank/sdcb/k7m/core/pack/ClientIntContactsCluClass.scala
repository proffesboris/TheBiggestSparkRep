package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientIntContactsCluClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_client_int_contacts_cluIN = s"${config.stg}.int_contact"
  val Node2t_team_k7m_aux_d_client_int_contacts_cluIN = s"${config.aux}.clu_base"
   val Nodet_team_k7m_aux_d_client_int_contacts_cluOUT = s"${config.aux}.client_int_contacts_clu"
  val dashboardPath = s"${config.auxPath}client_int_contacts_clu"


  override val dashboardName: String = Nodet_team_k7m_aux_d_client_int_contacts_cluOUT //витрина
  override def processName: String = "CLU"

  def DoClientIntContactsClu() {

    Logger.getLogger(Nodet_team_k7m_aux_d_client_int_contacts_cluOUT).setLevel(Level.WARN)



    val createHiveTableStage1 = spark.sql(
      s"""   select inn,
                    coalesce(phone_egrul,phone_rosstat,phone_zakupki) as phone,   --Номер телефона
                    coalesce(email_egrul,email_rosstat,email_zakupki) as email,   --Email
                    coalesce(fax_egrul,fax_rosstat) as fax                        --Факс
             from (
                select c.inn,
                    max(case when lower(c.contact_type)='телефон' and lower(source_cd)='egrul' then c.contact_details end) as phone_egrul,
                    max(case when lower(c.contact_type)='телефон' and lower(source_cd)='zakupki' then c.contact_details end) as phone_ZAKUPKI,
                    max(case when lower(c.contact_type)='телефон' and lower(source_cd)='rosstat' then c.contact_details end) as phone_rosstat,
                    max(case when lower(c.contact_type)='электронная почта' and lower(source_cd)='egrul' then c.contact_details end) as email_egrul,
                    max(case when lower(c.contact_type)='электронная почта' and lower(source_cd)='zakupki' then c.contact_details end) as email_ZAKUPKI,
                    max(case when lower(c.contact_type)='электронная почта' and lower(source_cd)='rosstat' then c.contact_details end) as email_rosstat,
                    max(case when lower(c.contact_type)='факс' and lower(source_cd)='rosstat' then c.contact_details end) as fax_rosstat,
                    max(case when lower(c.contact_type)='факс' and lower(source_cd)='egrul' then c.contact_details end) as fax_egrul
                from $Node1t_team_k7m_aux_d_client_int_contacts_cluIN c
                join $Node2t_team_k7m_aux_d_client_int_contacts_cluIN b on c.inn=b.inn
                group by c.inn)
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_client_int_contacts_cluOUT")

    logInserted()

  }


}
