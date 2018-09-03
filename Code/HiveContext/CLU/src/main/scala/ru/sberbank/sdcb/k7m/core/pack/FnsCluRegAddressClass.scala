package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FnsCluRegAddressClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Node1t_team_k7m_aux_d_fns_clu_reg_addressIN = s"${config.stg}.fns_egrul"
   val Nodet_team_k7m_aux_d_fns_clu_reg_addressOUT = s"${config.aux}.fns_clu_reg_address"
  val dashboardPath = s"${config.auxPath}fns_clu_reg_address"

  override val dashboardName: String = Nodet_team_k7m_aux_d_fns_clu_reg_addressOUT //витрина
  override def processName: String = "CLU"

  def DoFnsCluRegAddress() {

    Logger.getLogger(Nodet_team_k7m_aux_d_fns_clu_reg_addressOUT).setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s""" select
                fns.ogrn,                                                                                                                             /*ОГРН*/
                fns.svadresul.AdresRF.Indeks as address_postalcode,                                                                                   /*Индекс*/
                case
                           when lower(fns.svadresul.AdresRF.Region.TipRegion) rlike 'город|республика'
                             and lower(fns.svadresul.AdresRF.Region.NaimRegion) not rlike 'чеченская|кабардино-балкарская|карачаево-черкесская|удмуртская'
                             then concat(fns.svadresul.AdresRF.Region.TipRegion,' ',fns.svadresul.AdresRF.Region.NaimRegion)
                           else concat(fns.svadresul.AdresRF.Region.NaimRegion,' ',fns.svadresul.AdresRF.Region.TipRegion)
                end as address_regionname,                                                                                                          /*Адрес регистрации: Название региона*/
                fns.svadresul.AdresRF.Region.TipRegion as address_shortregiontypename,                                                              /*Адрес регистрации: Название типа региона*/
                fns.svadresul.AdresRF.NaselPunkt.NaimNaselPunkt as address_villagename,                                                             /*Адрес регистрации: Наименование населенного пункта*/
                fns.svadresul.AdresRF.NaselPunkt.TipNaselPunkt as address_shortvillagetypename,                                                 /*Адрес регистрации: Сокращенное название населенного пункта*/
                concat(fns.svadresul.AdresRF.Gorod.TipGorod,' ',fns.svadresul.AdresRF.Gorod.NaimGorod) as address_cityname,                     /*Адрес регистрации: Название города*/
                fns.svadresul.AdresRF.Gorod.TipGorod as address_shortcitytypename,                                                              /*Адрес регистрации: Сокращенное название типа города*/
                concat(fns.svadresul.AdresRF.Raion.NaimRaion,' ',fns.svadresul.AdresRF.Raion.TipRaion) as address_districtname,                     /*Адрес регистрации: Название района*/
                fns.svadresul.AdresRF.Raion.TipRaion as address_shortdistricttypename,                                                              /*Адрес регистрации: Сокращенное название типа района*/
                fns.svadresul.AdresRF.Ulica.NaimUlica as address_streetname,                                                                    /*Адрес регистрации: Наименование улицы*/
                fns.svadresul.AdresRF.Ulica.TipUlica as address_shortStreetTypeName,                                                            /*Адрес регистрации: Сокращенное наименование типа улицы*/
                regexp_replace(fns.svadresul.AdresRF.Dom,'\r\n\r\n','') as address_housenumber,                                                     /*Адрес регистрации: Номер дома*/
                regexp_replace(fns.svadresul.AdresRF.Korpus,'\r\n\r\n','') as address_blocknumber,                                                 /*Адрес регистрации: Номер корпуса*/
                regexp_replace(fns.svadresul.AdresRF.Kvart,'\r\n\r\n','') as address_flatnumber,                                                    /*Адрес регистрации: Номер строения*/
                case
                   when fns.svadresul.AdresRF.Indeks is not null
                   and fns.svadresul.AdresRF.Region.NaimRegion is not null
                   and fns.svadresul.AdresRF.Ulica.NaimUlica is not null then 1
                   else 0
                end is_full_fns_address,                                                                                                             /*Вспомогательный признак наличия адреса из ФНС*/
                fns.kpp as kpp,
                fns.svnaimul.NaimULPoln as ru_full_name,
                fns.svnaimul.NaimULSokr as ru_short_name,
                fns.kodopf
         from $Node1t_team_k7m_aux_d_fns_clu_reg_addressIN fns
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_fns_clu_reg_addressOUT")

    logInserted()

  }


}
