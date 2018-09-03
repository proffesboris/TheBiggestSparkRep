package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class EksCluAddressClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_client"
  val Node2t_team_k7m_aux_d_eks_clu_addressIN = s"${DevSchema}.clu_base"
  val Node3t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_personal_address"
  val Node4t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_address_type"
  val Node5t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_names_city"
  val Node6t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_sprav_street"
  val Node7t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_street_name"
  val Node8t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_people_place"
  val Node9t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_region"
  val Node10t_team_k7m_aux_d_eks_clu_addressIN = s"${Stg0Schema}.eks_z_territory"
  val Nodet_team_k7m_aux_d_eks_clu_addressOUT = s"${DevSchema}.eks_clu_address"
  val dashboardPath = s"${config.auxPath}eks_clu_address"

  override val dashboardName: String = Nodet_team_k7m_aux_d_eks_clu_addressOUT //витрина
  override def processName: String = "CLU"

  def DoEksCluAddress()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_eks_clu_addressOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""select t.*,
						row_number() over (partition by cl_inn order by range_type_fact_adr) as rn_fact,
            row_number() over (partition by cl_inn order by range_type_reg_adr) as rn_reg
			from
				(select
						c.id as cl_id,     									/*EKS_ID*/
						c.c_name as cl_name,    							/*наименование организации*/
						c.c_inn as cl_inn,			                    	/*ИНН организации*/
						case
							when pa.c_post_code is not null then  pa.c_post_code
							when pa_village.c_post_code is not null and pa.c_post_code is null then pa_village.c_post_code
							else null
						end address_postalcode,         					/*Почтовый индекс*/
						case
							when lower(pa_village.c_name) rlike '(москва|петербург)$$' then null
							else ter.c_name
						end address_regionname,             	  			 /*Регион: область, республика, край*/
						pa_village.c_name as address_villagename,     	 /*Населенный пункт*/
						pl.c_short_name as address_VillageType,   		 /*Тип населенного пункта_сокращенно*/
						case
							when lower(pl.c_name)='код 0 в спр. банков цб' and lower(pa_village.c_name) not rlike '(москва|петербург|петербург,)$$' then ''
              when lower(pl.c_name)='код 0 в спр. банков цб' and lower(pa_village.c_name) rlike '(москва|петербург|петербург,) $$' then 'город'
							when lower(pl.c_name)<>'город' and lower(pa_village.c_name) rlike '(москва|петербург|петербург,)$$' then 'город'
							else pl.c_name
						end address_shortVillageTypeName,    	 /*Тип населенного пункта*/
						case
							when pa_village_area.c_name is not null and lower(pa_village.c_name) not rlike '(москва|петербург)$$' then  pa_village_area.c_name
							when pa_district.c_name is not null and pa_village_area.c_name is null  and lower(pa_village.c_name) not rlike '(москва|петербург)$$' then pa_district.c_name
							else null
						end address_districtname,    	                     /*Район населенного пункта*/
						pa.c_sprav_street,   	  							 /*id в z_sprav_street*/
						pa_street.c_street_name,  							 /*id в z_street_name из z_sprav_street*/
						case
							when pa_street_nm.c_name is not null then pa_street_nm.c_name
							when pa_street_nm.c_name is null and pa_street.c_name_street is not null then pa_street.c_name_street
							when pa_street_nm.c_name is null and pa_street.c_name_street is null and length(pa.c_street)<=50 then pa.c_street
							else null
						end address_streetname,  							 /*Улица*/
						pa_street.c_short_name_tip,  						 /*Сокращенный тип улицы*/
						pa_street.c_name_tip as address_shortStreetTypeName,/*Тип улицы*/
						pa_street.c_from_kladr,						  		 /*Признак улицы из КЛАДР*/
						pa.c_street as address_streetname_str,  	         /*Адрес строкой*/
						regexp_replace(
							regexp_replace(
								regexp_replace(
									regexp_replace(
										regexp_replace(
											regexp_replace(
												regexp_replace(
													regexp_replace(pa.c_street,"СТОЛИЦА РОССИЙСКОЙ ФЕДЕРАЦИИ",""),
															"ГОРОД ФЕДЕРАЛЬНОГО ЗНАЧЕНИЯ",""),
																	"ГОРОД МОСКВА",""),
																			"ГОРОД САНКТ-ПЕТЕРБУРГ",""),
																				"РОССИЯ",""),
																					"РОССИЙСКАЯ ФЕДЕРАЦИЯ",""),
																					",   , ", ", "),
																						"0 МОСКВА", "г. Москва") as address_streetname_str2,/*Адрес строкой_очищенный*/
						pa.c_building as address_buildingnumber, 	           /*Корпус*/
						pa.c_house as address_housenumber, 	          	   /*Дом*/
						pa.c_korpus as address_blocknumber,	          	   /*Строение*/
						pa.c_flat as address_flatnumber,
						pa_type.c_name,	          	   						   /*Тип адреса*/
						pa.c_reg_date,          	   						   /*Дата регистрации*/
						case
							when lower(pa_type.c_kod)='fact' then 1
							when lower(pa_type.c_kod)='fact_1' then 2
							when lower(pa_type.c_kod)='real_live' then 3
							when lower(pa_type.c_kod)='post' then 4
							when lower(pa_type.c_kod)='posh' then 5
							else 6        	   						  			/*Ранжирование по типу адреса*/
						end range_type_fact_adr,                                     /*Ранжирование по типу адреса для фактических адресов*/
            pa_type.c_kod as address_type,
            case
               when lower(pa_type.c_kod)='corp_add' then 1
               when lower(pa_type.c_kod)='registration' then 2
               when lower(pa_type.c_kod)='registration1' then 3
               when lower(pa_type.c_kod)='reg_city' then 4
               else 5
            end range_type_reg_adr                                      /*Ранжирование по типу адреса для регистрационных адресов*/
			 from $Node1t_team_k7m_aux_d_eks_clu_addressIN c
			 join $Node2t_team_k7m_aux_d_eks_clu_addressIN clubase on clubase.eks_id=c.id
  left join $Node3t_team_k7m_aux_d_eks_clu_addressIN pa on pa.collection_id=c.c_addresses
  left join $Node4t_team_k7m_aux_d_eks_clu_addressIN pa_type on pa_type.id=pa.c_type /*and lower(pa_type.c_kod) in ('real_live','fact','fact_1', 'post', 'posh')*/
  left join $Node5t_team_k7m_aux_d_eks_clu_addressIN pa_village on pa_village.id=pa.c_city
  left join $Node6t_team_k7m_aux_d_eks_clu_addressIN pa_street on pa_street.id=pa.c_sprav_street
  left join $Node7t_team_k7m_aux_d_eks_clu_addressIN pa_street_nm on pa_street_nm.id=pa_street.c_street_name
  left join $Node8t_team_k7m_aux_d_eks_clu_addressIN pl on cast(pl.id as decimal(38,12))=pa_village.c_people_place
  left join $Node9t_team_k7m_aux_d_eks_clu_addressIN pa_village_area on pa_village_area.id=pa_village.c_area
  left join $Node9t_team_k7m_aux_d_eks_clu_addressIN pa_district on pa_district.id=pa.c_district
  left join $Node10t_team_k7m_aux_d_eks_clu_addressIN ter on pa_village.c_region=ter.id) t
			 --where t.range_type_adr<>6
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_eks_clu_addressOUT")

    logInserted()

  }
}
