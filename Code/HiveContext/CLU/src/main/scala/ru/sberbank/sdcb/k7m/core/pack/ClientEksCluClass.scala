package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientEksCluClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_client"
  val Node2t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_country"
  val Node3t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_cl_org"
  val Node4t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_cl_corp"
  val Node5t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_depart"
  val Node6t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_form_property"
  val Node7t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_solclient"
  val Node8t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_product"
  val Node9t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node10t_team_k7m_aux_d_client_eks_cluIN = s"${DevSchema}.clu_base"
  val Node11t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_contacts"
  val Node12t_team_k7m_aux_d_client_eks_cluIN = s"${Stg0Schema}.eks_z_objects_guid"
   val Nodet_team_k7m_aux_d_client_eks_cluOUT = s"${DevSchema}.client_eks_clu"
  val dashboardPath = s"${config.auxPath}client_eks_clu"


  override val dashboardName: String = Nodet_team_k7m_aux_d_client_eks_cluOUT //витрина
  override def processName: String = "CLU"

  def DoClientEksClu()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_client_eks_cluOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""      select
                   c.id as cl_id,                                     --Уникальный id клиента в EKS
                   c.c_name as cl_name,                               --Наименование клиента в EKS
                   cl_corp.c_long_name,                               --Полное наименование клиента в EKS
                   case
                     when cntr.C_ALFA_3<>'RUS' then true
                     else false
                   end eks_flag_resident,                             --Признак того, что клиент является резидентом на основе данных EKS
                   cntr.C_ALFA_3 as reg_country,                        /*230418 - Добавлено поле. Страна регистрации*/
                   cl_org.c_kio as kio,                              --КИО клиента в EKS
                   cl_corp.c_kod_okpo as okpo,                        --Код ОКПО (Общероссийский классификатор предприятий и организаций) клиента в EKS
                   cl_corp.c_register_date_reestr as reg_date,    --080518 Добавлено поле - Дата регистрации клиента по данным из EKS
                   cl_corp.c_register_gos_reg_num_rec as ogrn,    --220518 Добавлено поле ОГРН
                   c.c_inn,                                     --ИНН клиента в EKS
                   c.c_kpp,                                     --КПП клиента в EKS
                   sbl.sbbol_ndog,                   				   /*240418 Номер договора СББОЛ*/
					         sbl.sbbol_ddog,                   				   /*240418 Дата создания договора СББОЛ*/
                   dep.id as dep_id,                                 --id подразделения банка
                   dep.c_code as dep_cd,                               --код подразделения банка
                   dep.c_name as dep_nm,                               --наименования подразделения банка
                   g.c_guid as org_id,                      /*13.04.2018 ЕКС (orgUID) для определения личного кабинета поручителя в СББОЛ */
                   fp.opf_id,
                   fp.opf_code,
                   fp.opf_code_new,
                   fp.opf_short_name,
                   fp.opf_name,
                   fp.high_opf_code,
                   fp.high_opf_code_new,
                   fp.high_opf_short_name,
                   fp.high_opf_name,
                   cl_org.c_doc_ser,
                   cl_org.c_doc_num,
                   cl_org.c_doc_type,
                   cl_org.c_doc_date_end,
                   contacts_mail.email as email,               /*Email*/
                   contacts_o_phone.o_phone,                   /*Городской телефон*/
                   contacts_o_phone.isworkcontact_o_phone as ophone_work, /*Признак того, что полученный Городской телефон - рабочий*/
                   contacts_m_phone.m_phone,                   /*Мобильный телефон*/
                   contacts_m_phone.isworkcontact_m_phone as mphone_work, /*Признак того, что полученный Мобильный телефон - рабочий*/
                   contacts_fax.fax                            /*Факс*/
                from $Node1t_team_k7m_aux_d_client_eks_cluIN c
                join $Node10t_team_k7m_aux_d_client_eks_cluIN clubase on clubase.eks_id=c.id
                left join $Node2t_team_k7m_aux_d_client_eks_cluIN cntr on cntr.id=c.c_country
                left join $Node3t_team_k7m_aux_d_client_eks_cluIN cl_org on cl_org.id=c.id
                left join $Node4t_team_k7m_aux_d_client_eks_cluIN cl_corp on cl_corp.id=c.id
                left join $Node5t_team_k7m_aux_d_client_eks_cluIN dep on c.c_depart=dep.id
                left join $Node12t_team_k7m_aux_d_client_eks_cluIN g on g.c_object_id=c.id and lower(g.c_class_id) = 'cl_org'
                left join (select
                        s1.c_client as c_cl,
                        s1.c_num_dog as sbbol_ndog,
                        s1.c_date_begin as sbbol_ddog,
                        s1.c_date_begining,
                        s1.rn
                     from (select
                            sol.c_client,
                            pr.c_num_dog,
                            pr.c_date_begin,
                            pr.c_date_begining,
                            row_number() over (partition by sol.c_client order by pr.c_date_begin desc) as rn
                     from $Node7t_team_k7m_aux_d_client_eks_cluIN sol
                     join $Node8t_team_k7m_aux_d_client_eks_cluIN pr on pr.id=sol.id
                     join $Node9t_team_k7m_aux_d_client_eks_cluIN stat on stat.id=pr.c_com_status and lower(stat.c_code)='work') s1
                    where s1.rn=1) sbl on sbl.c_cl=c.id
                    left join (select c2.* from
						(select c1.*,
								row_number() over (partition by c1.id order by c1.isworkcontact desc) as rn
             from   (select c.id,
                            c.c_contacts,
                            coalesce(contact_mail.c_numb, contact_exchange.c_numb, contact_e_mail.c_numb) as email,
                            case
                                when contact_mail.c_numb is not null and contact_mail.c_private in (2051815,5494748220010) then 1
                                when contact_mail.c_numb is null and contact_exchange.c_numb  is not null and contact_exchange.c_private in (2051815,5494748220010) then 1
                                when contact_mail.c_numb is null and contact_exchange.c_numb  is null and contact_e_mail.c_numb is not null and contact_e_mail.c_private in (2051815,5494748220010) then 1
                                else 0
                            end isworkcontact
								 from $Node1t_team_k7m_aux_d_client_eks_cluIN c
								 join $Node10t_team_k7m_aux_d_client_eks_cluIN clubase on clubase.eks_id=c.id
								 left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_mail on contact_mail.collection_id=c.c_contacts and contact_mail.c_type=1919468
								 left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_exchange on contact_exchange.collection_id=c.c_contacts and contact_exchange.c_type=114471164120
								 left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_e_mail on contact_e_mail.collection_id=c.c_contacts and contact_e_mail.c_type=2922860691394) c1
					) c2
					where c2.rn=1) contacts_mail on contacts_mail.c_contacts=c.c_contacts		/*280418*/
     left join (select c2.* from
							(select c1.*,
									row_number() over (partition by c1.id order by isworkcontact_o_phone desc) as rn
							from   (select  c.id,
										    c.c_contacts,
										    case
                            when contact_phone.c_numb is not null then contact_phone.c_numb
                            else null
                        end o_phone,
                        case
                            when contact_phone.c_numb is not null and contact_phone.c_private in (2051815,5494748220010) then 1
                            else 0
                        end isworkcontact_o_phone
									from $Node1t_team_k7m_aux_d_client_eks_cluIN c
									join $Node10t_team_k7m_aux_d_client_eks_cluIN clubase on clubase.eks_id=c.id
									left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_phone on contact_phone.collection_id=c.c_contacts and contact_phone.c_type=1919465  			 /*280418*/
         )c1
						) c2
         where c2.rn=1) contacts_o_phone on contacts_o_phone.c_contacts=c.c_contacts	 	/*280418*/
     left join (select c2.* from
							(select c1.*,
									row_number() over (partition by c1.id order by isworkcontact_m_phone desc) as rn
							from   (select  c.id,
										    c.c_contacts,
										    case
                             when contact_mphone.c_numb is not null then contact_mphone.c_numb
                            else null
                        end m_phone,
                        case
                            when contact_mphone.c_numb is not null and contact_mphone.c_private in (2051815,5494748220010) then 1
                            else 0
                        end isworkcontact_m_phone
									from $Node1t_team_k7m_aux_d_client_eks_cluIN c
									join $Node10t_team_k7m_aux_d_client_eks_cluIN clubase on clubase.eks_id=c.id
									left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_mphone on contact_mphone.collection_id=c.c_contacts and contact_mphone.c_type=64828905491
									) c1
						) c2
						where c2.rn=1) contacts_m_phone on contacts_m_phone.c_contacts=c.c_contacts
     left join (select c2.* from
                (select c1.*,
                        row_number() over (partition by c1.id order by isworkcontact_fax desc) as rn
                from   (select  c.id,
                          c.c_contacts,
                          case
                               when contact_fax.c_numb is not null then contact_fax.c_numb
                              else null
                          end fax,
                          case
                              when contact_fax.c_numb is not null  and contact_fax.c_private in (2051815,5494748220010) then 1
                              else 0
                          end isworkcontact_fax
									from $Node1t_team_k7m_aux_d_client_eks_cluIN c
									join $Node10t_team_k7m_aux_d_client_eks_cluIN clubase on clubase.eks_id=c.id
									left join $Node11t_team_k7m_aux_d_client_eks_cluIN contact_fax on contact_fax.collection_id=c.c_contacts and contact_fax.c_type=1919466) c1 		 /*280418*/
						) c2
						where c2.rn=1) contacts_fax on contacts_fax.c_contacts=c.c_contacts	 	/*280418*/
                left join (select fp1.id as opf_id,                       --id ОПФ в ЕКС
                                       fp1.c_code as opf_code,                  --Код ОПФ в ЕКС
                                       fp1.c_code_new as opf_code_new,          --Код ОПФ по новой кодировке в ЕКС
                                       fp1.c_short_name as opf_short_name,
                                       fp1.c_name  as opf_name,                 --ОПФ в ЕКС
                                       fp2.c_code as high_opf_code,             --Код вышестоящей ОПФ в ЕКС
                                       fp2.c_code_new as high_opf_code_new,     --Код по новой классификации вышестоящей ОПФ в ЕКС
                                       fp2.c_short_name as high_opf_short_name, --вышестоящий ОПФ в ЕКС - сокращенный формат
                                       fp2.c_name  as high_opf_name          --вышестоящий ОПФ в ЕКС
                                from $Node6t_team_k7m_aux_d_client_eks_cluIN fp1
                                left join $Node6t_team_k7m_aux_d_client_eks_cluIN fp2 on fp1.c_upper=fp2.id) fp on fp.opf_id=cl_corp.c_forma
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_client_eks_cluOUT")

    logInserted()

  }
}
