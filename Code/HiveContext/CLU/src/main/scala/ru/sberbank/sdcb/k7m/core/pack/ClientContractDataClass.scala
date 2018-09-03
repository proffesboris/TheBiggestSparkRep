package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class ClientContractDataClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_client_contact_dataIN = s"${DevSchema}.clu_base"
  val Node2t_team_k7m_aux_d_client_contact_dataIN = s"${DevSchema}.client_eks_clu"
  val Node3t_team_k7m_aux_d_client_contact_dataIN = s"${DevSchema}.client_int_contacts_clu"
  val Node4t_team_k7m_aux_d_client_contact_dataIN = s"${DevSchema}.client_crm_clu"
  val Node5t_team_k7m_aux_d_client_contact_dataIN = s"${Stg0Schema}.prv_organization"
   val Nodet_team_k7m_aux_d_client_contact_dataOUT = s"${DevSchema}.client_contact_data"
  val dashboardPath = s"${config.auxPath}client_contact_data"


  override val dashboardName: String = Nodet_team_k7m_aux_d_client_contact_dataOUT //витрина
  override def processName: String = "CLU"

  def DoClientContractData()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_client_contact_dataOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(s"""
                select contact2.inn,
                       contact2.email_source,
                       concat(substr(contact2.email,1,instr(contact2.email,'@')),
                               regexp_replace(
                                 regexp_replace(substr(contact2.email,instr(contact2.email,'@')+1),'(,|;).*',''),       /*удаление 2, 3 и т.д. адресов*/
                                 '\r\n|\r|\n','')) as email,
                       contact2.ophone_source,
                       regexp_replace(
                         regexp_replace(contact2.o_phone,'(,|;).*',''),
                         '\r\n|\r|\n','') as o_phone,
                       regexp_replace(
                         regexp_replace(contact2.m_phone,'(,|;).*',''),
                         '\r\n|\r|\n','') as m_phone,
                       contact2.fax_source,
                       regexp_replace(
                         regexp_replace(contact2.fax,'(,|;).*',''),
                         '\r\n|\r|\n','') as fax
                from (select
                    contact.inn,
                                            case
                                            when contact.eks_id is not null then 'eks_email'
                                            when contact.eks_id is null and int_email is not null then 'int_email'
                                            when contact.eks_id is null and int_email is null and prv_email is not null then 'prv_email'
                                            end email_source,
                                            case
                                             when contact.eks_id is not null then eks_email
                                             when contact.eks_id is null then coalesce(int_email, prv_email)
                                             end email,
                                             case
                                             when contact.eks_id is not null and eks_o_phone is not null then 'eks_o_phone'
                                             when contact.eks_id is null and int_phone is not null then 'int_phone'
                                             when contact.eks_id is null and int_phone is null and prv_phone is not null then 'prv_phone'
                                             end ophone_source,
                                             case when contact.eks_id is not null then coalesce(eks_o_phone)
                                             else coalesce(int_phone, crm_phone, prv_phone)
                                             end o_phone,
                    contact.eks_m_phone as m_phone,
                    case
                       when eks_email is not null then 'eks_fax'
                       when eks_email is null and int_email is not null then 'int_fax'
                       when eks_email is null and int_email is null and  prv_email is not null then 'prv_fax'
                    end fax_source,
                    coalesce(eks_fax,int_fax, prv_fax) as fax
             from (select clubase.inn,
                    case
                        when lower(cl_eks.email) not rlike '[а-яё]' and
                             lower(cl_eks.email) like ('%@%') and lower(cl_eks.email) not rlike '^@' /*проверяем, что в поле - адрес электронной почты*/
                             then regexp_replace(
                                            regexp_replace(lower(cl_eks.email),',ru','.ru'),        /*заменяем все запятые на точки перед доменом .ru*/
                                       ',com','.com')                                               /*заменяем все запятые на точки перед доменом .com*/
                        else null
                        end eks_email,                                                               /*EKS: Email*/
                                            case
                                             when cl_eks.o_phone is not null then regexp_replace(cl_eks.o_phone,'[^-0-9(){},]','')
                                             else regexp_replace(cl_eks.m_phone,'[^-0-9(){},]','')
                                             end eks_o_phone,                                        /*EKS: Городской телефон*/
                    regexp_replace(cl_eks.m_phone,'[^-0-9(){},]','') as eks_m_phone,                 /*EKS: Мобильный телефон*/
                    cl_eks.fax  as eks_fax,                                                          /*EKS: Факс*/
                    case
                        when lower(cl_int.email) not rlike '[а-яё]' and
                             lower(cl_int.email) like ('%@%') and lower(cl_int.email) not rlike '^@'
                             then
                                       regexp_replace(
                                            regexp_replace(lower(cl_int.email),',ru','.ru'),        /*заменяем все запятые на точки перед доменом .ru*/
                                       ',com','.com')
                        else null
                    end int_email,                                                                   /*Integrum: Email*/
                    regexp_replace(cl_int.phone,'[^-0-9(){},]','') as int_phone,                     /*Integrum: Телефон*/
                       cl_int.fax as int_fax,                                                        /*Integrum: Факс*/
                    case
                        when lower(prv.org_email) not rlike '[а-яё]' and
                             lower(prv.org_email) like ('%@%') and lower(prv.org_email) not rlike '^@'
                             then regexp_replace(
                                            regexp_replace(lower(prv.org_email),',ru','.ru'), /*заменяем все запятые на точки перед доменом .ru*/
                                       ',com','.com')
                        else null
                        end prv_email,                                                            /*Pravo: Email*/
                    regexp_replace(prv.org_phone,'[^-0-9(){},]','') as prv_phone,                 /*Pravo: Телефон*/
                    prv.org_fax as prv_fax,                                                       /*Pravo: Факс*/
                    regexp_replace(cl_crm.main_ph_num,'[^-0-9(){},]','') as crm_phone,              /*CRM: Телефон*/
                    clubase.eks_id as eks_id
             from $Node1t_team_k7m_aux_d_client_contact_dataIN clubase
             left join $Node2t_team_k7m_aux_d_client_contact_dataIN cl_eks on clubase.inn=cl_eks.c_inn
             left join $Node3t_team_k7m_aux_d_client_contact_dataIN cl_int on clubase.inn=cl_int.inn
             left join $Node4t_team_k7m_aux_d_client_contact_dataIN cl_crm on cl_crm.org_inn=clubase.inn
             left join (select org_inn,
                               org_phone,
                               org_email,
                               org_fax
                          from $Node5t_team_k7m_aux_d_client_contact_dataIN where year(effectiveto)=2999) prv
               on prv.org_inn=clubase.inn)
             contact)
            contact2
        """)

    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_client_contact_dataOUT")

    logInserted()

  }
}
