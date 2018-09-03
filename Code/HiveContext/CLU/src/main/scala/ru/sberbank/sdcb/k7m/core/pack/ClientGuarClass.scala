package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientGuarClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Node1t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_product"
  val Node2t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_guaranties"
  val Node3t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_client"
  val Node4t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_com_status_prd"
  val Node5t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_ft_money"
  val Node6t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_kind_credits"
  val Node7t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_types_cred"
  val Node8t_team_k7m_aux_d_basis_client_guar_cluIN = s"${config.stg}.eks_z_ac_fin"
  val Nodet_team_k7m_aux_d_basis_client_guar_cluOUT = s"${config.aux}.client_guar_clu"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_guar_cluOUT //витрина
  override def processName: String = "CLU"

  val dashboardPath = s"${config.auxPath}basis_client_guar_clu"

  def DoClientGuar() {

    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_guar_cluOUT).setLevel(Level.WARN)



    val createHiveTableStage1 = spark.sql(
      s"""    select
            g.c_principal as principal_id,					/*FK на core_internal_eks.z_client. Ссылка на id клиента, являющегося принципалом*/
            c.c_name as principal_name,                     /*Наименование принципала - ЮЛ, с которым заключен договор о банковской ганартии*/
            c.C_INN as principal_inn,                        /*ИНН принципала*/
            p.C_NUM_DOG as num_dog,							/*Номер договора*/
            f.C_MAIN_V_ID as num_acc,						/*Номер внебалансового счета 913...*/
            f.C_SALDO as acc_saldo, 						/*Остаток на внебалансовом счете*/
            p.C_DATE_BEGIN as prod_date_begin,				/*Дата создания договора*/
            p.C_DATE_BEGINING as prod_date_begining,		/*Дата начала действия договора*/
            p.C_DATE_ENDING as prod_date_endining,			/*Дата окончания действия договора*/
            s.C_code as prod_code,							/*Код статуса договора*/
            s.C_NAME as prod_state,							/*Статус договора*/
            g.C_SUMMA as guar_sum,							/*Сумма договора*/
            ft2.C_CUR_SHORT as val_giv,						/*Валюта выдачи*/
            ft1.C_CUR_SHORT as val_lim,						/*Валюта договора*/
            p.C_DATE_CLOSE as dog_date_close,				/*Дата закрытия договора*/
            kc.c_code as kind_code,					 		/*Код вида договора*/
            kc.c_name as kind_name,					 		/*Вид договора*/
            tc.c_short_name as type_name,					/*Тип договора*/
            concat("(", c.c_inn, ") ", c.c_name) as principal_name_inn,					/*Вспомогательный атрибут для валидации. Имя и ИНН организации в одну строку*/
            concat("(", p.C_NUM_DOG, ") ", kc.c_name, "_begd: ", case when p.C_DATE_BEGINING is not null then p.C_DATE_BEGINING else null end, "_endd: ", case when p.C_DATE_ENDING is not null then p.C_DATE_ENDING else null end) as dog_num_kind_liveperiod 					/*Вспомогательный атрибут для валидации. Информация по кредитному договору в одну строку*/
       FROM $Node1t_team_k7m_aux_d_basis_client_guar_cluIN p
       join $Node2t_team_k7m_aux_d_basis_client_guar_cluIN g on g.ID = p.ID
       join $Node3t_team_k7m_aux_d_basis_client_guar_cluIN c on c.id = g.c_principal
  left join $Node4t_team_k7m_aux_d_basis_client_guar_cluIN s on p.c_com_status = s.ID
  left join $Node5t_team_k7m_aux_d_basis_client_guar_cluIN ft1  on g.c_ft_limit = ft1.ID
  left join $Node6t_team_k7m_aux_d_basis_client_guar_cluIN kc  on g.C_KIND_CREDIT = kc.ID
  left join $Node7t_team_k7m_aux_d_basis_client_guar_cluIN tc on kc.c_reg_rules=tc.id
  left join $Node5t_team_k7m_aux_d_basis_client_guar_cluIN ft2  on g.C_VALUTA = ft2.ID
  left join $Node8t_team_k7m_aux_d_basis_client_guar_cluIN f  on g.C_VNB_ACCOUNT = f.ID
			WHERE
            lower(p.CLASS_ID) = 'guaranties' and
            lower(s.C_code) in (${SparkMainClass.cCodes})
            --order by principal_inn, num_dog, prod_date_begining
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_guar_cluOUT")

    logInserted()

  }


}
