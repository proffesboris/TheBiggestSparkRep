package Tables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

class ClientFilterEnd extends Table{

	val name: String = ClientFilterEndName

	import spark.implicits._

	private val distclient = spark.table(DebitUnionKreditName)

	private val pre_innsec = distclient.select("idclient").where($"idclient".isNotNull)
	private val pre_innsec_hive010617 = z_client.join(broadcast(pre_innsec),z_client("id") === pre_innsec("idclient"),"inner").select(z_client("*"))

	val dataframe: DataFrame = pre_innsec_hive010617.select("*")

	val SQLTableStructure: String =
"c_inspect decimal(38,12) , " +
"c_welf_hist decimal(38,12) , " +
"c_vids_cl decimal(38,12) , " +
"class_id string , " +
"c_inn string , " +
"c_limits_history decimal(38,12) , " +
"c_audit_unicum_code string , " +
"c_hist_okato decimal(38,12) , " +
"c_crm_priority string , " +
"id decimal(38,12) , " +
"c_date_ank timestamp , " +
"c_codes_okonh decimal(38,12) , " +
"c_okveds decimal(38,12) , " +
"c_country decimal(38,12) , " +
"c_limit_val decimal(38,12) , " +
"c_group_link decimal(38,12) , " +
"c_audit_key_valid string , " +
"c_internal_code string , " +
"c_depart decimal(38,12) , " +
"c_stop_list_ref decimal(38,12) , " +
"c_gr_risk_hist decimal(38,12) , " +
"c_limit_cred decimal(17,2) , " +
"c_com_status decimal(38,12) , " +
"c_fail_level decimal(38,12) , " +
"c_name string , " +
"c_vid_cl decimal(38,12) , " +
"c_chk_alt_name string , " +
"c_crm_segment string , " +
"c_properties decimal(38,12) , " +
"c_sbp_time_client string , " +
"c_contacts decimal(38,12) , " +
"c_audit_rcv_file string , " +
"c_ratings decimal(38,12) , " +
"c_cat_class decimal(38,12) , " +
"c_res_info decimal(38,12) , " +
"c_cl_oper1 decimal(38,12) , " +
"c_audit_date_edit timestamp , " +
"c_audit_create decimal(38,12) , " +
"c_addresses decimal(38,12) , " +
"c_kpp string , " +
"c_date_cl timestamp , " +
"c_risk_limits decimal(38,12) , " +
"c_date_form_upd timestamp , " +
"c_audit_edit decimal(38,12) , " +
"sn decimal(38,12) , " +
"c_audit_op_date timestamp , " +
"c_okato_code string , " +
"c_filial decimal(38,12) , " +
"c_date_op timestamp , " +
"c_oper_character string , " +
"c_patt_sign decimal(38,12) , " +
"su decimal(38,12) , " +
"c_fonds_ar decimal(38,12) , " +
"c_audit_date_create timestamp , " +
"c_fail_mark string , " +
"c_registr_num decimal(9,0) , " +
"c_audit_from_rout string , " +
"c_notes string , " +
"c_names decimal(38,12) , " +
"c_acc_bank decimal(38,12) , " +
"c_disposit decimal(38,12) , " +
"c_disposits decimal(38,12) , " +
"c_links_other decimal(38,12) , " +
"c_offshore decimal(38,12) , " +
"c_inc_resources decimal(38,12) , " +
"c_i_name string , " +
"c_privileges decimal(38,12) , " +
"c_loan_ins_history decimal(38,12) , " +
"c_decl_fio decimal(38,12) , " +
"c_name_valid string , " +
"c_loan_historys decimal(38,12) , " +
"ctl_action string , " +
"ctl_validfrom timestamp , " +
"ctl_loading bigint , " +
"ctl_pa_loading bigint , " +
"ctl_csn decimal(38,0)" 
}
