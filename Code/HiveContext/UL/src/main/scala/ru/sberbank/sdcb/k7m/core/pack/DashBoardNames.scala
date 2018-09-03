package ru.sberbank.sdcb.k7m.core.pack

trait DashBoardNames {

	val prefix = "gsl_proto_"

	/**
		* BEN CRM
		* */

	/** STG */

	val CRM_stg_cust_ShortName: String = prefix + "stg_cust"
	val CRM_stg_addr_ShortName: String = prefix + "stg_addr"
	val CRM_stg_doc_ShortName: String  = prefix + "stg_doc"

	/** DDS */

	val DDS_BEN_SHR_CRM_T1_ShortName: String		 = prefix + "dds_ben_shr_crm_t1"
	val tmp_mdm_inn_ShortName: String 					 = prefix + "tmp_mdm_inn"
	val tmp_mdm_person_ShortName: String			   = prefix + "tmp_mdm_person"
	val tmp_ben_keys_ShortName: String 					 = prefix + "tmp_mdm_keys"
	val tmp_ben_keys_mdm_step1_ShortName: String = prefix + "tmp_ben_keys_mdm_step1"
	val tmp_ben_keys_mdm_step2_ShortName: String = prefix + "tmp_ben_keys_mdm_step2"
	val tmp_ben_keys_inn_ShortName: String  		 = prefix + "tmp_ben_keys_inn"
	val CRM_BENFR_TABLE_ShortName: String 			 = prefix + "dds_ben_shr_crm_enrich"

	/**
		* UL
		* */

	/** STG */
	val STG_UL_Egrul_ShortName: String 	 = prefix + "stg_ul_egrul"
	val STG_UL_Rosstat_ShortName: String = prefix + "stg_ul_rosstat"
	val STG_UL_ShortName: String 				 = prefix + "stg_ul"
	val STG_fnd_eg_ShortName: String 		 = prefix + "stg_fnd_eg"
	val STG_fnd_rs_T1_ShortName: String  = prefix + "stg_fnd_rs_T1"
	val STG_fnd_rs_T2_ShortName: String  = prefix + "stg_fnd_rs_T2"
	val STG_fnd_rs_ShortName: String		 = prefix + "stg_fnd_rs"
	val STG_shr_T1_ShortName: String		 = prefix + "stg_shr_T1"
	val STG_shr_ShortName: String			   = prefix + "stg_shr"
	val STG_exec_ShortName: String			 = prefix + "stg_exec"
	val STG_tr_prsn_ShortName: String	   = prefix + "stg_tr_prsn"
	val STG_dir_brd_ShortName: String    = prefix + "stg_dir_brd"
	val STG_ko_lst_ShortName: String		 = prefix + "stg_ko_lst"
	val STG_appl_T2_ShortName: String		 = prefix + "stg_appl_T2"
	val STG_appl_T1_ShortName: String		 = prefix + "stg_appl_T1"
	val STG_appl_T0_clean_pass_ShortName: String		 = prefix + "stg_appl_T0_clean_pass"
	val STG_appl_T0_restored_ShortName: String		 = prefix + "stg_appl_T0_restored"
	val STG_appl_ShortName: String			 = prefix + "stg_appl"
	val STG_rel_ShortName: String			 	 = prefix + "stg_rel"
	val STG_ip_ShortName: String			   = prefix + "stg_ip"
	/** PRDDS1 */
	val PRDDS1_fnd_eg_ShortName: String	 = prefix + "prdds1_fnd_eg"
	val PRDDS1_fnd_rs_ShortName: String	 = prefix + "prdds1_fnd_rs"
	/** PRDDS2 */
	val PRDDS2_fnd_T1_ShortName: String	   = prefix + "prdds2_fnd_T1"
	val PRDDS2_fnd_ShortName: String	   = prefix + "prdds2_fnd"
	val STG_rel_union_ShortName: String  = prefix + "stg_rel_union"
	val PRDDS2_rel_T1_ShortName: String	   = prefix + "prdds2_rel_T1"
	val PRDDS2_rel_ShortName: String	   = prefix + "prdds2_rel"
	/** DDS */
	val DDS_ownr_ShortName: String 			 = prefix + "dds_ownr"
	val DDS_exec_ShortName: String 			 = prefix + "dds_exec"
	val DDS_dir_brd_ShortName: String 	 = prefix + "dds_dir_brd"
	val DDS_rel_TX_ShortName: String 		 = prefix + "dds_rel_TX"
	val DDS_rel_TX2_ShortName: String 	 = prefix + "dds_rel_TX2"
	val DDS_rel_ShortName: String 			 = prefix + "dds_rel"
	val DDS_ko_list_ShortName: String 	 = prefix + "dds_ko_list"
	/** PRMTR1 */
	val PRMTR1_ownr_rec_ShortName: String	 			 = prefix + "prmtr1_ownr_rec"
	val PRMTR1_dir_aggr_ShortName: String	 			 = prefix + "prmtr1_dir_aggr"
	val PRMTR1_exec_aggr_ShortName: String 			 = prefix + "prmtr1_exec_aggr"
	val PRMTR1_ben_crm_ShortName: String   			 = prefix + "prmtr1_ben_crm"
	val PRMTR1_ben_crm_cruel_ShortName: String   = prefix + "prmtr1_ben_crm_cruel"
	/** PRMTR2 */
	val PRMTR2_ownr_res_t1_ShortName: String = prefix + "prmtr2_ownr_res_t1"
	val PRMTR2_ownr_res_t2_ShortName: String = prefix + "prmtr2_ownr_res_t2"
	val PRMTR2_ownr_res_t3_ShortName: String = prefix + "prmtr2_ownr_res_t3"
	/** PRMTR3 */
	val PRMTR3_ownr_ul_ShortName: String = prefix + "prmtr3_ownr_ul"
	val PRMTR3_ownr_fl_ShortName: String = prefix + "prmtr3_ownr_fl"
	/** PRMTR4 */
	val PRMTR4_bg_ownr_ShortName: String 		= prefix + "prmtr4_bg_ownr"
	val PRMTR4_bh_ownr_ShortName: String 		= prefix + "prmtr4_bh_ownr"
	val PRMTR4_dir_mtch_ShortName: String 	= prefix + "prmtr4_dir_mtch"
	val PRMTR4_exec_mtch_ShortName: String 	= prefix + "prmtr4_exec_mtch"
	val PRMTR4_extpownr_ShortName: String 	= prefix + "prmtr4_exetpownr" // EXTPOWNR
	val PRMTR4_bencrm_ex_ShortName: String 	= prefix + "prmtr4_bencrm_ex"
	/** MRT1 */
	val MRT1_Crta_5_1_1_ShortName: String 		= prefix + "mrt1_crta_5_1_1"
	val MRT1_Crta_5_1_4_ShortName: String 		= prefix + "mrt1_crta_5_1_4"
	val MRT1_Crta_5_1_5_ShortName: String 		= prefix + "mrt1_crta_5_1_5"
	val MRT1_Crta_5_1_6_ShortName: String 		= prefix + "mrt1_crta_5_1_6"
	val MRT1_Crta_5_1_7_T0_ShortName: String 	= prefix + "mrt1_crta_5_1_7_T0"
	val MRT1_Crta_5_1_7_ShortName: String 	  = prefix + "mrt1_crta_5_1_7"
	val MRT1_Crta_5_1_7ben_ShortName: String 	= prefix + "mrt1_crta_5_1_7ben"
	val MRT1_Crta_5_1_8_T1_ShortName: String 	= prefix + "mrt1_crta_5_1_8_T1"
	val MRT1_Crta_5_1_8_T3_ShortName: String 	= prefix + "mrt1_crta_5_1_8_T3"
	val MRT1_Crta_5_1_8_T5_ShortName: String 	= prefix + "mrt1_crta_5_1_8_T5"
	val MRT1_Crta_5_1_8_ShortName: String 	  = prefix + "mrt1_crta_5_1_8"
	val MRT1_Crta_5_1_7ip_ShortName: String 	= prefix + "mrt1_crta_5_1_7ip"
	val MRT1_Crta_5_1_9_ShortName: String 		= prefix + "mrt1_crta_5_1_9"
	/** MRT2 */
	val MRT2_GSL_ALL_ShortName: String        = prefix + "mrt2_gsl_all"
	/** MRT3 */
	val MRT3_GSL_FINAL_ShortName: String      = "mrt3_gsl_final"

}
