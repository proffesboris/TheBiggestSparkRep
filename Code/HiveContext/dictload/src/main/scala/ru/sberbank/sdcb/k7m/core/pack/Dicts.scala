package ru.sberbank.sdcb.k7m.core.pack

class BoConfigClustersSrc(val config: Config) extends DictionaryDescription {
  override val fileName: String = "bo_config_clusters_src.csv"
  override val tableName: String = s"${config.aux}.bo_config_clusters_src"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(BO_ID string,
                                                     |CLUST_ID string,
                                                     |BO_QUALITY string)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class BoConfigCubesSrc(val config: Config) extends DictionaryDescription {
  override val fileName: String = "bo_config_cubes_src.csv"
  override val tableName: String = s"${config.aux}.bo_config_cubes_src"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(BO_ID string,
                                                     |FEATURE_ID string,
                                                     |MIN string,
                                                     |MAX string,
                                                     |categorial string)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class BoConfigFeaturesSrc(val config: Config) extends DictionaryDescription {
  override val fileName: String = "bo_config_features_src.csv"
  override val tableName: String = s"${config.aux}.bo_config_features_src"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(FEATURE_ID string,
                                                     |CODE string,
                                                     |NAME string,
                                                     |FUNC string,
                                                     |STEP string,
                                                     |TYPE string,
                                                     |COMMENT string,
                                                     |eks_fld string,
                                                     |_c8 string)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class Dict7mKey(val config: Config) extends DictionaryDescription {
  override val fileName: String = "dict_7mkey.csv"
  override val tableName: String = s"${config.aux}.dict_7mkey"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(obj string,
                                                     |obj_id string,
                                                     |code string,
                                                     |name string,
                                                     |data_type string,
                                                     |data_length string)
                                             				 |row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                                             				 |stored as textfile""".stripMargin
}


class DictBoPdByScale(val config: Config) extends DictionaryDescription {
  override val fileName: String = "dict_bo_pd_by_scale.csv"
  override val tableName: String = s"${config.aux}.dict_bo_pd_by_scale"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(SBER_RAITING tinyint,
                                                     |PD double,
                                                     |Lower_Bound double,
                                                     |Upper_Bound double)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class DictLkThresholds(val config: Config) extends DictionaryDescription {
  override val fileName: String = "dict_lk_thresholds.csv"
  override val tableName: String = s"${config.aux}.dict_lk_thresholds"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(crit_code string,
                                                     |min_val double,
                                                     |max_val int)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class DictMandatoryKeysOut(val config: Config) extends DictionaryDescription {
  override val fileName: String = "dict_mandatory_keys_out.csv"
  override val tableName: String = s"${config.aux}.dict_mandatory_keys_out"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(obj string,
                                                     |block_nm string,
                                                     |key string)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class RdmCollaborationListK7MMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_COLLABORATION_LIST_K7M_MAST.csv"
  override val tableName: String = s"${config.stg}.RDM_COLLABORATION_LIST_K7M_MAST"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |CRM_ID string,
                                                     |country string,
                                                     |Group string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                             				 |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmCountryComplMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_COUNTRY_COMPL_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_country_compl_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmCountryMainMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_COUNTRY_MAIN_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_country_main_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmDealTargetMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdm_deal_target_mast.csv"
  override val tableName: String = s"${config.stg}.rdm_deal_target_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(row_N string,
                                                     |target_cred string,
                                                     |N string,
                                                     |target string,
                                                     |corr_name string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}


class RdmIntCredHistMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_INT_CRED_HIST_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_int_cred_hist_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |T0 string,
                                                     |T1 string,
                                                     |T2 string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmLimitGuaranteeMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_LIMIT_GUARANTEE_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_limit_guarantee_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |BANK string,
                                                     |GUARANTEE string,
                                                     |BANK_RATE string,
                                                     |GUARANTEE_RATE string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmLinkCriteriaMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdm_link_criteria_mast.csv"
  override val tableName: String = s"${config.stg}.rdm_link_criteria_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(code string,
                                                     |nm string,
                                                     |cbr_flag string,
                                                     |min_val double,
                                                     |max_val double,
                                                     |start_date string,
                                                     |end_date string,
                                                     |rep_flag string,
                                                     |set_flag string,
                                                     |guid string,
                                                     |gl_flag string,
                                                     |econ_flag string,
                                                     |jur_flag string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmMajorGszMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_MAJOR_GSZ_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_major_gsz_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmMmzPtyRoleMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_MMZ_PTY_ROLE_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_mmz_pty_role_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmMmzPtyTypeMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_MMZ_PTY_TYPE_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_mmz_pty_type_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmProblemStatusMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_PROBLEM_STATUS_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_problem_status_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |START_DATE string,
                                                     |END_DATE string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmProdStateMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_PROD_STATE_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_prod_state_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |K7M_FLAG string,
                                                     |START_DATE string,
                                                     |END_DATE string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmRiskProductMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_RISK_PRODUCT_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_risk_product_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmRiskSegmentMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_RISK_SEGMENT_MAST.csv"
  override val tableName: String = s"${config.stg}.rdm_risk_segment_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(CODE string,
                                                     |NM string,
                                                     |CATEGORY string,
                                                     |RISK_PROFILE string,
                                                     |K7M_FLAG string,
                                                     |K7M_OLD_FLAG string,
                                                     |START_DATE string,
                                                     |END_DATE string,
                                                     |GUID string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmSetProvTypeMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdm_set_prov_type_mast.csv"
  override val tableName: String = s"${config.stg}.rdm_set_prov_type_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(nn string,
                                                     |nm string,
                                                     |vid string,
                                                     |ob string,
                                                     |up string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class RdmSetRegimesMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdm_set_regimes_mast.csv"
  override val tableName: String = s"${config.stg}.rdm_set_regimes_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(regime_prod string,
                                                     |n string,
                                                     |use string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}


class RdmSetVidCredMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdm_set_vid_cred_mast.csv"
  override val tableName: String = s"${config.stg}.rdm_set_vid_cred_mast"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(c_name string,
                                                     |target string)
                                             				 |row format delimited fields terminated by ";"
                                                     |stored as textfile
                                                     |tblproperties("skip.header.line.count"="1")""".stripMargin
}

class Rdoc(val config: Config) extends DictionaryDescription {
  override val fileName: String = "rdoc.csv"
  override val tableName: String = s"${config.pa}.rdoc"
  override val createTableQuery: String = s"""create table $tableName
                                             				 |(exec_id string,
                                                     |okr_id bigint,
                                                     |rdoc_id string,
                                                     |obj string,
                                                     |7m_id string,
                                                     |doc_name string,
                                                     |doc_num string,
                                                     |doc_date timestamp,
                                                     |parent_doc_name string,
                                                     |parent_doc_num string,
                                                     |parent_doc_date timestamp,
                                                     |reason_request_txt string,
                                                     |id_doc_ecm string)
                                             				 |row format delimited fields terminated by ","
                                             				 |stored as textfile""".stripMargin
}

class RdmIntOrgMast(val config: Config) extends DictionaryDescription {
  override val fileName: String = "RDM_INT_ORG_MAST_V.csv"
  override val tableName: String = s"${config.stg}.rdm_rdm_int_org_mast_v"
  override val createTableQuery: String = s"""create table $tableName
                                                     |(code string,
                                                     |nm string,
                                                     |crmorgunitid string,
                                                     |k7m_flag int,
                                                     |start_date timestamp,
                                                     |end_date timestamp,
                                                     |guid bigint,
                                                     |tmp int)
                                             				 |row format delimited fields terminated by ";"
                                             |stored as textfile
                                             |tblproperties("skip.header.line.count"="1")""".stripMargin
}